//! [`run`]: fd 3 → C4 → the plane (PROTOCOL §5, §9.2; Go `sdk.Run`).
//!
//! Threads (E03-D003):
//!
//! - the caller's thread runs the plane calls C4 asks for, Init, Start,
//!   Run's dispatcher loop, and an OnStop before Run, so every strategy hook
//!   runs on it;
//! - `mftik-c4` owns the [`Machine`] and performs its effects; every input,
//!   fd 3 frames, SIGTERM, timers, and plane results, reaches it through one
//!   channel, so it takes them in order and never blocks on a hook;
//! - `mftik-ctl-read` reads fd 3, `mftik-beat` sends Beat, `mftik-signal`
//!   takes SIGTERM with signal-hook (C4 itself does not, E08-D006), and
//!   `mftik-io` is the tokio runtime with NATS and the timer table;
//! - a short-lived thread waits for Run's ending (`Dispatcher::wait_run`) and
//!   one for OnStop through the dispatcher, so the C4 thread keeps reading.
//!
//! Every exit, the C4 `Exit` effect, a fatal write, a control fd that cannot
//! be taken, a runtime that cannot start, or a thread that died, goes
//! through [`crate::exit::exit_with`] (E03-D005).

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;

use mftik_core::c4::{self, CallError, Effect, Machine, RunResult, TimerToken};
use mftik_core::dispatch::StopRequest;
use mftik_proto::wire::mftiko::worker::v1::WorkerDown;
use signal_hook::consts::SIGTERM;
use signal_hook::iterator::Signals;

use crate::beat::BeatThread;
use crate::call::CallCtx;
use crate::clock::Clock;
use crate::ctl::CtlFd;
use crate::exit::exit_with;
use crate::frame::FrameWriter;
use crate::io::Io;
use crate::plane::{Plane, note};
use crate::reader::{ReadEvent, spawn_reader};
use crate::strategy::Strategy;
use crate::timers::{TimerKey, Timers};

/// `Hello.release`: this runtime's version.
pub const RELEASE: &str = env!("CARGO_PKG_VERSION");

type Out = Arc<FrameWriter<CtlFd>>;

/// Hosts `strategy` as a session worker: C4 on the control fd, NATS with the
/// session's credentials, and the dispatcher on this thread. It does not
/// return; the process ends with PROTOCOL §5.3's exit code (E03-D005).
pub fn run<S: Strategy>(strategy: S) -> ! {
    // SAFETY: `run` is the session worker's entry point and takes the
    // control fd the shim handed over, once.
    let ctl = match unsafe { CtlFd::from_env() } {
        Ok(ctl) => ctl,
        Err(err) => {
            note(&format!("control fd: {err}"));
            exit_with::<CtlFd>(None, err.exit_code())
        }
    };
    serve(ctl, strategy)
}

/// What the C4 thread takes, in arrival order.
#[derive(Debug)]
enum Event {
    Down(WorkerDown),
    Eof,
    Sigterm,
    Timer(TimerToken),
    InitReturned(Result<(), CallError>, Duration),
    StartReturned(Result<(), CallError>),
    RunReturned(RunResult),
    LiveReturned(Result<(), CallError>),
    OnStopReturned(Result<(), CallError>),
}

/// A plane call for the caller's thread.
enum Job {
    Init { payload: Vec<u8>, call: CallCtx },
    Start { payload: Vec<u8>, call: CallCtx },
    Run,
    OnStop,
}

fn serve<S: Strategy>(ctl: CtlFd, strategy: S) -> ! {
    let reading = ctl.try_clone();
    let out: Out = Arc::new(FrameWriter::new(ctl));
    let reading = match reading {
        Ok(fd) => fd,
        Err(err) => fatal(&out, &format!("control fd: {err}")),
    };
    let (events, inbox) = mpsc::channel::<Event>();
    // SIGTERM is taken before Hello, as Go's signal.NotifyContext.
    if let Err(err) = watch_sigterm(events.clone()) {
        fatal(&out, &format!("sigterm: {err}"));
    }
    let io = match Io::start() {
        Ok(io) => io,
        Err(err) => fatal(&out, &format!("io thread: {err}")),
    };
    let clock = Clock::new();
    let timers = Timers::new(io.clone(), clock);
    let plane = match Plane::start(strategy, io, clock, timers.clone()) {
        Ok(plane) => plane,
        Err(err) => fatal(&out, &format!("command thread: {err}")),
    };
    let (jobs, queue) = mpsc::channel::<Job>();
    let driver = Driver {
        machine: Machine::new(c4::Config::session(RELEASE, std::process::id() as i32)),
        out: Arc::clone(&out),
        plane: Arc::clone(&plane),
        clock,
        timers,
        events: events.clone(),
        jobs,
        beat: None,
    };
    let c4_out = Arc::clone(&out);
    let spawned = thread::Builder::new()
        .name("mftik-c4".into())
        .spawn(move || {
            let result = catch_unwind(AssertUnwindSafe(move || driver.serve(reading, inbox)));
            let why = match result {
                Ok(never) => match never {},
                Err(_) => "c4 thread panicked",
            };
            fatal(&c4_out, why)
        });
    if let Err(err) = spawned {
        fatal(&out, &format!("c4 thread: {err}"));
    }
    // Hooks catch their own panics; this catches one in the plane itself,
    // which would otherwise unwind out of `run` past E03-D005.
    match catch_unwind(AssertUnwindSafe(|| perform_jobs(&plane, &events, &queue))) {
        Ok(()) => fatal(&out, "c4 thread is gone"),
        Err(_) => fatal(&out, "dispatcher thread panicked"),
    }
}

/// The caller's thread: plane calls in the order C4 asks for them.
fn perform_jobs<S: Strategy>(plane: &Arc<Plane<S>>, events: &Sender<Event>, queue: &Receiver<Job>) {
    for job in queue {
        let event = match job {
            Job::Init { payload, call } => match plane.init(&payload, &call) {
                Ok(hook_timeout) => Event::InitReturned(Ok(()), hook_timeout),
                Err(err) => Event::InitReturned(Err(err), Duration::ZERO),
            },
            Job::Start { payload, call } => Event::StartReturned(plane.start_call(&payload, &call)),
            Job::Run => {
                plane.run_loop();
                continue;
            }
            Job::OnStop => Event::OnStopReturned(plane.on_stop_direct()),
        };
        let _ = events.send(event);
    }
}

fn watch_sigterm(events: Sender<Event>) -> std::io::Result<()> {
    let mut signals = Signals::new([SIGTERM])?;
    thread::Builder::new()
        .name("mftik-signal".into())
        .spawn(move || {
            for _ in signals.forever() {
                if events.send(Event::Sigterm).is_err() {
                    return;
                }
            }
        })?;
    Ok(())
}

/// Logs `why` and exits [`c4::EXIT_OTHER`]: the worker cannot go on
/// speaking C4.
fn fatal(out: &Out, why: &str) -> ! {
    note(why);
    exit_with(Some(&**out), c4::EXIT_OTHER)
}

struct Driver<S> {
    machine: Machine,
    out: Out,
    plane: Arc<Plane<S>>,
    clock: Clock,
    timers: Timers,
    events: Sender<Event>,
    jobs: Sender<Job>,
    beat: Option<BeatThread>,
}

impl<S: Strategy> Driver<S> {
    fn serve(mut self, reading: CtlFd, inbox: Receiver<Event>) -> std::convert::Infallible {
        let hello = self.machine.hello();
        self.perform(hello);
        let events = self.events.clone();
        let reader = spawn_reader(reading, move |event| {
            let event = match event {
                ReadEvent::Message(msg) => Event::Down(msg),
                ReadEvent::Eof => Event::Eof,
                // C4 treats a broken stream as the shim gone (§9.2.7, E03-D032).
                ReadEvent::Failed(err) => {
                    note(&format!("control fd: {err}"));
                    Event::Eof
                }
            };
            let _ = events.send(event);
        });
        if let Err(err) = reader {
            fatal(&self.out, &format!("control fd reader: {err}"));
        }
        loop {
            // The driver holds a sender, so the channel never closes.
            let Ok(event) = inbox.recv() else {
                fatal(&self.out, "c4 inbox closed");
            };
            let now = self.clock.now_ns();
            let effects = match event {
                Event::Down(msg) => self.machine.on_down(now, msg),
                Event::Eof => self.machine.on_eof(now),
                Event::Sigterm => self.machine.on_sigterm(now),
                Event::Timer(token) => self.machine.on_timer(now, token),
                Event::InitReturned(result, hook_timeout) => {
                    self.machine.init_returned(now, result, hook_timeout)
                }
                Event::StartReturned(result) => self.machine.start_returned(now, result),
                Event::RunReturned(result) => self.machine.run_returned(now, result),
                Event::LiveReturned(result) => self.machine.live_returned(now, result),
                Event::OnStopReturned(result) => self.machine.on_stop_returned(now, result),
            };
            self.perform(effects);
        }
    }

    fn perform(&mut self, effects: Vec<Effect>) {
        for effect in effects {
            match effect {
                Effect::Send { msg, fatal: must } => {
                    if let Err(err) = self.out.send(&msg)
                        && must
                    {
                        fatal(&self.out, &format!("control fd: {err}"));
                    }
                }
                Effect::StartBeat => {
                    if self.beat.is_none() {
                        match BeatThread::spawn(Arc::clone(&self.out)) {
                            Ok(beat) => self.beat = Some(beat),
                            // PROTOCOL §9.7's Beat check takes over.
                            Err(err) => note(&format!("beat thread: {err}")),
                        }
                    }
                }
                Effect::StopBeat => {
                    if let Some(beat) = &self.beat {
                        beat.stop();
                    }
                }
                Effect::CallInit {
                    payload,
                    deadline_ns,
                } => {
                    let call = CallCtx::new(&self.clock, deadline_ns);
                    self.plane.begin_call(call.clone());
                    self.job(Job::Init { payload, call });
                }
                Effect::CallStart {
                    payload,
                    deadline_ns,
                } => {
                    let call = CallCtx::new(&self.clock, deadline_ns);
                    self.plane.begin_call(call.clone());
                    self.job(Job::Start { payload, call });
                }
                Effect::CallRun => self.call_run(),
                Effect::CallLive { payload } => {
                    let plane = Arc::clone(&self.plane);
                    let events = self.events.clone();
                    self.waiter("mftik-live", move || {
                        let _ = events.send(Event::LiveReturned(plane.live(&payload)));
                    });
                }
                Effect::CallRelay { payload, .. } => self.plane.relay(payload),
                Effect::CallOnStop { deadline_ns } => self.call_on_stop(deadline_ns),
                Effect::Cancel => self.plane.cancel(),
                Effect::ArmTimer {
                    token, deadline_ns, ..
                } => {
                    let events = self.events.clone();
                    self.timers.arm(TimerKey::C4, deadline_ns, move || {
                        let _ = events.send(Event::Timer(token));
                    });
                }
                Effect::Log(line) => note(&line),
                Effect::Exit(code) => exit_with(Some(&*self.out), code),
            }
        }
    }

    fn call_run(&mut self) {
        let Some(disp) = self.plane.dispatcher().cloned() else {
            // Init succeeded, so the dispatcher exists; this cannot happen.
            let _ = self
                .events
                .send(Event::RunReturned(RunResult::Error("dispatcher".into())));
            return;
        };
        self.plane.begin_run();
        self.job(Job::Run);
        let events = self.events.clone();
        self.waiter("mftik-run", move || {
            let _ = events.send(Event::RunReturned(disp.wait_run().into()));
        });
    }

    fn call_on_stop(&mut self, deadline_ns: Option<i64>) {
        match self.plane.dispatcher().cloned() {
            Some(disp) if self.plane.loop_started() => {
                let events = self.events.clone();
                self.waiter("mftik-stop", move || {
                    let result = disp.on_stop(&StopRequest::new(deadline_ns));
                    let _ = events.send(Event::OnStopReturned(result.map_err(CallError::from)));
                });
            }
            _ => self.job(Job::OnStop),
        }
    }

    fn job(&self, job: Job) {
        if self.jobs.send(job).is_err() {
            fatal(&self.out, "dispatcher thread is gone");
        }
    }

    fn waiter(&self, name: &str, f: impl FnOnce() + Send + 'static) {
        if let Err(err) = thread::Builder::new().name(name.into()).spawn(f) {
            fatal(&self.out, &format!("{name}: {err}"));
        }
    }
}
