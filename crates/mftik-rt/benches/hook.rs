//! One hook through the dispatcher, with the event log off and on
//! (E07-D009), as Go's `BenchmarkHook` in `sdk/internal/dispatch`.
//!
//! Each iteration queues one input and runs its hook with `run_one`: a
//! 160-byte MD message, or a TD entry whose input body is that message's 120
//! payload bytes. On, the hook writes two frames into a real [`Log`] whose
//! file discards the bytes, and reads the recorder's clock three times: when
//! the input is queued, when the hook starts, and when it returns. The parts
//! are measured too: one clock read, and one frame appended with a 64-byte
//! body as Go's `BenchmarkAppend`. Report the numbers; no pass threshold.

use std::hint::black_box;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use mftik_core::dispatch::{self, Dispatcher, Input};
use mftik_core::eventlog::{Kind, Recorder};
use mftik_rt::clock::Clock;
use mftik_rt::eventlog::{Config, Header, Log, Recording, Sink};

static PAYLOAD: [u8; 120] = [b'x'; 120];

struct Discard;

impl Sink for Discard {
    fn write_all(&mut self, _bytes: &[u8]) -> io::Result<()> {
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn discarding_log() -> Arc<Log> {
    let log = Log::open(Config {
        dir: Some(std::env::temp_dir()),
        header: Header::mftik(1, "bench"),
        create: Some(Box::new(|_| Ok(Box::new(Discard) as Box<dyn Sink>))),
        ..Config::default()
    })
    .expect("event log");
    Arc::new(log)
}

/// A v1 envelope stamped `md_ts_ns` and [`PAYLOAD`].
fn md_message(md_ts_ns: i64) -> Vec<u8> {
    let mut msg = vec![0u8; 40];
    msg[0] = 1;
    msg[8..16].copy_from_slice(&md_ts_ns.to_le_bytes());
    msg.extend_from_slice(&PAYLOAD);
    msg
}

fn hook(c: &mut Criterion) {
    let clock = Clock::new();
    let msg = md_message(clock.now_ns());
    let input = Input {
        kind: Kind::TD_EVENT,
        r#ref: 7,
        body: Bytes::from_static(&PAYLOAD),
        ..Input::default()
    };
    let mut group = c.benchmark_group("hook");
    for mode in ["off", "on"] {
        let log = (mode == "on").then(discarding_log);
        let disp = Dispatcher::new(dispatch::Config {
            recv_window: Duration::from_secs(3600),
            now_ns: clock.now_ns(),
            ring_bytes: 1 << 20,
            feed: Some(Box::new(|msg| {
                black_box(msg);
            })),
            rec: log
                .as_ref()
                .map(|log| Arc::new(Recording::new(Arc::clone(log), clock)) as Arc<dyn Recorder>),
            ..dispatch::Config::default()
        });
        group.bench_function(format!("md/{mode}"), |b| {
            b.iter(|| {
                let _ = disp.push_md(black_box(&msg));
                disp.run_one();
            });
        });
        group.bench_function(format!("td/{mode}"), |b| {
            b.iter(|| {
                let _ = disp.push_in(black_box(input.clone()), || {});
                disp.run_one();
            });
        });
        if let Some(log) = log {
            log.close();
            let (_, dropped) = log.stats();
            if dropped > 0 {
                eprintln!("{mode}: the ring dropped {dropped} frames");
            }
        }
    }
    group.finish();
}

fn parts(c: &mut Criterion) {
    let clock = Clock::new();
    c.bench_function("clock/now", |b| b.iter(|| black_box(clock.now_ns())));
    let log = discarding_log();
    let body = [0u8; 64];
    let mut i = 0_i64;
    c.bench_function("frame/append", |b| {
        b.iter(|| {
            i += 1;
            black_box(log.append(Kind::MD_FEED, 0, i, i, 0, &[], black_box(&body)));
        });
    });
    log.close();
}

fn config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
        .sample_size(50)
}

criterion_group! {
    name = hooks;
    config = config();
    targets = hook, parts
}
criterion_main!(hooks);
