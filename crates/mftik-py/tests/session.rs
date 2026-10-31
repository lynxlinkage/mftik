//! Python session worker end to end: a wheel-installed `python -m mftik.run`
//! against the fake shim and nats-server (E04-01).
//!
//! `cargo test --workspace` compiles these and does not run them (`#[ignore]`).
//! The python CI job installs the wheel into a venv, points `MFTIK_PYTHON` at
//! that interpreter, and runs this file with `--ignored`.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use mftik_core::eventlog::END_PANIC;
use mftik_proto::wire::mftiko::common::v1::Outcome;
use mftik_proto::wire::mftiko::session::v1::EventLog;
use mftik_proto::wire::mftiko::stsctl::v1::InitPayload;
use mftik_proto::wire::mftiko::worker::v1::{self as worker, Verb, worker_down, worker_up};
use mftik_rt::eventlog::{self, Kind, Reader, Record};
use mftik_testkit::{
    Ended, Nats, Shim, WorkerSpawn, expect_applied, expect_done, expect_ending, expect_hello, init,
    init_payload, start, start_nats,
};

fn python_bin() -> PathBuf {
    PathBuf::from(std::env::var_os("MFTIK_PYTHON").expect("MFTIK_PYTHON"))
}

fn strategies_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/pystrategies")
}

fn package_version() -> String {
    let output = Command::new(python_bin())
        .args(["-c", "import mftik; print(mftik.__version__, end='')"])
        .output()
        .expect("read mftik.__version__");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("version is utf-8")
}

fn python_module(spec: &str) -> WorkerSpawn {
    WorkerSpawn::new(python_bin())
        .args(["-m", "mftik.run", spec])
        .env("PYTHONPATH", strategies_dir())
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .env_remove("MFTIK_RELEASE")
}

fn python_code(code: &str) -> WorkerSpawn {
    WorkerSpawn::new(python_bin())
        .args(["-c", code])
        .env("PYTHONPATH", strategies_dir())
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .env_remove("MFTIK_RELEASE")
}

fn spawn(worker: WorkerSpawn, nats: &Nats) -> Shim {
    Shim::spawn_worker(&nats.url, worker)
}

fn running(shim: &mut Shim, nats: &Nats, scenario: &str, release: &str) {
    running_with(shim, init_payload(&nats.creds, scenario), release);
}

fn running_with(shim: &mut Shim, payload: InitPayload, release: &str) {
    expect_hello(shim, release);
    init(shim, payload, 5000);
    expect_applied(shim, "live-1");
    expect_done(shim, Verb::Init, true, "");
    start(shim);
    expect_done(shim, Verb::Start, true, "");
    match shim.expect() {
        worker_up::Msg::Stage(stage) => assert_eq!(stage.name, "running"),
        other => panic!("want Stage(running), got {other:?}"),
    }
}

fn stop(shim: &Shim) {
    shim.send(worker_down::Msg::Stop(worker::Stop {
        grace_ms: 5000,
        reason: "operator".into(),
    }));
}

fn assert_traceback(ended: &Ended, type_name: &str) {
    assert!(
        ended.stderr.contains("Traceback"),
        "stderr missing traceback: {}",
        ended.stderr
    );
    assert!(
        ended.stderr.contains(type_name),
        "stderr missing {type_name}: {}",
        ended.stderr
    );
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn hello_init_start_stop() {
    let nats = start_nats();
    let release = package_version();
    let mut shim = spawn(python_module("cases:Ok"), &nats);
    running(&mut shim, &nats, "", &release);
    stop(&shim);
    expect_ending(&mut shim, Outcome::Done, "operator", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);
    assert!(
        ended.stdout.contains("{\"hook\":\"gate\"}"),
        "{}",
        ended.stdout
    );
    assert!(ended.stdout.contains("TAIL-stop"), "{}", ended.stdout);
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn release_comes_from_the_environment_then_the_argument() {
    let nats = start_nats();
    let version = package_version();

    let mut shim = spawn(
        python_module("cases:Ok").env("MFTIK_RELEASE", "rel-env"),
        &nats,
    );
    running(&mut shim, &nats, "", "rel-env");
    stop(&shim);
    expect_ending(&mut shim, Outcome::Done, "operator", Some(true));
    assert_eq!(shim.end().code, Some(0));

    let mut shim = spawn(
        python_code("import cases\nimport mftik\nmftik.run(cases.Ok(), release='rel-arg')\n"),
        &nats,
    );
    running(&mut shim, &nats, "", "rel-arg");
    stop(&shim);
    expect_ending(&mut shim, Outcome::Done, "operator", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);
    assert!(ended.stdout.contains("TAIL-stop"), "{}", ended.stdout);

    let mut shim = spawn(
        python_code("import cases\nimport mftik\nmftik.run(cases.Ok(), release='')\n"),
        &nats,
    );
    running(&mut shim, &nats, "", &version);
    stop(&shim);
    expect_ending(&mut shim, Outcome::Done, "operator", Some(true));
    assert_eq!(shim.end().code, Some(0));
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn on_init_exception_is_init_failed_and_flushes_stdout() {
    let nats = start_nats();
    let mut shim = spawn(python_module("cases:InitBoom"), &nats);
    expect_hello(&mut shim, &package_version());
    init(&shim, init_payload(&nats.creds, ""), 5000);
    expect_done(&mut shim, Verb::Init, false, "RuntimeError: init broke");
    expect_ending(
        &mut shim,
        Outcome::InitFailed,
        "RuntimeError: init broke",
        Some(false),
    );
    let ended = shim.end();
    assert_eq!(ended.code, Some(10), "{}", ended.stderr);
    assert!(ended.stdout.contains("TAIL-init"), "{}", ended.stdout);
    assert!(!ended.stdout.contains("TAIL-stop"), "{}", ended.stdout);
    assert!(!ended.stdout.contains("Traceback"), "{}", ended.stdout);
    assert_traceback(&ended, "RuntimeError");
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn on_start_exception_is_init_failed_and_on_stop_runs() {
    let nats = start_nats();
    let mut shim = spawn(python_module("cases:StartBoom"), &nats);
    expect_hello(&mut shim, &package_version());
    init(&shim, init_payload(&nats.creds, ""), 5000);
    expect_applied(&mut shim, "live-1");
    expect_done(&mut shim, Verb::Init, true, "");
    start(&shim);
    expect_done(&mut shim, Verb::Start, false, "ValueError: start broke");
    expect_ending(
        &mut shim,
        Outcome::InitFailed,
        "ValueError: start broke",
        Some(true),
    );
    let ended = shim.end();
    assert_eq!(ended.code, Some(10), "{}", ended.stderr);
    assert!(ended.stdout.contains("TAIL-start"), "{}", ended.stdout);
    assert_traceback(&ended, "ValueError");
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn an_after_callback_exception_is_crashed_and_on_stop_runs() {
    let nats = start_nats();
    let mut shim = spawn(python_module("cases:AfterBoom"), &nats);
    running(&mut shim, &nats, "", &package_version());
    expect_ending(
        &mut shim,
        Outcome::Crashed,
        "hook panic: RuntimeError: after broke",
        Some(true),
    );
    let ended = shim.end();
    assert_eq!(ended.code, Some(20), "{}", ended.stderr);
    assert!(ended.stdout.contains("TAIL-after"), "{}", ended.stdout);
    assert_traceback(&ended, "RuntimeError");
}

/// Every frame in the volume's event log directory, oldest first.
fn event_log(volume: &Path) -> Vec<Record> {
    let mut records = Vec::new();
    for file in eventlog::files(&volume.join(eventlog::DIR)).expect("log files") {
        let mut reader = Reader::open(&file.path).expect("log file");
        records.extend(reader.read_all().expect("frames"));
    }
    records
}

/// E07-D008: the exception is the `hook_end`'s text, flagged as a panic.
#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn an_after_callback_exception_still_gets_its_hook_end() {
    let nats = start_nats();
    let mut shim = spawn(python_module("cases:AfterBoom"), &nats);
    let mut payload = init_payload(&nats.creds, "");
    payload.event_log = Some(EventLog {
        enabled: true,
        keep_mb: 1,
    });
    running_with(&mut shim, payload, &package_version());
    expect_ending(
        &mut shim,
        Outcome::Crashed,
        "hook panic: RuntimeError: after broke",
        Some(true),
    );
    let ended = shim.end();
    assert_eq!(ended.code, Some(20), "{}", ended.stderr);

    let records = event_log(&ended.volume.0);
    let frames: Vec<_> = records
        .iter()
        .map(|r| (r.seq, r.kind, r.r#ref, r.flags))
        .collect();
    assert_eq!(
        frames,
        [
            (1, Kind::TIMER, 1, 0),
            (2, Kind::HOOK_END, 1, END_PANIC),
            (3, Kind::END, 0, 0),
        ]
    );
    assert_eq!(records[1].body, b"RuntimeError: after broke");
    assert!(records[0].t_ns <= records[1].t_ns);
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn a_context_dies_with_its_hook_and_poster_post_still_runs() {
    let nats = start_nats();
    let mut shim = spawn(python_module("cases:CtxLife"), &nats);
    running(&mut shim, &nats, "ctx", &package_version());
    expect_ending(&mut shim, Outcome::Done, "", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);
    assert!(ended.stdout.contains("TAIL-ctx"), "{}", ended.stdout);
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn the_gil_is_released_between_hooks() {
    let nats = start_nats();
    let mut shim = spawn(python_module("cases:GilGap"), &nats);
    running(&mut shim, &nats, "", &package_version());
    expect_ending(&mut shim, Outcome::Done, "", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);
    assert!(ended.stdout.contains("TAIL-gil"), "{}", ended.stdout);
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn a_hook_that_does_not_return_is_hung_and_drops_its_stdout_buffer() {
    let nats = start_nats();
    let mut shim = spawn(python_module("cases:Hung"), &nats);
    running(&mut shim, &nats, "", &package_version());
    let started = Instant::now();
    expect_ending(&mut shim, Outcome::Hung, "hook timeout", Some(false));
    assert!(started.elapsed() < Duration::from_secs(10));
    let ended = shim.end();
    assert_eq!(ended.code, Some(21), "{}", ended.stderr);
    assert!(
        !ended.stdout.contains("TAIL-hung"),
        "hung hook buffer was flushed: {}",
        ended.stdout
    );
    assert!(!ended.stdout.contains("TAIL-stop"), "{}", ended.stdout);
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn sys_exit_and_keyboard_interrupt_are_init_failed() {
    let nats = start_nats();
    let release = package_version();

    let mut shim = spawn(python_module("cases:SysExitInit"), &nats);
    expect_hello(&mut shim, &release);
    init(&shim, init_payload(&nats.creds, ""), 5000);
    expect_done(&mut shim, Verb::Init, false, "SystemExit: stop-via-sys");
    expect_ending(
        &mut shim,
        Outcome::InitFailed,
        "SystemExit: stop-via-sys",
        Some(false),
    );
    let ended = shim.end();
    assert_eq!(ended.code, Some(10), "{}", ended.stderr);
    assert!(ended.stdout.contains("TAIL-sysexit"), "{}", ended.stdout);
    assert_traceback(&ended, "SystemExit");

    let mut shim = spawn(python_module("cases:KeyInt"), &nats);
    expect_hello(&mut shim, &release);
    init(&shim, init_payload(&nats.creds, ""), 5000);
    expect_done(&mut shim, Verb::Init, false, "KeyboardInterrupt: tick");
    expect_ending(
        &mut shim,
        Outcome::InitFailed,
        "KeyboardInterrupt: tick",
        Some(false),
    );
    let ended = shim.end();
    assert_eq!(ended.code, Some(10), "{}", ended.stderr);
    assert!(ended.stdout.contains("TAIL-keyint"), "{}", ended.stdout);
    assert_traceback(&ended, "KeyboardInterrupt");
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn an_on_stop_exception_matches_a_rust_on_stop_panic() {
    let nats = start_nats();
    let mut shim = spawn(python_module("cases:StopBoom"), &nats);
    running(&mut shim, &nats, "", &package_version());
    stop(&shim);
    expect_ending(&mut shim, Outcome::Done, "operator", Some(false));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);
    assert!(ended.stdout.contains("TAIL-onstop"), "{}", ended.stdout);
    assert!(
        ended
            .stderr
            .contains("on stop: dispatch: onstop panic: RuntimeError: stop broke"),
        "{}",
        ended.stderr
    );
    assert_traceback(&ended, "RuntimeError");
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn a_non_finite_after_delay_is_a_value_error() {
    let nats = start_nats();
    let mut shim = spawn(python_module("cases:NanDelay"), &nats);
    expect_hello(&mut shim, &package_version());
    init(&shim, init_payload(&nats.creds, ""), 5000);
    let error = "ValueError: delay must be a finite number of seconds >= 0";
    expect_done(&mut shim, Verb::Init, false, error);
    expect_ending(&mut shim, Outcome::InitFailed, error, Some(false));
    let ended = shim.end();
    assert_eq!(ended.code, Some(10), "{}", ended.stderr);
    assert!(ended.stdout.contains("TAIL-nan"), "{}", ended.stdout);
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn import_construct_and_type_failures_are_init_failed() {
    let nats = start_nats();
    let release = package_version();

    let mut shim = spawn(python_module("no_such_mod:Missing"), &nats);
    expect_hello(&mut shim, &release);
    init(&shim, init_payload(&nats.creds, ""), 5000);
    let missing = "load no_such_mod:Missing: ModuleNotFoundError: No module named 'no_such_mod'";
    expect_done(&mut shim, Verb::Init, false, missing);
    expect_ending(&mut shim, Outcome::InitFailed, missing, Some(false));
    let ended = shim.end();
    assert_eq!(ended.code, Some(10), "{}", ended.stderr);
    assert!(!ended.stdout.contains("Traceback"), "{}", ended.stdout);
    assert_traceback(&ended, "ModuleNotFoundError");

    let mut shim = spawn(python_module("badimport:Unused"), &nats);
    expect_hello(&mut shim, &release);
    init(&shim, init_payload(&nats.creds, ""), 5000);
    let broken = "load badimport:Unused: ImportError: missing piece";
    expect_done(&mut shim, Verb::Init, false, broken);
    expect_ending(&mut shim, Outcome::InitFailed, broken, Some(false));
    let ended = shim.end();
    assert_eq!(ended.code, Some(10), "{}", ended.stderr);
    assert!(ended.stdout.contains("TAIL-import"), "{}", ended.stdout);
    assert!(!ended.stdout.contains("Traceback"), "{}", ended.stdout);
    assert_traceback(&ended, "ImportError");

    let mut shim = spawn(python_module("cases:Plain"), &nats);
    expect_hello(&mut shim, &release);
    init(&shim, init_payload(&nats.creds, ""), 5000);
    let plain = "load cases:Plain: TypeError: cases:Plain is not a mftik.Strategy";
    expect_done(&mut shim, Verb::Init, false, plain);
    expect_ending(&mut shim, Outcome::InitFailed, plain, Some(false));
    assert_eq!(shim.end().code, Some(10));

    let mut shim = spawn(python_module("not-a-spec"), &nats);
    expect_hello(&mut shim, &release);
    init(&shim, init_payload(&nats.creds, ""), 5000);
    let spec = "load not-a-spec: ValueError: want module:Class, got 'not-a-spec'";
    expect_done(&mut shim, Verb::Init, false, spec);
    expect_ending(&mut shim, Outcome::InitFailed, spec, Some(false));
    assert_eq!(shim.end().code, Some(10));
}
