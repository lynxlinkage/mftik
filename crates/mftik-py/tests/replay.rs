//! Record `examples/python/sampler`, then `mftik.replay` (E07-D024).
//!
//! `cargo test --workspace` compiles this and does not run it (`#[ignore]`).
//! The python CI job installs the wheel, points `MFTIK_PYTHON` at that
//! interpreter, and runs this file with `--ignored`.

use std::path::{Path, PathBuf};
use std::process::Command;

use mftik_proto::wire::mftiko::common::v1::Outcome;
use mftik_proto::wire::mftiko::session::v1::EventLog;
use mftik_proto::wire::mftiko::worker::v1::{self as worker, Verb, worker_down, worker_up};
use mftik_rt::eventlog;
use mftik_testkit::{
    Nats, Shim, WorkerSpawn, expect_applied, expect_done, expect_ending, expect_hello, init,
    init_payload, start, start_nats,
};

fn python_bin() -> PathBuf {
    PathBuf::from(std::env::var_os("MFTIK_PYTHON").expect("MFTIK_PYTHON"))
}

fn sampler_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../examples/python/sampler")
        .canonicalize()
        .expect("examples/python/sampler")
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

fn spawn_sampler(nats: &Nats) -> Shim {
    let worker = WorkerSpawn::new(python_bin())
        .args(["-m", "mftik.run", "sampler:Sampler"])
        .env("PYTHONPATH", sampler_dir())
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .env_remove("MFTIK_RELEASE");
    Shim::spawn_worker(&nats.url, worker)
}

fn running(shim: &mut Shim, nats: &Nats, release: &str) {
    expect_hello(shim, release);
    let mut payload = init_payload(&nats.creds, r#"{"every_s":30}"#);
    payload.event_log = Some(EventLog {
        enabled: true,
        keep_mb: 1,
    });
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

/// `body` defines `Cls`. `sys.argv[1]` is the event-log directory.
fn python_replay(dir: &Path, body: &str) -> String {
    let script = format!(
        r#"
import sys
import mftik
from sampler import Sampler
{body}
report = mftik.replay(Cls, sys.argv[1])
print("session_id", report.session_id)
print("inputs", report.inputs)
print("skipped", report.skipped)
print("missing", report.missing)
print("orders", report.orders)
print("reads", report.reads)
print("n_mismatches", len(report.mismatches))
for line in report.mismatches:
    print("mismatch", line)
"#
    );
    let output = Command::new(python_bin())
        .args(["-c", &script, dir.to_str().expect("utf-8")])
        .env("PYTHONPATH", sampler_dir())
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()
        .expect("python replay");
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("utf-8")
}

#[test]
#[ignore = "python CI job: MFTIK_PYTHON is the venv whose wheel is installed"]
fn sampler_replays_with_no_mismatch_and_a_changed_strategy_mismatches() {
    let nats = start_nats();
    let mut shim = spawn_sampler(&nats);
    running(&mut shim, &nats, &package_version());
    shim.send(worker_down::Msg::Stop(worker::Stop {
        grace_ms: 5000,
        reason: "operator".into(),
    }));
    expect_ending(&mut shim, Outcome::Done, "operator", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);

    let dir = ended.volume.0.join(eventlog::DIR);
    let matched = python_replay(
        &dir,
        r#"
Cls = Sampler
first = mftik.replay(Sampler, sys.argv[1])
if first.mismatches:
    raise SystemExit(repr(first.mismatches))
"#,
    );
    assert!(matched.contains("session_id c0ffee"), "{matched}");
    assert!(matched.contains("n_mismatches 0"), "{matched}");
    assert!(matched.contains("skipped 0"), "{matched}");
    assert!(matched.contains("missing 0"), "{matched}");
    assert!(matched.contains("orders 0"), "{matched}");
    assert!(matched.contains("reads 0"), "{matched}");

    let diverged = python_replay(
        &dir,
        r#"
class Cls(Sampler):
    def on_start(self, ctx):
        raise RuntimeError("changed")
"#,
    );
    assert!(
        diverged
            .lines()
            .any(|line| line.starts_with("n_mismatches ") && line != "n_mismatches 0"),
        "{diverged}"
    );
    assert!(
        diverged.contains("changed"),
        "changed strategy must mismatch: {diverged}"
    );
}
