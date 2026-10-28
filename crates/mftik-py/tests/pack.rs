//! Unpacked strategy rootfs against the fake shim (E04-07, PROTOCOL §9.1).
//!
//! `cargo test --workspace` compiles this and does not run it (`#[ignore]`).
//! The pack CI job sets `MFTIK_PACK_LAUNCH` to a file describing the rootfs
//! `internal/platform/artifact` unpacked, and `MFTIK_NATS_SERVER`. The
//! data-input job's launch file also names the read-only input directory and
//! the sha256 the strategy must read from it.

use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

use mftik_proto::wire::mftiko::common::v1::Outcome;
use mftik_proto::wire::mftiko::worker::v1::{self as worker, Verb, worker_down, worker_up};
use mftik_testkit::{
    Shim, WorkerSpawn, expect_applied, expect_done, expect_ending, expect_hello, init,
    init_payload, start, start_nats,
};

const HOST_LEAK: &[&str] = &[
    "PYTHONPATH",
    "PYTHONHOME",
    "PYTHONSTARTUP",
    "PYTHONUSERBASE",
    "PYTHONPYCACHEPREFIX",
    "VIRTUAL_ENV",
    "PYTHONEXECUTABLE",
    "PYTHONDONTWRITEBYTECODE",
    "MFTIK_RELEASE",
    "PATH",
    "LD_LIBRARY_PATH",
];

struct Launch {
    rootfs: PathBuf,
    program: PathBuf,
    args: Vec<String>,
    env: Vec<(String, String)>,
    release: String,
    /// `MFTIKO_INPUT_DIR`, laid out as the STS agent stages it.
    input_dir: Option<PathBuf>,
    /// sha256 of the input file the strategy reads.
    sha256: Option<String>,
}

fn load_launch() -> Launch {
    let path = std::env::var_os("MFTIK_PACK_LAUNCH").expect("MFTIK_PACK_LAUNCH");
    let text = fs::read_to_string(path).expect("launch file");
    let mut rootfs = None;
    let mut program = None;
    let mut release = None;
    let mut args = Vec::new();
    let mut env = Vec::new();
    let mut input_dir = None;
    let mut sha256 = None;
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        let mut parts = line.split('\t');
        let kind = parts.next().expect("field");
        match kind {
            "rootfs" => rootfs = Some(PathBuf::from(parts.next().expect("rootfs"))),
            "program" => program = Some(PathBuf::from(parts.next().expect("program"))),
            "release" => release = Some(parts.next().expect("release").to_owned()),
            "input_dir" => input_dir = Some(PathBuf::from(parts.next().expect("input_dir"))),
            "sha256" => sha256 = Some(parts.next().expect("sha256").to_owned()),
            "arg" => args.push(parts.next().expect("arg").to_owned()),
            "env" => env.push((
                parts.next().expect("env key").to_owned(),
                parts.next().expect("env value").to_owned(),
            )),
            other => panic!("launch field {other}"),
        }
    }
    Launch {
        rootfs: rootfs.expect("rootfs"),
        program: program.expect("program"),
        args,
        env,
        release: release.expect("release"),
        input_dir,
        sha256,
    }
}

fn field(stdout: &str, name: &str) -> String {
    let prefix = format!("{name} ");
    for line in stdout.lines() {
        if let Some(value) = line.strip_prefix(&prefix) {
            return value.to_owned();
        }
    }
    panic!("missing {name} in {stdout}");
}

fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
    let mut entries: Vec<_> = fs::read_dir(dir)
        .unwrap_or_else(|err| panic!("read {}: {err}", dir.display()))
        .map(|entry| entry.expect("dir entry").path())
        .collect();
    entries.sort();
    for path in entries {
        out.push(path.clone());
        let meta = fs::symlink_metadata(&path).expect("metadata");
        if meta.is_dir() {
            collect(&path, out);
        }
    }
}

fn fingerprint(root: &Path) -> u64 {
    let mut paths = Vec::new();
    collect(root, &mut paths);
    let mut hasher = DefaultHasher::new();
    paths.len().hash(&mut hasher);
    for path in paths {
        let rel = path.strip_prefix(root).expect("relative");
        rel.to_string_lossy().hash(&mut hasher);
        let meta = fs::symlink_metadata(&path).expect("metadata");
        meta.file_type().is_dir().hash(&mut hasher);
        meta.file_type().is_symlink().hash(&mut hasher);
        if let Ok(modified) = meta.modified()
            && let Ok(since) = modified.duration_since(UNIX_EPOCH)
        {
            since.as_nanos().hash(&mut hasher);
        }
        if meta.file_type().is_symlink() {
            fs::read_link(&path)
                .expect("link")
                .to_string_lossy()
                .hash(&mut hasher);
        } else if meta.is_file() {
            fs::read(&path).expect("file").hash(&mut hasher);
        }
    }
    hasher.finish()
}

/// Spawns the rootfs entrypoint with no host Python on `PATH`, runs Init,
/// Start, and Stop, and returns the worker's stdout and stderr.
fn run_rootfs(launch: &Launch, extra_env: &[(&str, &Path)]) -> (String, String) {
    let nats = start_nats();
    let path_dir = std::env::temp_dir().join(format!("mftik-pack-path-{}", std::process::id()));
    let _ = fs::remove_dir_all(&path_dir);
    fs::create_dir_all(&path_dir).expect("empty PATH");

    let mut worker = WorkerSpawn::new(launch.rootfs.join(&launch.program))
        .args(&launch.args)
        .current_dir(&launch.rootfs);
    for key in HOST_LEAK {
        worker = worker.env_remove(key);
    }
    worker = worker.env("PATH", &path_dir);
    for (key, value) in &launch.env {
        worker = worker.env(key, value);
    }
    for (key, value) in extra_env {
        worker = worker.env(key, value);
    }

    let mut shim = Shim::spawn_worker(&nats.url, worker);
    expect_hello(&mut shim, &launch.release);
    init(&shim, init_payload(&nats.creds, ""), 15_000);
    expect_applied(&mut shim, "live-1");
    expect_done(&mut shim, Verb::Init, true, "");
    start(&shim);
    expect_done(&mut shim, Verb::Start, true, "");
    match shim.expect() {
        worker_up::Msg::Stage(stage) => assert_eq!(stage.name, "running"),
        other => panic!("want Stage(running), got {other:?}"),
    }
    shim.send(worker_down::Msg::Stop(worker::Stop {
        grace_ms: 5000,
        reason: "operator".into(),
    }));
    expect_ending(&mut shim, Outcome::Done, "operator", Some(true));
    let ended = shim.end();
    assert_eq!(ended.code, Some(0), "{}", ended.stderr);
    let _ = fs::remove_dir_all(&path_dir);
    (ended.stdout, ended.stderr)
}

#[test]
#[ignore = "pack CI job: MFTIK_PACK_LAUNCH describes the unpacked rootfs"]
fn unpacked_rootfs_runs_without_host_python() {
    let launch = load_launch();
    let before = fingerprint(&launch.rootfs);
    let (stdout, _stderr) = run_rootfs(&launch, &[]);
    assert!(stdout.contains("PACK-init"), "{stdout}");
    assert!(stdout.contains("PACK-start"), "{stdout}");
    assert!(stdout.contains("PACK-stop"), "{stdout}");
    assert!(stdout.contains("WHICH None"), "{stdout}");
    let root = launch.rootfs.to_string_lossy().into_owned();
    let exe = field(&stdout, "EXE");
    let prefix = field(&stdout, "PREFIX");
    assert!(exe.starts_with(&root), "{exe}");
    assert!(prefix.starts_with(&root), "{prefix}");
    let after = fingerprint(&launch.rootfs);
    assert_eq!(before, after, "rootfs changed while the worker ran");
}

/// The STS agent unpacks an input read-only (mftiko E09-D075). Running as the
/// worker's user, a new file and a write to an existing one are both refused.
fn assert_read_only(dir: &Path, existing: &Path) {
    let created = dir.join("written-by-test");
    let err = fs::write(&created, b"x").expect_err("create in a read-only input");
    assert_eq!(err.kind(), ErrorKind::PermissionDenied, "{err}");
    let err = fs::OpenOptions::new()
        .write(true)
        .open(existing)
        .expect_err("write to a read-only input file");
    assert_eq!(err.kind(), ErrorKind::PermissionDenied, "{err}");
}

#[test]
#[ignore = "data-input CI job: MFTIK_PACK_LAUNCH also names the input"]
fn unpacked_rootfs_reads_a_data_input() {
    let launch = load_launch();
    let inputs = launch
        .input_dir
        .clone()
        .expect("input_dir in the launch file");
    let sha256 = launch.sha256.clone().expect("sha256 in the launch file");
    let model = inputs.join("model");
    let rootfs_before = fingerprint(&launch.rootfs);
    let input_before = fingerprint(&model);

    let (_stdout, stderr) = run_rootfs(&launch, &[("MFTIKO_INPUT_DIR", &inputs)]);
    let init = stderr
        .lines()
        .find(|line| line.contains(r#""hook":"OnInit""#))
        .unwrap_or_else(|| panic!("no OnInit line in {stderr}"));
    assert!(init.contains(&format!(r#""sha256":"{sha256}""#)), "{init}");
    assert!(stderr.contains(r#""hook":"OnStop""#), "{stderr}");

    assert_read_only(&model, &model.join("weights.json"));
    assert_eq!(input_before, fingerprint(&model), "input changed");
    assert_eq!(rootfs_before, fingerprint(&launch.rootfs), "rootfs changed");
}
