//! What the end-to-end tests share: nats-server with operator JWT auth, and
//! a fake shim on a socketpair handed to the worker as the control fd
//! (E03-05, E03-D056).
//!
//! Each test binary has its own `worker_entry` test that runs its strategy
//! when `WORKER_ENV` is set; [`Shim::spawn`] re-runs the binary filtered to it.

#![allow(dead_code)] // Each test binary uses part of it.

use std::io::{BufRead, BufReader, Read};
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{env, fs};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use mftik_proto::wire::mftiko::common::v1::{Outcome, Version};
use mftik_proto::wire::mftiko::stsctl::v1::{Bindings, InitPayload, NatsCredentials, StartPayload};
use mftik_proto::wire::mftiko::worker::v1::{
    self as worker, Verb, WorkerDown, WorkerUp, worker_down, worker_up,
};
use mftik_rt::frame::{FrameReader, FrameWriter};
use nkeys::KeyPair;
use prost::Message;

/// `Hello.release` the test worker passes to `run` (E03-D057).
pub const TEST_RELEASE: &str = "conform-test 1.2.3";
pub const WORKER_ENV: &str = "MFTIK_RT_TEST_WORKER";
pub const NATS_SERVER_ENV: &str = "MFTIK_NATS_SERVER";
pub const FUSE: Duration = Duration::from_secs(20);

// ---------- nats-server with operator JWT auth ----------

pub fn b64(bytes: &[u8]) -> String {
    URL_SAFE_NO_PAD.encode(bytes)
}

pub fn now_s() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after 1970")
        .as_secs()
}

/// A NATS JWT (nats-io/jwt v2) for `sub`, signed by `issuer`.
pub fn jwt(issuer: &KeyPair, sub: &str, name: &str, nats: &str) -> String {
    static JTI: AtomicU64 = AtomicU64::new(0);
    let header = b64(br#"{"typ":"JWT","alg":"ed25519-nkey"}"#);
    let claims = format!(
        r#"{{"jti":"MFTIKRTTEST{}","iat":{},"iss":"{}","name":"{name}","sub":"{sub}","nats":{nats}}}"#,
        JTI.fetch_add(1, Ordering::Relaxed),
        now_s(),
        issuer.public_key()
    );
    let body = format!("{header}.{}", b64(claims.as_bytes()));
    let sig = issuer.sign(body.as_bytes()).expect("sign jwt");
    format!("{body}.{}", b64(&sig))
}

pub const ACCOUNT: &str = r#"{"limits":{"subs":-1,"data":-1,"payload":-1,"imports":-1,"exports":-1,"wildcards":true,"conn":-1,"leaf":-1},"default_permissions":{"pub":{},"sub":{}},"type":"account","version":2}"#;
pub const USER: &str =
    r#"{"pub":{},"sub":{},"subs":-1,"data":-1,"payload":-1,"type":"user","version":2}"#;

pub struct TempDir(pub PathBuf);

impl TempDir {
    pub fn new(tag: &str) -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let path = env::temp_dir().join(format!(
            "mftik-rt-session-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).expect("temp dir");
        Self(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub struct Nats {
    pub child: Child,
    pub url: String,
    /// What the session's `InitPayload.nats` carries.
    pub creds: NatsCredentials,
    pub _dir: TempDir,
}

impl Drop for Nats {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub fn nats_server_bin() -> PathBuf {
    if let Some(path) = env::var_os(NATS_SERVER_ENV) {
        return PathBuf::from(path);
    }
    let found = env::var_os("PATH").and_then(|paths| {
        env::split_paths(&paths)
            .map(|dir| dir.join("nats-server"))
            .find(|bin| bin.is_file())
    });
    found.unwrap_or_else(|| {
        panic!(
            "nats-server not found: set {NATS_SERVER_ENV} or put it on PATH \
             (go install github.com/nats-io/nats-server/v2@v2.15.0)"
        )
    })
}

pub fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

/// Operator mode with a memory resolver: one operator, a system account, and
/// the session's account, whose user is the session (E09-D006).
pub fn start_nats() -> Nats {
    let dir = TempDir::new("nats");
    let operator = KeyPair::new_operator();
    let system = KeyPair::new_account();
    let account = KeyPair::new_account();
    let user = KeyPair::new_user();
    let op_jwt = jwt(
        &operator,
        &operator.public_key(),
        "mftik-rt-test",
        r#"{"type":"operator","version":2}"#,
    );
    let sys_jwt = jwt(&operator, &system.public_key(), "SYS", ACCOUNT);
    let acc_jwt = jwt(&operator, &account.public_key(), "MFTIKO", ACCOUNT);
    let user_jwt = jwt(&account, &user.public_key(), "session-c0ffee", USER);
    let port = free_port();
    let op_path = dir.0.join("operator.jwt");
    fs::write(&op_path, &op_jwt).expect("operator jwt");
    let conf = format!(
        "listen: 127.0.0.1:{port}\n\
         operator: {op:?}\n\
         system_account: {sys}\n\
         resolver: MEMORY\n\
         resolver_preload: {{\n  {sys}: {sys_jwt:?}\n  {acc}: {acc_jwt:?}\n}}\n\
         max_control_line: 262144\n",
        op = op_path.to_str().expect("utf-8 path"),
        sys = system.public_key(),
        acc = account.public_key(),
    );
    let conf_path = dir.0.join("nats-server.conf");
    fs::write(&conf_path, conf).expect("server config");
    let child = Command::new(nats_server_bin())
        .arg("-c")
        .arg(&conf_path)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start nats-server");
    let start = Instant::now();
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(start.elapsed() < FUSE, "nats-server did not listen");
        thread::sleep(Duration::from_millis(20));
    }
    Nats {
        child,
        url: format!("nats://127.0.0.1:{port}"),
        creds: NatsCredentials {
            user_jwt,
            seed: user.seed().expect("user seed"),
            inbox_prefix: "_INBOX_c0ffee".into(),
        },
        _dir: dir,
    }
}

// ---------- the fake shim ----------

pub struct Shim {
    pub child: Child,
    pub sock: UnixStream,
    pub frames: FrameReader<UnixStream>,
    pub out: FrameWriter<UnixStream>,
    pub beats: u64,
    pub volume: TempDir,
}

pub struct Ended {
    pub code: Option<i32>,
    /// The JSON lines the worker printed, in order.
    pub lines: Vec<String>,
    pub stderr: String,
}

impl Shim {
    pub fn spawn(nats_url: &str) -> Shim {
        let (ours, theirs) = UnixStream::pair().expect("socketpair");
        ours.set_read_timeout(Some(FUSE)).expect("read timeout");
        let volume = TempDir::new("volume");
        let fd = theirs.as_raw_fd();
        let mut cmd = Command::new(env::current_exe().expect("test binary"));
        cmd.args(["worker_entry", "--exact", "--nocapture", "--test-threads=1"])
            .env(WORKER_ENV, "1")
            .env("MFTIKO_CTL_FD", "3")
            .env("MFTIKO_NATS_URL", nats_url)
            .env("MFTIKO_VOLUME_DIR", &volume.0)
            .env_remove("MFTIKO_INPUT_DIR")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        // SAFETY: dup2 is async-signal-safe. The new fd 3 has no
        // close-on-exec, so the worker inherits it as the shim's would.
        unsafe {
            cmd.pre_exec(move || {
                if libc::dup2(fd, 3) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = cmd.spawn().expect("spawn worker");
        drop(theirs);
        let write = ours.try_clone().expect("clone socket");
        let sock = ours.try_clone().expect("clone socket");
        Shim {
            child,
            sock,
            frames: FrameReader::new(ours),
            out: FrameWriter::new(write),
            beats: 0,
            volume,
        }
    }

    pub fn send(&self, msg: worker_down::Msg) {
        self.out
            .send(&WorkerDown { msg: Some(msg) })
            .expect("send to worker");
    }

    /// The next frame that is not Beat. `None` at EOF.
    pub fn next(&mut self) -> Option<worker_up::Msg> {
        loop {
            let up: WorkerUp = self.frames.read_message().expect("read from worker")?;
            match up.msg.expect("a WorkerUp message") {
                worker_up::Msg::Beat(beat) => {
                    self.beats += 1;
                    assert_eq!(beat.seq, self.beats, "beat seq");
                }
                other => return Some(other),
            }
        }
    }

    #[track_caller]
    pub fn expect(&mut self) -> worker_up::Msg {
        self.next().expect("worker closed fd 3 early")
    }

    /// Reads until a Beat arrives.
    pub fn wait_beat(&mut self) {
        let want = self.beats + 1;
        while self.beats < want {
            let up: WorkerUp = self
                .frames
                .read_message()
                .expect("read from worker")
                .expect("worker closed fd 3 early");
            match up.msg.expect("a WorkerUp message") {
                worker_up::Msg::Beat(beat) => {
                    self.beats += 1;
                    assert_eq!(beat.seq, self.beats, "beat seq");
                }
                other => panic!("unexpected {other:?} while waiting for Beat"),
            }
        }
    }

    pub fn end(mut self) -> Ended {
        assert!(self.next().is_none(), "frames after the last one");
        let start = Instant::now();
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("wait") {
                break status;
            }
            assert!(start.elapsed() < FUSE, "worker did not exit");
            thread::sleep(Duration::from_millis(10));
        };
        let mut stdout = String::new();
        let mut stderr = String::new();
        let _ = self
            .child
            .stdout
            .take()
            .expect("stdout")
            .read_to_string(&mut stdout);
        let _ = self
            .child
            .stderr
            .take()
            .expect("stderr")
            .read_to_string(&mut stderr);
        let lines = BufReader::new(stdout.as_bytes())
            .lines()
            .map_while(Result::ok)
            // libtest's `test worker_entry ... ` has no newline before them.
            .filter_map(|line| line.find("{\"hook\"").map(|at| line[at..].to_owned()))
            .collect();
        Ended {
            code: status.code(),
            lines,
            stderr,
        }
    }
}

pub fn hello(shim: &mut Shim) {
    match shim.expect() {
        worker_up::Msg::Hello(hello) => {
            assert_eq!(hello.kind, "session");
            assert_eq!(
                hello.contracts,
                ["worker.v1", "stsctl.v1", "md.v1", "td.v1"]
            );
            assert_eq!(hello.pid as u32, shim.child.id());
            assert_eq!(hello.release, TEST_RELEASE);
        }
        other => panic!("want Hello, got {other:?}"),
    }
}

pub fn init_payload(creds: &NatsCredentials, scenario: &str) -> InitPayload {
    InitPayload {
        session_id: "c0ffee".into(),
        params: scenario.as_bytes().to_vec(),
        nats: Some(creds.clone()),
        recv_window_ms: 500,
        hook_timeout_s: 1,
        ..Default::default()
    }
}

pub fn init(shim: &Shim, payload: InitPayload, deadline_ms: u32) {
    shim.send(worker_down::Msg::Init(worker::Init {
        contract: "stsctl.v1".into(),
        payload: payload.encode_to_vec(),
        deadline_ms,
        live_digest: "live-1".into(),
    }));
}

pub fn bindings(seq: u64) -> Bindings {
    Bindings {
        version: Some(Version { epoch: 1, seq }),
        ..Default::default()
    }
}

pub fn start(shim: &Shim) {
    start_with(shim, bindings(1));
}

pub fn start_with(shim: &Shim, bindings: Bindings) {
    shim.send(worker_down::Msg::Start(worker::Start {
        contract: "stsctl.v1".into(),
        payload: StartPayload {
            bindings: Some(bindings),
        }
        .encode_to_vec(),
        deadline_ms: 5000,
    }));
}

#[track_caller]
pub fn expect_done(shim: &mut Shim, verb: Verb, ok: bool, error: &str) {
    match shim.expect() {
        worker_up::Msg::Done(done) => {
            assert_eq!(done.verb, verb as i32);
            assert_eq!(done.ok, ok);
            assert_eq!(done.error, error);
        }
        other => panic!("want Done({verb:?}), got {other:?}"),
    }
}

#[track_caller]
pub fn expect_applied(shim: &mut Shim, digest: &str) {
    match shim.expect() {
        worker_up::Msg::Applied(applied) => assert_eq!(applied.digest, digest),
        other => panic!("want Applied({digest}), got {other:?}"),
    }
}

#[track_caller]
pub fn expect_ending(shim: &mut Shim, outcome: Outcome, reason: &str, stopped: Option<bool>) {
    match shim.expect() {
        worker_up::Msg::Exiting(exiting) => {
            assert_eq!(exiting.outcome, outcome as i32, "{exiting:?}");
            assert_eq!(exiting.reason, reason);
        }
        other => panic!("want Exiting({outcome:?}), got {other:?}"),
    }
    if let Some(completed) = stopped {
        match shim.expect() {
            worker_up::Msg::Stopped(s) => assert_eq!(s.on_stop_completed, completed),
            other => panic!("want Stopped, got {other:?}"),
        }
    }
}

/// Hello, Init (with the live digest's Applied), Start, and Stage(running).
pub fn up_to_running(shim: &mut Shim, nats: &Nats, scenario: &str) {
    hello(shim);
    init(shim, init_payload(&nats.creds, scenario), 5000);
    expect_applied(shim, "live-1");
    expect_done(shim, Verb::Init, true, "");
    start(shim);
    expect_done(shim, Verb::Start, true, "");
    match shim.expect() {
        worker_up::Msg::Stage(stage) => assert_eq!(stage.name, "running"),
        other => panic!("want Stage(running), got {other:?}"),
    }
}

pub fn hooks(ended: &Ended) -> Vec<String> {
    ended
        .lines
        .iter()
        .map(|line| {
            let start = line.find("\"hook\":\"").expect("hook field") + 8;
            let end = start + line[start..].find('"').expect("hook end");
            line[start..end].to_owned()
        })
        .collect()
}

pub fn assert_same_thread(ended: &Ended) {
    for line in &ended.lines {
        if line.contains("same_thread") {
            assert!(line.contains("\"same_thread\":true"), "{line}");
        }
    }
}

pub fn read_volume(dir: &Path) -> Vec<u8> {
    fs::read(dir.join("state")).expect("volume file")
}
