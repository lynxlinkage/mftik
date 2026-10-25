//! `MFTIKO_CTL_FD` and fd 3 in a real child process (PROTOCOL §5.1, §9.2.1).
//!
//! Each test starts this test binary again as the worker, with the
//! socketpair end at fd 3 and close-on-exec cleared, as the shim leaves it.
//! The child runs [`child_entry`] and does what `MFTIK_RT_TEST_CHILD` says.

use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::net::UnixStream;
use std::os::unix::process::{CommandExt, ExitStatusExt};
use std::process::{Command, ExitStatus, Stdio};
use std::time::Duration;

use mftik_proto::wire::mftiko::worker::v1::{Beat, Hello, WorkerUp, worker_up};
use mftik_rt::ctl::{CTL_FD_ENV, CtlFd, EXIT_BAD_ENV, EXIT_BAD_FD};
use mftik_rt::frame::{FrameError, FrameReader, FrameWriter};

const ROLE_ENV: &str = "MFTIK_RT_TEST_CHILD";

/// A command that runs this binary's [`child_entry`] in `role`, with no
/// `MFTIKO_CTL_FD` unless the test sets one.
fn child(role: &str) -> Command {
    let mut cmd = Command::new(std::env::current_exe().expect("test binary"));
    cmd.args([
        "child_entry",
        "--exact",
        "--ignored",
        "--nocapture",
        "--test-threads=1",
    ])
    .env(ROLE_ENV, role)
    .env_remove(CTL_FD_ENV)
    .stdin(Stdio::null())
    .stdout(Stdio::null());
    cmd
}

/// Puts `fd` at fd 3 in the child without close-on-exec, as the shim does.
fn hand_over_fd3(cmd: &mut Command, fd: RawFd) {
    // SAFETY: dup2 and fcntl are async-signal-safe.
    unsafe {
        cmd.pre_exec(move || {
            let ok = if fd == 3 {
                libc::fcntl(3, libc::F_SETFD, 0) != -1
            } else {
                libc::dup2(fd, 3) != -1
            };
            if ok {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        });
    }
}

/// Makes sure nothing is open at `fd` in the child.
fn close_in_child(cmd: &mut Command, fd: RawFd) {
    // SAFETY: close is async-signal-safe.
    unsafe {
        cmd.pre_exec(move || {
            libc::close(fd);
            Ok(())
        });
    }
}

fn run(mut cmd: Command) -> ExitStatus {
    cmd.status().expect("run child")
}

fn cloexec(fd: RawFd) -> bool {
    // SAFETY: F_GETFD on an open fd.
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    assert!(flags >= 0, "fd {fd} is open");
    flags & libc::FD_CLOEXEC != 0
}

#[test]
#[ignore = "runs only as the child process of the other tests in this file"]
fn child_entry() {
    let Some(role) = std::env::var(ROLE_ENV).ok() else {
        return;
    };
    match role.as_str() {
        // Exit with the code the control fd gives: 0 when it was taken.
        "exit" => {
            // SAFETY: fd 3 belongs to no one else in this process.
            let code = match unsafe { CtlFd::from_env() } {
                Ok(_) => 0,
                Err(err) => err.exit_code(),
            };
            std::process::exit(code);
        }
        // Take fd 3, start a long-lived grandchild, report its pid, exit.
        "cloexec" => {
            assert!(!cloexec(3), "the shim's fd 3 arrives without close-on-exec");
            // SAFETY: as above.
            let ctl = unsafe { CtlFd::from_env() }.expect("control fd");
            assert_eq!(ctl.as_raw_fd(), 3);
            assert!(cloexec(3));
            #[cfg(target_os = "linux")]
            {
                let status = Command::new("sh")
                    .args(["-c", "test -e /proc/self/fd/0 && ! test -e /proc/self/fd/3"])
                    .status()
                    .expect("sh");
                assert!(status.success(), "a child of the worker sees fd 3");
            }
            #[expect(
                clippy::zombie_processes,
                reason = "the grandchild outlives the worker; the shim side kills it"
            )]
            let grandchild = Command::new("sleep")
                .arg("30")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .expect("sleep");
            let out = FrameWriter::new(ctl);
            out.send(&WorkerUp {
                msg: Some(worker_up::Msg::Hello(Hello {
                    kind: "grandchild".into(),
                    pid: grandchild.id() as i32,
                    ..Hello::default()
                })),
            })
            .expect("send");
            // Leave without waiting: the grandchild outlives the worker.
        }
        // With SIGPIPE at its default, a write to a closed shim is EPIPE.
        "sigpipe" => {
            // SAFETY: restores the default disposition in this child only.
            unsafe { libc::signal(libc::SIGPIPE, libc::SIG_DFL) };
            // SAFETY: as above.
            let ctl = unsafe { CtlFd::from_env() }.expect("control fd");
            let err = FrameWriter::new(ctl)
                .send(&WorkerUp {
                    msg: Some(worker_up::Msg::Beat(Beat {
                        seq: 1,
                        progress: Vec::new(),
                    })),
                })
                .unwrap_err();
            assert!(
                matches!(&err, FrameError::Io(e) if e.kind() == io::ErrorKind::BrokenPipe),
                "{err}"
            );
        }
        other => panic!("unknown role {other:?}"),
    }
}

#[test]
fn a_bad_value_exits_10_and_a_closed_fd_exits_1() {
    let (_shim, worker) = UnixStream::pair().expect("socketpair");
    let cases: [(Option<&str>, bool, i32); 7] = [
        (None, true, 0),
        (Some(""), true, 0),
        (Some("3"), true, 0),
        (Some("x"), true, EXIT_BAD_ENV),
        (Some("-1"), true, EXIT_BAD_ENV),
        (Some("4294967299"), true, EXIT_BAD_ENV),
        (Some("7"), false, EXIT_BAD_FD),
    ];
    for (value, with_fd3, want) in cases {
        let mut cmd = child("exit");
        if let Some(value) = value {
            cmd.env(CTL_FD_ENV, value);
        }
        if with_fd3 {
            hand_over_fd3(&mut cmd, worker.as_raw_fd());
        } else {
            close_in_child(&mut cmd, 7);
        }
        let status = run(cmd);
        assert_eq!(
            status.code(),
            Some(want),
            "{CTL_FD_ENV}={value:?}: {status}"
        );
    }
}

#[test]
fn a_child_of_the_worker_does_not_keep_fd3_open() {
    let (shim, worker) = UnixStream::pair().expect("socketpair");
    let mut cmd = child("cloexec");
    cmd.env(CTL_FD_ENV, "3");
    hand_over_fd3(&mut cmd, worker.as_raw_fd());
    let mut worker_process = cmd.spawn().expect("spawn worker");
    drop(worker);

    // A regression would leave the grandchild holding fd 3; fail, not hang.
    shim.set_read_timeout(Some(Duration::from_secs(10)))
        .expect("timeout");
    let mut frames = FrameReader::new(&shim);
    let hello = frames.read_message::<WorkerUp>().expect("read");
    let pid = match hello.and_then(|m| m.msg) {
        Some(worker_up::Msg::Hello(hello)) => hello.pid,
        other => panic!("expected the grandchild's pid, got {other:?}"),
    };
    let status = worker_process.wait().expect("wait worker");

    // The worker is gone and the grandchild still runs: the shim sees EOF.
    let end = frames.next_frame().map(|frame| frame.is_none());
    // SAFETY: signals to the grandchild this test started through the worker.
    let alive = unsafe { libc::kill(pid, 0) } == 0;
    unsafe { libc::kill(pid, libc::SIGKILL) };

    assert!(status.success(), "worker: {status}");
    assert!(matches!(end, Ok(true)), "shim saw {end:?} instead of EOF");
    assert!(alive, "the grandchild ended before the check");
}

#[test]
fn a_shim_that_went_away_is_epipe_not_sigpipe() {
    let (shim, worker) = UnixStream::pair().expect("socketpair");
    drop(shim);
    let mut cmd = child("sigpipe");
    hand_over_fd3(&mut cmd, worker.as_raw_fd());
    let status = run(cmd);
    assert_eq!(status.signal(), None, "killed by a signal: {status}");
    assert!(status.success(), "{status}");
}
