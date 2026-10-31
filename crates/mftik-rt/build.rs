//! `sdk_version` is this crate's package version and git commit (E06-D008).
//!
//! vergen-gitcl emits `VERGEN_GIT_SHA` when `git rev-parse` accepts the
//! worktree. A build whose user does not own the checkout (root in a
//! container on a runner-owned checkout, for one) makes git refuse the
//! worktree (`not within a suitable 'git' worktree`).
//! The SHA is still written: `git -c safe.directory=* rev-parse HEAD`, then
//! `GITHUB_SHA`. vergen reads `VERGEN_GIT_SHA` on that failure path.

use std::process::Command;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-env-changed=GITHUB_SHA");
    println!("cargo:rerun-if-env-changed=VERGEN_GIT_SHA");
    if std::env::var_os("VERGEN_GIT_SHA").is_none()
        && !git_accepts_worktree()
        && let Some(sha) = fallback_sha()
    {
        // SAFETY: a build script is one thread until it returns. `set_var`
        // only publishes the SHA for vergen's own process, which reads it
        // after its git check fails. No other thread is reading the environment.
        unsafe { std::env::set_var("VERGEN_GIT_SHA", sha) };
    }
    let gitcl = vergen_gitcl::Gitcl::builder().sha(false).build();
    vergen_gitcl::Emitter::default()
        .add_instructions(&gitcl)?
        .emit()?;
    Ok(())
}

fn git_accepts_worktree() -> bool {
    command_stdout(&["rev-parse", "--is-inside-work-tree"])
        .is_some_and(|text| text.trim() == "true")
}

fn fallback_sha() -> Option<String> {
    command_stdout(&["-c", "safe.directory=*", "rev-parse", "HEAD"])
        .and_then(|text| full_sha(&text))
        .or_else(|| {
            std::env::var("GITHUB_SHA")
                .ok()
                .and_then(|text| full_sha(&text))
        })
}

fn command_stdout(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}

fn full_sha(text: &str) -> Option<String> {
    let sha = text.trim();
    if sha.len() == 40 && sha.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Some(sha.to_owned())
    } else {
        None
    }
}
