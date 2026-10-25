//! Strategy volume IO: runs `mftik_core::volume`'s plans (PROTOCOL §9.1.4,
//! E08-D051, E08-D052, E09-D004).
//!
//! [`write()`] is the atomic replace of [`WriteTxn`]: a temp file in the
//! destination's own directory, write, fsync, close, rename onto the name,
//! fsync the directory. A failure before the rename removes the temp file; a
//! crash may leave it, never a partial destination. [`read`] is the read
//! order of [`ReadPlan`]: the volume directory, then the input directory,
//! then nothing. The SDK does not read on startup; the strategy calls these
//! with [`crate::Context::volume_dir`] and [`crate::Context::input_dir`],
//! like Go's `sdk/volume`.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use mftik_core::volume::{
    FailKind, Observed, ReadPlan, ReadPoll, Source, VolumeError, WriteAction, WriteTxn,
};

/// How many names [`write`] tries before giving up on a temp file, as
/// `os.CreateTemp`.
const TEMP_TRIES: u32 = 10_000;

/// Replaces `dir`/`name` with `data` atomically. A `name` the volume rules
/// refuse, or an empty `dir`, is an `InvalidInput` error and touches nothing.
pub fn write(dir: &str, name: &str, data: &[u8]) -> io::Result<()> {
    let mut txn = WriteTxn::prepare(dir, name, data).map_err(invalid)?;
    let mut file: Option<File> = None;
    let mut temp: Option<String> = None;
    while let Some(action) = txn.action() {
        let create = matches!(action, WriteAction::CreateTemp { .. });
        let step = match action {
            WriteAction::CreateTemp { directory, prefix } => {
                create_temp(directory, prefix).map(|(f, path)| {
                    file = Some(f);
                    temp = Some(path);
                })
            }
            WriteAction::WriteAll { data } => open(&mut file).and_then(|f| f.write_all(data)),
            WriteAction::SyncFile => open(&mut file).and_then(|f| f.sync_all()),
            WriteAction::CloseFile => {
                file = None;
                Ok(())
            }
            WriteAction::Rename { from, to } => fs::rename(from, to),
            WriteAction::SyncDir { directory } => File::open(directory).and_then(|d| d.sync_all()),
        };
        match step {
            Ok(()) if create => {
                let path = temp.as_deref().expect("create_temp records the path");
                txn.created_temp(path).map_err(invalid)?;
            }
            Ok(()) => txn.completed().map_err(invalid)?,
            Err(err) => {
                drop(file.take());
                let outcome = txn.fail(FailKind::Io).map_err(invalid)?;
                if outcome.remove_temp
                    && let Some(path) = &temp
                {
                    let _ = fs::remove_file(path);
                }
                return Err(err);
            }
        }
    }
    Ok(())
}

/// The file `name` from `volume_dir`, else from `input_dir`. `Ok(None)` when
/// neither has it; an empty directory string is skipped, not the current
/// directory. An error other than not-exist stops the search.
pub fn read(volume_dir: &str, input_dir: &str, name: &str) -> io::Result<Option<Vec<u8>>> {
    let mut plan = ReadPlan::open(volume_dir, input_dir, name).map_err(invalid)?;
    loop {
        let attempt = match plan.poll() {
            ReadPoll::Attempt(attempt) => attempt,
            ReadPoll::Missing => return Ok(None),
            ReadPoll::Found { .. } | ReadPoll::Failed { .. } => {
                unreachable!("read returns as soon as the plan settles")
            }
        };
        let source: Source = attempt.source;
        match fs::read(attempt.path) {
            Ok(data) => {
                plan.observe(Observed::Present).map_err(invalid)?;
                debug_assert_eq!(plan.poll(), ReadPoll::Found { source });
                return Ok(Some(data));
            }
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                plan.observe(Observed::Absent).map_err(invalid)?;
            }
            Err(err) => {
                plan.observe(Observed::Failed).map_err(invalid)?;
                return Err(err);
            }
        }
    }
}

fn open(file: &mut Option<File>) -> io::Result<&mut File> {
    file.as_mut()
        .ok_or_else(|| io::Error::other("volume: temp file is closed"))
}

/// `os.CreateTemp(dir, prefix + "*")`: a new file, mode 0600, opened with
/// O_EXCL under a name that is not taken.
fn create_temp(dir: &str, prefix: &str) -> io::Result<(File, String)> {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    for _ in 0..TEMP_TRIES {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.subsec_nanos());
        let seq = SEQ.fetch_add(1, Ordering::Relaxed);
        let mixed = (u64::from(nanos) ^ (u64::from(std::process::id()) << 32))
            .wrapping_add(seq.wrapping_mul(0x9e37_79b9_7f4a_7c15));
        // A decimal u32, as os.CreateTemp's random part.
        let path = format!("{dir}/{prefix}{}", (mixed >> 16) as u32);
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
        {
            Ok(file) => return Ok((file, path)),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(err) => return Err(err),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "volume: no free temp name",
    ))
}

fn invalid(err: VolumeError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, err)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;

    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            static N: AtomicU64 = AtomicU64::new(0);
            let path = std::env::temp_dir().join(format!(
                "mftik-rt-volume-{tag}-{}-{}",
                std::process::id(),
                N.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&path).expect("temp dir");
            Self(path)
        }

        fn s(&self) -> &str {
            self.0.to_str().expect("utf-8 temp dir")
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn entries(dir: &Path) -> Vec<String> {
        let mut names: Vec<_> = fs::read_dir(dir)
            .expect("read dir")
            .map(|e| e.expect("entry").file_name().into_string().expect("utf-8"))
            .collect();
        names.sort();
        names
    }

    #[test]
    fn write_replaces_the_file_and_leaves_no_temp() {
        let vol = TempDir::new("w");
        write(vol.s(), "state.bin", b"one").expect("first write");
        write(vol.s(), "state.bin", b"two").expect("second write");
        assert_eq!(fs::read(vol.0.join("state.bin")).expect("read"), b"two");
        assert_eq!(entries(&vol.0), vec!["state.bin".to_owned()]);
        fs::create_dir(vol.0.join("sub")).expect("sub");
        write(vol.s(), "sub/x", b"").expect("nested write");
        assert_eq!(fs::read(vol.0.join("sub/x")).expect("read"), b"");
    }

    #[test]
    fn a_failed_step_removes_the_temp_and_keeps_the_old_file() {
        let vol = TempDir::new("f");
        write(vol.s(), "keep", b"old").expect("write");
        // The rename fails: the destination is a non-empty directory.
        fs::create_dir(vol.0.join("dir")).expect("dir");
        fs::write(vol.0.join("dir/inner"), b"x").expect("inner");
        let err = write(vol.s(), "dir", b"new").expect_err("rename onto a directory");
        assert_ne!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(entries(&vol.0), vec!["dir".to_owned(), "keep".to_owned()]);
        // A missing parent fails at CreateTemp; nothing is created.
        assert!(write(vol.s(), "missing/x", b"new").is_err());
        assert_eq!(fs::read(vol.0.join("keep")).expect("read"), b"old");
    }

    #[test]
    fn bad_names_and_an_empty_dir_are_refused_before_any_io() {
        let vol = TempDir::new("n");
        for name in ["", ".", "/abs", "../x", "a/../b", "a//b"] {
            let err = write(vol.s(), name, b"x").expect_err(name);
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{name}");
            let err = read(vol.s(), "", name).expect_err(name);
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{name}");
        }
        let err = write("", "x", b"x").expect_err("empty dir");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(entries(&vol.0).is_empty());
    }

    #[test]
    fn read_tries_the_volume_then_the_input() {
        let vol = TempDir::new("rv");
        let input = TempDir::new("ri");
        assert_eq!(read(vol.s(), input.s(), "s").expect("none"), None);
        fs::write(input.0.join("s"), b"in").expect("input");
        assert_eq!(
            read(vol.s(), input.s(), "s").expect("input"),
            Some(b"in".to_vec())
        );
        fs::write(vol.0.join("s"), b"").expect("volume");
        assert_eq!(
            read(vol.s(), input.s(), "s").expect("volume"),
            Some(Vec::new())
        );
        assert_eq!(
            read("", input.s(), "s").expect("input only"),
            Some(b"in".to_vec())
        );
        assert_eq!(read("", "", "s").expect("neither"), None);
        // A missing directory is a missing file.
        let gone = format!("{}/gone", vol.s());
        assert_eq!(
            read(&gone, input.s(), "s").expect("gone"),
            Some(b"in".to_vec())
        );
        // Another error stops the search before the input.
        fs::create_dir(vol.0.join("d")).expect("d");
        fs::write(input.0.join("d"), b"in").expect("input d");
        assert!(read(vol.s(), input.s(), "d").is_err());
    }
}
