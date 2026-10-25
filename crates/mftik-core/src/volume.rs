//! Strategy volume read order and atomic replace, without doing IO.
//!
//! PROTOCOL §9.3: a strategy writes only its volume directory, by creating
//! a temp file in the destination file's own directory, writing the bytes,
//! syncing the file, renaming it onto the name, and syncing that directory.
//! A crash before the rename leaves the previous file, or no file. The
//! destination is never a partial write. The temp file may remain.
//!
//! Read order is a function the strategy calls. The volume directory is
//! tried first, then the input directory. Neither means start from scratch.
//! An empty directory string is not the process's current directory. A
//! missing directory is a missing file. The SDK does not read on startup.
//!
//! Path spelling follows Go's `path/filepath` on Linux, the session worker
//! OS. The separator is `/`. The caller (E03) performs the actions.
//!
//! [`WriteTxn`] and [`ReadPlan`] do not read a clock.

use std::fmt;

/// Prefix of the temp file created in the destination directory.
/// The executor replaces the trailing star the way `os.CreateTemp` does.
pub const TEMP_PREFIX: &str = ".vol-";

/// A path the volume rules refuse, or a step call that does not match
/// the plan in progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolumeError {
    /// `Write` was given an empty directory. This is not the current directory.
    EmptyDirectory,
    /// `name` is not a relative path inside the directory.
    Name,
    /// `created_temp`, `completed`, `fail`, or `observe` does not match
    /// the step in progress.
    Step,
}

impl fmt::Display for VolumeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            VolumeError::EmptyDirectory => "volume: empty directory",
            VolumeError::Name => "volume: name must be a relative path inside the directory",
            VolumeError::Step => "volume: step does not match the plan",
        })
    }
}

impl std::error::Error for VolumeError {}

/// One step of an atomic replace. The executor runs these in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteAction<'a> {
    /// Create a new file in `directory`. Its name starts with `prefix`
    /// and continues with a unique suffix. This is not the destination path.
    CreateTemp {
        directory: &'a str,
        prefix: &'static str,
    },
    WriteAll {
        data: &'a [u8],
    },
    SyncFile,
    CloseFile,
    /// Publish `data` as the whole destination file. Readers see either the
    /// previous complete file or these bytes, never a prefix of them.
    Rename {
        from: &'a str,
        to: &'a str,
    },
    SyncDir {
        directory: &'a str,
    },
}

/// Why the executor stopped the replace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailKind {
    /// The step returned an error and this process is still running.
    Io,
    /// The process died during the step. A temp file already created may remain.
    Crash,
}

/// What the executor does after [`WriteTxn::fail`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "remove_temp is the only signal to delete the temp file"]
pub struct FailOutcome {
    /// An in-process error before rename deletes the temp file.
    /// A crash does not: the process is gone. After rename the temp name
    /// is the destination, so it is not deleted.
    pub remove_temp: bool,
    /// Rename has already published the new bytes.
    pub destination_replaced: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    CreateTemp,
    Write,
    SyncFile,
    Close,
    Rename,
    SyncDir,
    Done,
    Failed,
}

/// Atomic replace of one file. Preparing it does not touch a filesystem.
#[derive(Debug)]
pub struct WriteTxn {
    dest_dir: String,
    dest_path: String,
    data: Vec<u8>,
    temp: Option<String>,
    phase: Phase,
}

impl WriteTxn {
    /// Plan the replace of `dir`/`name` with `data`.
    ///
    /// `dir` is the directory itself, not a parent `name` may escape.
    /// `name` is a relative path with no `..` element. The temp file is
    /// planned in the destination file's own directory, so the rename stays
    /// there.
    pub fn prepare(dir: &str, name: &str, data: &[u8]) -> Result<Self, VolumeError> {
        let (dest_dir, dest_path) = within(dir, name)?;
        Ok(Self {
            dest_dir,
            dest_path,
            data: data.to_vec(),
            temp: None,
            phase: Phase::CreateTemp,
        })
    }

    /// Next action, or `None` when the replace is finished or has failed.
    pub fn action(&self) -> Option<WriteAction<'_>> {
        Some(match self.phase {
            Phase::CreateTemp => WriteAction::CreateTemp {
                directory: &self.dest_dir,
                prefix: TEMP_PREFIX,
            },
            Phase::Write => WriteAction::WriteAll { data: &self.data },
            Phase::SyncFile => WriteAction::SyncFile,
            Phase::Close => WriteAction::CloseFile,
            Phase::Rename => WriteAction::Rename {
                from: self.temp.as_deref().expect("rename records the temp path"),
                to: &self.dest_path,
            },
            Phase::SyncDir => WriteAction::SyncDir {
                directory: &self.dest_dir,
            },
            Phase::Done | Phase::Failed => return None,
        })
    }

    /// `CreateTemp` created `path`. Later steps rename this path.
    pub fn created_temp(&mut self, path: &str) -> Result<(), VolumeError> {
        if self.phase != Phase::CreateTemp {
            return Err(VolumeError::Step);
        }
        self.temp = Some(path.to_string());
        self.phase = Phase::Write;
        Ok(())
    }

    /// The current action, other than creating the temp file, finished.
    pub fn completed(&mut self) -> Result<(), VolumeError> {
        self.phase = match self.phase {
            Phase::Write => Phase::SyncFile,
            Phase::SyncFile => Phase::Close,
            Phase::Close => Phase::Rename,
            Phase::Rename => Phase::SyncDir,
            Phase::SyncDir => Phase::Done,
            Phase::CreateTemp | Phase::Done | Phase::Failed => return Err(VolumeError::Step),
        };
        Ok(())
    }

    /// The current action failed, or the process is modelled as having crashed.
    ///
    /// Before rename, the destination bytes are unchanged. After rename, they
    /// are the new contents even when the directory sync fails.
    pub fn fail(&mut self, kind: FailKind) -> Result<FailOutcome, VolumeError> {
        let outcome = match self.phase {
            Phase::CreateTemp => FailOutcome {
                remove_temp: false,
                destination_replaced: false,
            },
            Phase::Write | Phase::SyncFile | Phase::Close | Phase::Rename => FailOutcome {
                remove_temp: kind == FailKind::Io,
                destination_replaced: false,
            },
            Phase::SyncDir => FailOutcome {
                remove_temp: false,
                destination_replaced: true,
            },
            Phase::Done | Phase::Failed => return Err(VolumeError::Step),
        };
        self.phase = Phase::Failed;
        Ok(outcome)
    }
}

/// Which directory a read attempt opens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Volume,
    Input,
}

/// One file the executor should open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadAttempt<'a> {
    pub source: Source,
    pub path: &'a str,
}

/// What the executor found at the current path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Observed {
    /// Not-exist, including a missing parent directory.
    /// An empty file is [`Present`](Self::Present), not this.
    Absent,
    /// The file exists. Empty contents are still a hit.
    Present,
    /// An IO error other than not-exist. The other directory is not tried.
    Failed,
}

/// Where a [`ReadPlan`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadPoll<'a> {
    Attempt(ReadAttempt<'a>),
    /// The attempt just observed is the file to use.
    Found {
        source: Source,
    },
    /// Every directory that was searched is missing the file.
    Missing,
    /// The attempt just observed failed. Do not open another directory.
    Failed {
        source: Source,
    },
}

#[derive(Debug)]
struct Attempt {
    source: Source,
    path: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadState {
    Running,
    Found { source: Source },
    Missing,
    Failed { source: Source },
}

/// Volume-then-input lookup. Opening it does not touch a filesystem.
#[derive(Debug)]
pub struct ReadPlan {
    attempts: Vec<Attempt>,
    index: usize,
    state: ReadState,
}

impl ReadPlan {
    /// Plan the read of `name` from `volume_dir`, then `input_dir`.
    ///
    /// An empty directory is skipped. It is not replaced with the current
    /// directory. `name` is checked even when both directories are empty.
    pub fn open(volume_dir: &str, input_dir: &str, name: &str) -> Result<Self, VolumeError> {
        check_name(name)?;
        let mut attempts = Vec::new();
        if !volume_dir.is_empty() {
            attempts.push(Attempt {
                source: Source::Volume,
                path: located(volume_dir, name)?,
            });
        }
        if !input_dir.is_empty() {
            attempts.push(Attempt {
                source: Source::Input,
                path: located(input_dir, name)?,
            });
        }
        let state = if attempts.is_empty() {
            ReadState::Missing
        } else {
            ReadState::Running
        };
        Ok(Self {
            attempts,
            index: 0,
            state,
        })
    }

    pub fn poll(&self) -> ReadPoll<'_> {
        match self.state {
            ReadState::Running => match self.attempts.get(self.index) {
                Some(attempt) => ReadPoll::Attempt(ReadAttempt {
                    source: attempt.source,
                    path: &attempt.path,
                }),
                None => ReadPoll::Missing,
            },
            ReadState::Found { source } => ReadPoll::Found { source },
            ReadState::Missing => ReadPoll::Missing,
            ReadState::Failed { source } => ReadPoll::Failed { source },
        }
    }

    /// Record what the executor saw for [`ReadPoll::Attempt`].
    pub fn observe(&mut self, observed: Observed) -> Result<(), VolumeError> {
        if self.state != ReadState::Running {
            return Err(VolumeError::Step);
        }
        let Some(attempt) = self.attempts.get(self.index) else {
            return Err(VolumeError::Step);
        };
        let source = attempt.source;
        match observed {
            Observed::Present => self.state = ReadState::Found { source },
            Observed::Failed => self.state = ReadState::Failed { source },
            Observed::Absent => {
                self.index += 1;
                if self.index >= self.attempts.len() {
                    self.state = ReadState::Missing;
                }
            }
        }
        Ok(())
    }
}

fn located(dir: &str, name: &str) -> Result<String, VolumeError> {
    let (_dir, path) = within(dir, name)?;
    Ok(path)
}

fn within(dir: &str, name: &str) -> Result<(String, String), VolumeError> {
    if dir.is_empty() {
        return Err(VolumeError::EmptyDirectory);
    }
    check_name(name)?;
    let root = clean(dir);
    let path = join(&[&root, name]);
    let rel = rel(&root, &path).ok_or(VolumeError::Name)?;
    if rel == ".." || rel.starts_with("../") {
        return Err(VolumeError::Name);
    }
    let dest_dir = dir_of(&path);
    Ok((dest_dir, path))
}

fn check_name(name: &str) -> Result<(), VolumeError> {
    if name.is_empty() || name == "." || is_abs(name) || clean(name) != name {
        return Err(VolumeError::Name);
    }
    for part in name.split('/') {
        if part.is_empty() || part == ".." {
            return Err(VolumeError::Name);
        }
    }
    Ok(())
}

fn is_abs(path: &str) -> bool {
    path.as_bytes().first() == Some(&b'/')
}

/// Go's `path/filepath.Clean` on Linux.
fn clean(path: &str) -> String {
    if path.is_empty() {
        return ".".to_string();
    }
    let bytes = path.as_bytes();
    let n = bytes.len();
    let rooted = bytes[0] == b'/';
    let mut out = Vec::with_capacity(n);
    let mut r = 0;
    let mut dotdot = 0;
    if rooted {
        out.push(b'/');
        r = 1;
        dotdot = 1;
    }
    while r < n {
        if bytes[r] == b'/' {
            r += 1;
            continue;
        }
        let dot = bytes[r] == b'.' && (r + 1 == n || bytes[r + 1] == b'/');
        if dot {
            r += 1;
            continue;
        }
        let dotdot_elem = r + 1 < n
            && bytes[r] == b'.'
            && bytes[r + 1] == b'.'
            && (r + 2 == n || bytes[r + 2] == b'/');
        if dotdot_elem {
            r += 2;
            if out.len() > dotdot {
                out.pop();
                while out.len() > dotdot && *out.last().unwrap() != b'/' {
                    out.pop();
                }
                if out.len() > dotdot && *out.last().unwrap() == b'/' {
                    out.pop();
                }
            } else if !rooted {
                if !out.is_empty() {
                    out.push(b'/');
                }
                out.push(b'.');
                out.push(b'.');
                dotdot = out.len();
            }
            continue;
        }
        if (rooted && out.len() != 1) || (!rooted && !out.is_empty()) {
            out.push(b'/');
        }
        while r < n && bytes[r] != b'/' {
            out.push(bytes[r]);
            r += 1;
        }
    }
    if out.is_empty() {
        return ".".to_string();
    }
    String::from_utf8(out).expect("clean copies a utf-8 path")
}

/// Go's `path/filepath.Join` on Linux.
fn join(elem: &[&str]) -> String {
    let Some(start) = elem.iter().position(|part| !part.is_empty()) else {
        return String::new();
    };
    clean(&elem[start..].join("/"))
}

/// Go's `path/filepath.Dir` on Linux.
fn dir_of(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut i = bytes.len() as isize - 1;
    while i >= 0 && bytes[i as usize] != b'/' {
        i -= 1;
    }
    let end = (i + 1) as usize;
    clean(&path[..end])
}

/// Go's `path/filepath.Rel` on Linux. `None` is Go's error.
fn rel(base_path: &str, targ_path: &str) -> Option<String> {
    let mut base = clean(base_path);
    let targ = clean(targ_path);
    if targ == base {
        return Some(".".to_string());
    }
    if base == "." {
        base.clear();
    }
    let base_b = base.as_bytes();
    let targ_b = targ.as_bytes();
    let base_slashed = base_b.first() == Some(&b'/');
    let targ_slashed = targ_b.first() == Some(&b'/');
    if base_slashed != targ_slashed {
        return None;
    }
    let bl = base_b.len();
    let tl = targ_b.len();
    let mut b0 = 0;
    let mut bi = 0;
    let mut t0 = 0;
    let mut ti = 0;
    loop {
        while bi < bl && base_b[bi] != b'/' {
            bi += 1;
        }
        while ti < tl && targ_b[ti] != b'/' {
            ti += 1;
        }
        if base_b[b0..bi] != targ_b[t0..ti] {
            break;
        }
        if bi < bl {
            bi += 1;
        }
        if ti < tl {
            ti += 1;
        }
        b0 = bi;
        t0 = ti;
    }
    if &base_b[b0..bi] == b".." {
        return None;
    }
    if b0 != bl {
        let seps = base_b[b0..bl].iter().filter(|b| **b == b'/').count();
        let mut buf = String::from("..");
        for _ in 0..seps {
            buf.push('/');
            buf.push_str("..");
        }
        if t0 != tl {
            buf.push('/');
            buf.push_str(&targ[t0..]);
        }
        return Some(clean(&buf));
    }
    Some(targ[t0..].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn child(dir: &str, name: &str) -> String {
        if dir == "." {
            name.to_string()
        } else {
            format!("{dir}/{name}")
        }
    }

    #[test]
    fn rejects_names_that_escape_the_directory() {
        let bad = [
            "",
            ".",
            "..",
            "/tmp/x",
            "a/../../b",
            "a/../b",
            "a//b",
            "a/./b",
            "a/b/",
        ];
        for name in bad {
            assert!(
                matches!(WriteTxn::prepare("vol", name, b"x"), Err(VolumeError::Name)),
                "{name}"
            );
            assert!(
                matches!(ReadPlan::open("vol", "in", name), Err(VolumeError::Name)),
                "{name}"
            );
        }
        assert_eq!(
            WriteTxn::prepare("", "a", b"x").unwrap_err(),
            VolumeError::EmptyDirectory
        );
        assert_eq!(
            WriteTxn::prepare("", "", b"x").unwrap_err(),
            VolumeError::EmptyDirectory
        );
        assert!(matches!(ReadPlan::open("", "", ""), Err(VolumeError::Name)));
    }

    #[test]
    fn nested_replace_keeps_the_temp_file_beside_the_destination() {
        let mut txn = WriteTxn::prepare("vol/./nested", "state.bin", b"ab").unwrap();
        match txn.action() {
            Some(WriteAction::CreateTemp { directory, prefix }) => {
                assert_eq!(directory, "vol/nested");
                assert_eq!(prefix, TEMP_PREFIX);
            }
            other => panic!("{other:?}"),
        }
        let temp = child("vol/nested", ".vol-1");
        txn.created_temp(&temp).unwrap();
        match txn.action() {
            Some(WriteAction::WriteAll { data }) => assert_eq!(data, b"ab"),
            other => panic!("{other:?}"),
        }
        txn.completed().unwrap();
        assert!(matches!(txn.action(), Some(WriteAction::SyncFile)));
        txn.completed().unwrap();
        assert!(matches!(txn.action(), Some(WriteAction::CloseFile)));
        txn.completed().unwrap();
        match txn.action() {
            Some(WriteAction::Rename { from, to }) => {
                assert_eq!(from, temp);
                assert_eq!(to, "vol/nested/state.bin");
            }
            other => panic!("{other:?}"),
        }
        txn.completed().unwrap();
        match txn.action() {
            Some(WriteAction::SyncDir { directory }) => assert_eq!(directory, "vol/nested"),
            other => panic!("{other:?}"),
        }
        txn.completed().unwrap();
        assert!(txn.action().is_none());
        assert_eq!(txn.completed().unwrap_err(), VolumeError::Step);
    }

    #[test]
    fn backslash_is_one_element_on_linux() {
        let mut txn = WriteTxn::prepare("vol", "a\\b", b"z").unwrap();
        match txn.action() {
            Some(WriteAction::CreateTemp { directory, .. }) => assert_eq!(directory, "vol"),
            other => panic!("{other:?}"),
        }
        txn.created_temp("vol/.vol-1").unwrap();
        txn.completed().unwrap();
        txn.completed().unwrap();
        txn.completed().unwrap();
        match txn.action() {
            Some(WriteAction::Rename { to, .. }) => assert_eq!(to, "vol/a\\b"),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn directory_dotdot_is_the_callers_directory() {
        let mut txn = WriteTxn::prepare("..", "a", b"z").unwrap();
        txn.created_temp("../.vol-1").unwrap();
        txn.completed().unwrap();
        txn.completed().unwrap();
        txn.completed().unwrap();
        match txn.action() {
            Some(WriteAction::Rename { to, .. }) => assert_eq!(to, "../a"),
            other => panic!("{other:?}"),
        }
    }

    fn publish(fs: &mut BTreeMap<String, Vec<u8>>, txn: &mut WriteTxn, temp_name: &str) -> String {
        let WriteAction::CreateTemp { directory, prefix } = txn.action().unwrap() else {
            panic!("create");
        };
        assert_eq!(prefix, TEMP_PREFIX);
        let temp = child(directory, temp_name);
        fs.insert(temp.clone(), Vec::new());
        txn.created_temp(&temp).unwrap();
        let WriteAction::WriteAll { data } = txn.action().unwrap() else {
            panic!("write");
        };
        let data = data.to_vec();
        fs.insert(temp.clone(), data);
        txn.completed().unwrap();
        assert!(matches!(txn.action(), Some(WriteAction::SyncFile)));
        txn.completed().unwrap();
        assert!(matches!(txn.action(), Some(WriteAction::CloseFile)));
        txn.completed().unwrap();
        temp
    }

    #[test]
    fn destination_changes_only_at_rename() {
        let mut fs = BTreeMap::from([("vol/state.bin".to_string(), b"old".to_vec())]);
        let mut txn = WriteTxn::prepare("vol", "state.bin", b"new").unwrap();
        let temp = publish(&mut fs, &mut txn, ".vol-1");
        assert_eq!(fs["vol/state.bin"], b"old");
        assert_eq!(fs[&temp], b"new");
        let WriteAction::Rename { from, to } = txn.action().unwrap() else {
            panic!("rename");
        };
        assert_eq!(from, temp);
        assert_eq!(to, "vol/state.bin");
        let bytes = fs.remove(&temp).unwrap();
        fs.insert(to.to_string(), bytes);
        txn.completed().unwrap();
        assert_eq!(fs["vol/state.bin"], b"new");
        assert!(!fs.keys().any(|k| k.contains(".vol-")));
        let WriteAction::SyncDir { directory } = txn.action().unwrap() else {
            panic!("sync dir");
        };
        assert_eq!(directory, "vol");
        txn.completed().unwrap();
        assert!(txn.action().is_none());
    }

    #[test]
    fn io_error_before_rename_removes_the_temp_and_keeps_the_old_bytes() {
        let mut txn = WriteTxn::prepare("vol", "state.bin", b"new").unwrap();
        txn.created_temp("vol/.vol-1").unwrap();
        txn.completed().unwrap();
        let outcome = txn.fail(FailKind::Io).unwrap();
        assert!(outcome.remove_temp);
        assert!(!outcome.destination_replaced);
        assert!(txn.action().is_none());
        assert_eq!(txn.fail(FailKind::Io).unwrap_err(), VolumeError::Step);
    }

    #[test]
    fn crash_before_rename_leaves_the_temp_and_the_old_bytes() {
        let mut fs = BTreeMap::from([("vol/state.bin".to_string(), b"old".to_vec())]);
        let mut txn = WriteTxn::prepare("vol", "state.bin", b"new").unwrap();
        let temp = publish(&mut fs, &mut txn, ".vol-1");
        let outcome = txn.fail(FailKind::Crash).unwrap();
        assert!(!outcome.remove_temp);
        assert!(!outcome.destination_replaced);
        assert_eq!(fs["vol/state.bin"], b"old");
        assert_eq!(fs[&temp], b"new");
    }

    #[test]
    fn crash_before_any_temp_leaves_no_file() {
        let mut txn = WriteTxn::prepare("vol", "state.bin", b"new").unwrap();
        let outcome = txn.fail(FailKind::Crash).unwrap();
        assert!(!outcome.remove_temp);
        assert!(!outcome.destination_replaced);
        let outcome = WriteTxn::prepare("vol", "state.bin", b"new")
            .unwrap()
            .fail(FailKind::Io)
            .unwrap();
        assert!(!outcome.remove_temp);
        assert!(!outcome.destination_replaced);
    }

    #[test]
    fn directory_sync_failure_keeps_the_new_bytes() {
        let mut txn = WriteTxn::prepare("vol", "state.bin", b"new").unwrap();
        txn.created_temp("vol/.vol-1").unwrap();
        txn.completed().unwrap();
        txn.completed().unwrap();
        txn.completed().unwrap();
        txn.completed().unwrap();
        assert!(matches!(txn.action(), Some(WriteAction::SyncDir { .. })));
        let outcome = txn.fail(FailKind::Io).unwrap();
        assert!(!outcome.remove_temp);
        assert!(outcome.destination_replaced);
    }

    #[test]
    fn empty_directories_are_not_the_current_directory() {
        let mut plan = ReadPlan::open("", "", "state.bin").unwrap();
        assert!(matches!(plan.poll(), ReadPoll::Missing));
        assert_eq!(
            plan.observe(Observed::Absent).unwrap_err(),
            VolumeError::Step
        );
    }

    fn lookup(
        plan: &mut ReadPlan,
        files: &BTreeMap<String, Result<Vec<u8>, ()>>,
        opened: &mut Vec<String>,
    ) -> Result<Option<Vec<u8>>, ()> {
        loop {
            let ReadPoll::Attempt(attempt) = plan.poll() else {
                return match plan.poll() {
                    ReadPoll::Missing => Ok(None),
                    ReadPoll::Failed { .. } => Err(()),
                    ReadPoll::Attempt(_) | ReadPoll::Found { .. } => panic!("read plan"),
                };
            };
            let source = attempt.source;
            let path = attempt.path.to_string();
            opened.push(path.clone());
            match files.get(&path) {
                None => plan.observe(Observed::Absent).unwrap(),
                Some(Err(())) => {
                    plan.observe(Observed::Failed).unwrap();
                    assert!(
                        matches!(plan.poll(), ReadPoll::Failed { source: got } if got == source)
                    );
                    return Err(());
                }
                Some(Ok(bytes)) => {
                    let bytes = bytes.clone();
                    plan.observe(Observed::Present).unwrap();
                    assert!(
                        matches!(plan.poll(), ReadPoll::Found { source: got } if got == source)
                    );
                    return Ok(Some(bytes));
                }
            }
        }
    }

    #[test]
    fn read_prefers_volume_then_input() {
        let vol = "vol/state.bin";
        let input = "in/state.bin";
        let both = BTreeMap::from([
            (vol.to_string(), Ok(b"v".to_vec())),
            (input.to_string(), Ok(b"i".to_vec())),
        ]);
        let mut opened = Vec::new();
        let mut plan = ReadPlan::open("vol", "in", "state.bin").unwrap();
        assert_eq!(
            lookup(&mut plan, &both, &mut opened).unwrap().unwrap(),
            b"v"
        );
        assert_eq!(opened, vec![vol]);

        let input_only = BTreeMap::from([(input.to_string(), Ok(b"i".to_vec()))]);
        opened.clear();
        let mut plan = ReadPlan::open("vol", "in", "state.bin").unwrap();
        assert_eq!(
            lookup(&mut plan, &input_only, &mut opened)
                .unwrap()
                .unwrap(),
            b"i"
        );
        assert_eq!(opened, vec![vol, input]);

        opened.clear();
        let mut plan = ReadPlan::open("vol", "in", "state.bin").unwrap();
        assert_eq!(
            lookup(&mut plan, &BTreeMap::new(), &mut opened).unwrap(),
            None
        );
        assert_eq!(opened, vec![vol, input]);
    }

    #[test]
    fn empty_volume_file_is_a_hit() {
        let files = BTreeMap::from([
            ("vol/state.bin".to_string(), Ok(Vec::new())),
            ("in/state.bin".to_string(), Ok(b"i".to_vec())),
        ]);
        let mut opened = Vec::new();
        let mut plan = ReadPlan::open("vol", "in", "state.bin").unwrap();
        assert_eq!(
            lookup(&mut plan, &files, &mut opened).unwrap().unwrap(),
            b""
        );
        assert_eq!(opened, vec!["vol/state.bin"]);
    }

    #[test]
    fn volume_io_error_does_not_open_the_input() {
        let files = BTreeMap::from([
            ("vol/state.bin".to_string(), Err(())),
            ("in/state.bin".to_string(), Ok(b"i".to_vec())),
        ]);
        let mut opened = Vec::new();
        let mut plan = ReadPlan::open("vol", "in", "state.bin").unwrap();
        assert!(lookup(&mut plan, &files, &mut opened).is_err());
        assert_eq!(opened, vec!["vol/state.bin"]);
        assert!(matches!(
            plan.poll(),
            ReadPoll::Failed {
                source: Source::Volume
            }
        ));
    }

    #[test]
    fn missing_nested_directory_is_a_missing_file() {
        let mut opened = Vec::new();
        let mut plan = ReadPlan::open("no-such-volume", "", "nested/state.bin").unwrap();
        assert_eq!(
            lookup(&mut plan, &BTreeMap::new(), &mut opened).unwrap(),
            None
        );
        assert_eq!(opened, vec!["no-such-volume/nested/state.bin"]);
    }

    #[test]
    fn skipped_volume_dir_reads_the_input() {
        let files = BTreeMap::from([("in/state.bin".to_string(), Ok(b"i".to_vec()))]);
        let mut opened = Vec::new();
        let mut plan = ReadPlan::open("", "in", "state.bin").unwrap();
        assert_eq!(
            lookup(&mut plan, &files, &mut opened).unwrap().unwrap(),
            b"i"
        );
        assert_eq!(opened, vec!["in/state.bin"]);
    }
}
