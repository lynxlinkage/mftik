//! An event log in a directory of its own, for the tests of the paths that
//! record into it.

use std::sync::Arc;

use mftik_core::eventlog::Recorder;
use mftik_testkit::TempDir;

use super::{Config, Header, Log, Reader, Record, Recording, files};
use crate::clock::Clock;

pub(crate) struct Scratch {
    log: Arc<Log>,
    clock: Clock,
    dir: TempDir,
}

impl Scratch {
    pub(crate) fn open() -> Self {
        let dir = TempDir::new("eventlog");
        let log = Log::open(Config {
            dir: Some(dir.0.clone()),
            header: Header::mftik(1, "c0ffee"),
            ..Config::default()
        })
        .expect("event log");
        Self {
            log: Arc::new(log),
            clock: Clock::new(),
            dir,
        }
    }

    /// The dispatcher's recorder.
    pub(crate) fn recorder(&self) -> Arc<dyn Recorder> {
        Arc::new(Recording::new(Arc::clone(&self.log), self.clock))
    }

    /// The writer itself, for a plane that makes its recorder from it.
    pub(crate) fn log(&self) -> Arc<Log> {
        Arc::clone(&self.log)
    }

    /// Closes the log and reads its frames back, `end` last.
    pub(crate) fn close(&self) -> Vec<Record> {
        self.log.close();
        let mut records = Vec::new();
        for file in files(&self.dir.0).expect("log files") {
            let mut reader = Reader::open(&file.path).expect("log file");
            records.extend(reader.read_all().expect("frames"));
        }
        records
    }
}
