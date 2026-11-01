//! Offline replay of one incarnation of the strategy event log
//! (PROTOCOL §12.6, E07-D016–E07-D019).
//!
//! [`replay`] reads `dir` (the session volume's `eventlog/`), defaulting to
//! the newest incarnation, and runs each recorded input hook in order on the
//! caller's thread. It does not connect NATS or talk to C4. Differences
//! from the recording are listed on [`ReplayReport`]; an error means the log
//! could not be replayed at all.

use std::collections::HashMap;
use std::fmt;
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use mftik_core::dispatch::{self, Dispatcher};
use mftik_core::eventlog::{
    self, Kind, Recorder, STOP_DELIVER, STOP_HUNG, outcome, parse_init, parse_order_call,
    parse_read_call, parse_resolve,
};
use mftik_core::symbol::Info;
use mftik_core::symclient;
use mftik_proto::wire::mftiko::stsctl::v1::{InitPayload, StartPayload};
use mftik_proto::wire::mftiko::sym::v1::SymbolInfo;
use mftik_proto::wire::mftiko::td::v1::OrderRequest;
use prost::Message;

use crate::clock::Clock;
use crate::context::Error;
use crate::eventlog::{self as evfile, Header, ReadError, Reader, Record};
use crate::io::Io;
use crate::plane::Plane;
use crate::strategy::Strategy;
use crate::timers::Timers;

/// Tunes [`replay`]. The zero value replays the newest incarnation in the
/// directory with no volume or input directory.
#[derive(Clone, Debug, Default)]
pub struct ReplayOptions {
    /// The incarnation to replay. Zero is the newest one in the directory.
    pub incarnation: u32,
    /// What [`crate::Context::volume_dir`] uses during the replay.
    pub volume_dir: String,
    /// What [`crate::Context::input_dir`] uses during the replay.
    pub input_dir: String,
}

/// What [`replay`] found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReplayReport {
    pub session_id: String,
    pub incarnation: u32,
    /// The release that wrote the log.
    pub sdk_version: String,
    pub sdk_commit: String,
    /// Recorded inputs whose hooks ran again, OnInit and OnStart included.
    pub inputs: usize,
    /// Inputs whose hook did not return in the recording: they are not run
    /// again.
    pub skipped: usize,
    /// `rec_seq` numbers that are not in the files.
    pub missing: u64,
    /// Recorded order calls that were compared.
    pub orders: usize,
    /// Recorded md reads that were compared.
    pub reads: usize,
    /// Each way the replay differs from the recording. Empty means the
    /// outputs are the same, call for call and client order id for client
    /// order id.
    pub mismatches: Vec<String>,
}

/// The log could not be replayed at all. Differences belong on the report.
#[derive(Debug)]
pub struct ReplayError(String);

impl ReplayError {
    fn new(text: impl Into<String>) -> Self {
        Self(text.into())
    }
}

impl fmt::Display for ReplayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ReplayError {}

impl From<io::Error> for ReplayError {
    fn from(err: io::Error) -> Self {
        Self(format!("sdk: replay: {err}"))
    }
}

/// Runs `strategy` again over one incarnation of the event log in `dir`, the
/// session's `VolumeDir()/eventlog`, without NATS or C4 (E07-D017). `strategy`
/// must be a new value, as the worker would construct it.
pub fn replay<S: Strategy>(
    strategy: S,
    dir: impl AsRef<Path>,
    opt: ReplayOptions,
) -> Result<ReplayReport, ReplayError> {
    let dir = dir.as_ref();
    let (recs, header) = read_log(dir, opt.incarnation)?;
    let io = replay_io()?;
    let clock = Clock::new();
    let timers = Timers::new(io.clone(), clock);
    let plane = Plane::start(strategy, io, clock, timers).map_err(ReplayError::from)?;
    let mut r = Replayer {
        plane,
        out: Memory::new(),
        recs,
        ends: HashMap::new(),
        shared: Arc::new(Shared {
            resolves: Vec::new(),
            resolved: Mutex::new(0),
            mismatches: Mutex::new(Vec::new()),
            calls: HashMap::new(),
            seed: Mutex::new(None),
        }),
        stopped: false,
        report: ReplayReport {
            session_id: header.session_id,
            incarnation: header.incarnation,
            sdk_version: header.sdk_version,
            sdk_commit: header.sdk_commit,
            ..ReplayReport::default()
        },
        opt,
    };
    r.prepare();
    r.run()?;
    r.report.mismatches = lock(&r.shared.mismatches).clone();
    Ok(r.report)
}

fn replay_io() -> io::Result<Io> {
    static IO: OnceLock<Io> = OnceLock::new();
    if let Some(io) = IO.get() {
        return Ok(io.clone());
    }
    let started = Io::start()?;
    Ok(IO.get_or_init(|| started).clone())
}

/// Reads every frame of one incarnation, oldest file first. A file that ends
/// inside a frame is what a process killed during a write leaves; the frames
/// before it are kept.
fn read_log(dir: &Path, mut inc: u32) -> Result<(Vec<Record>, Header), ReplayError> {
    let files = evfile::files(dir)
        .map_err(|err| ReplayError::new(format!("sdk: replay: {}: {err}", dir.display())))?;
    if inc == 0 {
        for file in &files {
            inc = inc.max(file.incarnation);
        }
    }
    let mut recs = Vec::new();
    let mut header = Header::default();
    let mut found = false;
    for file in files {
        if file.incarnation != inc {
            continue;
        }
        let mut reader = Reader::open(&file.path).map_err(|err| {
            ReplayError::new(format!("sdk: replay: {}: {err}", file.path.display()))
        })?;
        if !found {
            header = reader.header().clone();
            found = true;
        }
        loop {
            match reader.next_record() {
                Ok(Some(record)) => recs.push(record),
                Ok(None) | Err(ReadError::Truncated) => break,
                Err(err) => {
                    return Err(ReplayError::new(format!(
                        "sdk: replay: {}: {err}",
                        file.path.display()
                    )));
                }
            }
        }
    }
    if !found {
        return Err(ReplayError::new(format!(
            "sdk: replay: {} has no event log of incarnation {inc}",
            dir.display()
        )));
    }
    Ok((recs, header))
}

struct Replayer<S: Strategy> {
    plane: Arc<Plane<S>>,
    out: Arc<Memory>,
    recs: Vec<Record>,
    ends: HashMap<u64, Record>,
    shared: Arc<Shared>,
    stopped: bool,
    report: ReplayReport,
    opt: ReplayOptions,
}

struct Shared {
    resolves: Vec<Record>,
    resolved: Mutex<usize>,
    mismatches: Mutex<Vec<String>>,
    calls: HashMap<u64, Record>,
    seed: Mutex<Option<SeedInfo>>,
}

type SeedInfo = Arc<dyn Fn(Info) + Send + Sync>;

impl Shared {
    fn mismatch(&self, text: String) {
        lock(&self.mismatches).push(text);
    }

    fn resolve(&self, ticker: &str) -> Result<Info, Error> {
        let mut resolved = lock(&self.resolved);
        if *resolved == self.resolves.len() {
            self.mismatch(format!(
                "Resolve({ticker:?}): the recording has no more Resolve calls"
            ));
            return Err(Error::Other(
                "sdk: replay: the recording has no such call".into(),
            ));
        }
        let x = self.resolves[*resolved].clone();
        *resolved += 1;
        let nth = *resolved;
        drop(resolved);
        let (asked, rest) = match parse_resolve(&x.body) {
            Ok(parsed) => parsed,
            Err(err) => {
                self.mismatch(format!("rec_seq {}: {err}", x.seq));
                return Err(Error::Other(
                    "sdk: replay: the recording has no such call".into(),
                ));
            }
        };
        if asked != ticker {
            self.mismatch(format!(
                "Resolve {nth}: the replay asked for {ticker:?}; the recording for {asked:?}"
            ));
            return Err(Error::Other(
                "sdk: replay: the recording has no such call".into(),
            ));
        }
        if x.flags != outcome::OK {
            return Err(resolve_err(x.flags, rest));
        }
        let msg = match SymbolInfo::decode(rest) {
            Ok(msg) => msg,
            Err(_) => {
                self.mismatch(format!("rec_seq {}: SymbolInfo does not decode", x.seq));
                return Err(Error::Other(
                    "sdk: replay: the recording has no such call".into(),
                ));
            }
        };
        let info = match symclient::info_from_proto(msg) {
            Ok(info) => info,
            Err(err) => {
                self.mismatch(format!("rec_seq {}: {err}", x.seq));
                return Err(Error::Other(
                    "sdk: replay: the recording has no such call".into(),
                ));
            }
        };
        if let Some(seed) = lock(&self.seed).clone() {
            seed(info.clone());
        }
        Ok(info)
    }

    fn call(&self, id: u64) -> Option<(i64, u64)> {
        let x = self.calls.get(&id)?;
        let c = parse_order_call(&x.body).ok()?;
        Some((x.t_ns, c.submit_gen))
    }
}

impl<S: Strategy> Replayer<S> {
    fn prepare(&mut self) {
        let mut last = 0u64;
        let mut resolves = Vec::new();
        let mut calls = HashMap::new();
        for x in &self.recs {
            if x.seq > last + 1 {
                self.report.missing += x.seq - last - 1;
            }
            last = last.max(x.seq);
            match x.kind {
                Kind::HOOK_END => {
                    self.ends.insert(x.r#ref, x.clone());
                }
                Kind::RESOLVE => resolves.push(x.clone()),
                Kind::ORDER_CALL => {
                    calls.insert(x.r#ref, x.clone());
                }
                _ => {}
            }
        }
        let shared = Arc::get_mut(&mut self.shared).expect("shared is unique before run");
        shared.resolves = resolves;
        shared.calls = calls;
    }

    fn run(&mut self) -> Result<(), ReplayError> {
        if self.recs.first().is_none_or(|x| x.kind != Kind::INIT) {
            return Err(ReplayError::new(
                "sdk: replay: the log does not start with the init frame",
            ));
        }
        let shared = Arc::clone(&self.shared);
        let plane = Arc::clone(&self.plane);
        *lock(&shared.seed) = Some(Arc::new(move |info| plane.wiring.seed_info(info)));
        for x in self.recs.clone() {
            match x.kind {
                Kind::INIT => {
                    if self.plane.dispatcher().is_some() {
                        return Err(ReplayError::new(format!(
                            "sdk: replay: rec_seq {}: a second init frame",
                            x.seq
                        )));
                    }
                    self.init(&x)?;
                }
                Kind::START => self.start(&x),
                Kind::BINDINGS => self.bindings(&x),
                Kind::STOP => self.stop(&x),
                Kind::ONSTOP => {
                    if self.stopped
                        && let Some(disp) = self.plane.dispatcher()
                    {
                        disp.close_td();
                    }
                    let plane = Arc::clone(&self.plane);
                    self.hook(&x, || {
                        plane.in_hook(|s, ctx| {
                            s.on_stop(ctx);
                            Ok(())
                        })
                    });
                }
                kind if kind.is_input() => self.step(&x),
                _ => {}
            }
        }
        let leftover = self
            .shared
            .resolves
            .len()
            .saturating_sub(*lock(&self.shared.resolved));
        if leftover > 0 {
            self.shared.mismatch(format!(
                "{leftover} recorded Resolve calls were not made again"
            ));
        }
        self.compare();
        Ok(())
    }

    fn init(&mut self, x: &Record) -> Result<(), ReplayError> {
        let (slot, payload) =
            parse_init(&x.body).map_err(|err| ReplayError::new(format!("sdk: replay: {err}")))?;
        let init = InitPayload::decode(payload)
            .map_err(|err| ReplayError::new(format!("sdk: replay: init payload: {err}")))?;
        let shared = Arc::clone(&self.shared);
        let resolve = Arc::new(move |ticker: &str| shared.resolve(ticker));
        let call_shared = Arc::clone(&self.shared);
        let call = Arc::new(move |id| call_shared.call(id));
        self.plane
            .open_replay(
                &init,
                slot,
                x.r#ref,
                self.opt.volume_dir.clone(),
                self.opt.input_dir.clone(),
                Arc::clone(&self.out) as Arc<dyn Recorder>,
                call,
                resolve,
            )
            .map_err(ReplayError::new)?;
        let plane = Arc::clone(&self.plane);
        plane.set_resolve_allowed(true);
        self.hook(x, || {
            plane.in_hook(|s, ctx| s.on_init(ctx).map_err(|err| err.to_string()))
        });
        plane.set_resolve_allowed(false);
        Ok(())
    }

    fn start(&mut self, x: &Record) {
        let sp = match StartPayload::decode(x.body.as_slice()) {
            Ok(sp) if sp.bindings.is_some() => sp,
            _ => {
                self.shared.mismatch(format!(
                    "rec_seq {}: the start payload does not decode",
                    x.seq
                ));
                return;
            }
        };
        if let Err(err) = self
            .plane
            .wiring
            .apply_bindings(sp.bindings.as_ref().expect("bindings"))
        {
            self.shared
                .mismatch(format!("rec_seq {}: start bindings: {err}", x.seq));
        }
        if let Err(err) = self.plane.wiring.listen_td() {
            self.shared.mismatch(format!("rec_seq {}: {err}", x.seq));
        }
        let plane = Arc::clone(&self.plane);
        plane.set_resolve_allowed(true);
        self.hook(x, || {
            plane.in_hook(|s, ctx| s.on_start(ctx).map_err(|err| err.to_string()))
        });
        plane.set_resolve_allowed(false);
        let _ = self.plane.wiring.recon();
    }

    fn stop(&mut self, x: &Record) {
        if x.flags & STOP_HUNG != 0 {
            return;
        }
        self.stopped = true;
        if x.flags & STOP_DELIVER != 0 {
            self.plane.wiring.unsubscribe_td();
        }
    }

    fn bindings(&mut self, x: &Record) {
        let b = match mftik_proto::wire::mftiko::stsctl::v1::Bindings::decode(x.body.as_slice()) {
            Ok(b) => b,
            Err(_) => {
                self.shared
                    .mismatch(format!("rec_seq {}: bindings do not decode", x.seq));
                return;
            }
        };
        if let Err(err) = self.plane.wiring.apply_bindings(&b) {
            self.shared
                .mismatch(format!("rec_seq {}: bindings: {err}", x.seq));
        }
    }

    fn step(&mut self, x: &Record) {
        let plane = Arc::clone(&self.plane);
        let fn_once: Option<Box<dyn FnOnce() + Send>> = match x.kind {
            Kind::MD_FEED => Some(Box::new(move || plane.on_feed(&x.body))),
            Kind::MD_BOOK => Some(Box::new(move || {
                if let Some(md) = plane.wiring.md() {
                    md.on_book_message(&x.body);
                }
            })),
            Kind::TD_EVENT => {
                let api_id = x.r#ref as i64;
                let body = x.body.clone();
                let seq = x.seq;
                let kind = x.kind;
                let shared = Arc::clone(&self.shared);
                Some(Box::new(move || {
                    if let Some(td) = plane.wiring.td()
                        && let Err(err) = td.replay_event(api_id, &body)
                    {
                        shared.mismatch(format!("rec_seq {seq} {kind}: {err}"));
                    }
                }))
            }
            Kind::TD_VIEW => {
                let api_id = x.r#ref as i64;
                let body = x.body.clone();
                let seq = x.seq;
                let kind = x.kind;
                let shared = Arc::clone(&self.shared);
                Some(Box::new(move || {
                    if let Some(td) = plane.wiring.td()
                        && let Err(err) = td.replay_view(api_id, &body)
                    {
                        shared.mismatch(format!("rec_seq {seq} {kind}: {err}"));
                    }
                }))
            }
            Kind::ORDER_REPLY => {
                let call = x.r#ref;
                let flags = x.flags;
                let body = x.body.clone();
                let seq = x.seq;
                let kind = x.kind;
                let shared = Arc::clone(&self.shared);
                Some(Box::new(move || {
                    if let Some(td) = plane.wiring.td()
                        && let Err(err) = td.replay_reply(call, flags, &body)
                    {
                        shared.mismatch(format!("rec_seq {seq} {kind}: {err}"));
                    }
                }))
            }
            Kind::READ_REPLY => {
                let id = x.r#ref;
                let flags = x.flags;
                let body = x.body.clone();
                let seq = x.seq;
                let kind = x.kind;
                let shared = Arc::clone(&self.shared);
                Some(Box::new(move || {
                    if let Some(md) = plane.wiring.md()
                        && let Err(err) = md.replay_reply(id, flags, &body)
                    {
                        shared.mismatch(format!("rec_seq {seq} {kind}: {err}"));
                    }
                }))
            }
            Kind::TIMER => match self.disp().and_then(|d| d.timer_func(x.r#ref)) {
                Some(fn_once) => Some(fn_once),
                None if self.ended(x) => {
                    self.shared.mismatch(format!(
                        "rec_seq {}: timer {} ran in the recording; the replay did not make it",
                        x.seq, x.r#ref
                    ));
                    None
                }
                None => None,
            },
            Kind::POST => match self.disp().and_then(|d| d.take_post(x.r#ref)) {
                Some(fn_once) => Some(fn_once),
                None if self.ended(x) => {
                    self.shared.mismatch(format!(
                        "rec_seq {}: post {} ran in the recording; the replay did not make it",
                        x.seq, x.r#ref
                    ));
                    None
                }
                None => None,
            },
            _ => None,
        };
        let Some(fn_once) = fn_once else {
            return;
        };
        self.hook(x, || {
            fn_once();
            Ok(())
        });
    }

    fn ended(&self, x: &Record) -> bool {
        self.ends.contains_key(&x.seq)
    }

    fn disp(&self) -> Option<&Dispatcher> {
        self.plane.dispatcher()
    }

    fn hook(&mut self, x: &Record, f: impl FnOnce() -> Result<(), String>) {
        let Some(end) = self.ends.get(&x.seq).cloned() else {
            self.report.skipped += 1;
            return;
        };
        let Some(disp) = self.plane.dispatcher() else {
            return;
        };
        let mut err = None;
        let panicked = disp.step(x.t_ns, || match f() {
            Ok(()) => {}
            Err(text) => err = Some(text),
        });
        self.report.inputs += 1;
        let (flags, text) = dispatch::end_of(panicked.as_deref(), err.as_deref());
        if flags != end.flags || text.as_slice() != end.body.as_slice() {
            self.shared.mismatch(format!(
                "rec_seq {} {}: the hook ended {}; recorded {}",
                x.seq,
                x.kind,
                ending(flags, &text),
                ending(end.flags, &end.body)
            ));
        }
    }

    fn compare(&mut self) {
        let mut calls = Vec::new();
        let mut reads = Vec::new();
        let mut pubs = HashMap::new();
        for x in &self.recs {
            match x.kind {
                Kind::ORDER_CALL => calls.push(x.clone()),
                Kind::ORDER_PUBLISH => {
                    pubs.insert(x.r#ref, x.clone());
                }
                Kind::READ_CALL => reads.push(x.clone()),
                _ => {}
            }
        }
        let mut calls_again = Vec::new();
        let mut reads_again = Vec::new();
        let mut pubs_again = HashMap::new();
        for x in self.out.records() {
            match x.kind {
                Kind::ORDER_CALL => calls_again.push(x),
                Kind::ORDER_PUBLISH => {
                    pubs_again.insert(x.r#ref, x);
                }
                Kind::READ_CALL => reads_again.push(x),
                _ => {}
            }
        }
        self.report.orders = calls.len();
        self.report.reads = reads.len();
        for i in 0..calls.len().max(calls_again.len()) {
            match (calls.get(i), calls_again.get(i)) {
                (Some(a), None) => self.shared.mismatch(format!(
                    "order call {}: recorded {}; the replay did not make it",
                    a.r#ref,
                    describe_call(a)
                )),
                (None, Some(b)) => self.shared.mismatch(format!(
                    "order call {}: the replay made {}; the recording did not",
                    b.r#ref,
                    describe_call(b)
                )),
                (Some(a), Some(b))
                    if a.flags != b.flags || a.r#ref != b.r#ref || a.body != b.body =>
                {
                    self.shared.mismatch(format!(
                        "order call {}: recorded {}; replayed {}",
                        a.r#ref,
                        describe_call(a),
                        describe_call(b)
                    ));
                }
                _ => {}
            }
        }
        let mut ids: Vec<u64> = pubs.keys().copied().collect();
        ids.sort_unstable();
        for id in ids {
            let Some(y) = pubs_again.get(&id) else {
                self.shared.mismatch(format!(
                    "request of order call {id}: recorded; the replay did not send it"
                ));
                continue;
            };
            let a = OrderRequest::decode(pubs[&id].body.as_slice()).ok();
            let b = OrderRequest::decode(y.body.as_slice()).ok();
            if a.is_none() || b.is_none() || a != b {
                self.shared.mismatch(format!(
                    "request of order call {id}: recorded {a:?}; replayed {b:?}"
                ));
            }
        }
        for i in 0..reads.len().max(reads_again.len()) {
            match (reads.get(i), reads_again.get(i)) {
                (Some(a), None) => self.shared.mismatch(format!(
                    "read {}: recorded {}; the replay did not make it",
                    a.r#ref,
                    describe_read(a)
                )),
                (None, Some(b)) => self.shared.mismatch(format!(
                    "read {}: the replay made {}; the recording did not",
                    b.r#ref,
                    describe_read(b)
                )),
                (Some(a), Some(b))
                    if a.flags != b.flags || a.r#ref != b.r#ref || a.body != b.body =>
                {
                    self.shared.mismatch(format!(
                        "read {}: recorded {}; replayed {}",
                        a.r#ref,
                        describe_read(a),
                        describe_read(b)
                    ));
                }
                _ => {}
            }
        }
    }
}

fn ending(flags: u16, text: &[u8]) -> String {
    let quoted = String::from_utf8_lossy(text);
    match flags {
        0 => "normally".into(),
        eventlog::END_PANIC => format!("with panic {quoted:?}"),
        eventlog::END_ERROR => format!("with error {quoted:?}"),
        _ => format!("with flags {flags} {quoted:?}"),
    }
}

fn describe_call(x: &Record) -> String {
    match parse_order_call(&x.body) {
        Err(_) => format!("{} (malformed)", eventlog::op_name(x.flags)),
        Ok(c) => {
            let mut s = format!("{} api {}", eventlog::op_name(x.flags), c.api_id);
            if !c.cloids.is_empty() {
                s.push_str(" [");
                s.push_str(&c.cloids.join(" "));
                s.push(']');
            }
            s
        }
    }
}

fn describe_read(x: &Record) -> String {
    match parse_read_call(&x.body) {
        Err(_) => format!("{} (malformed)", eventlog::read_name(x.flags)),
        Ok((subject, _)) => format!("{} {subject}", eventlog::read_name(x.flags)),
    }
}

fn resolve_err(flags: u16, text: &[u8]) -> Error {
    let text = String::from_utf8_lossy(text).into_owned();
    match flags {
        outcome::NOT_IN_INIT => Error::NotInInit,
        outcome::NOT_FOUND => Error::Other(text),
        outcome::STOPPING => Error::Stopping,
        _ => Error::Other(text),
    }
}

/// What the replay sends: `order_call`, `order_publish`, `read_call`.
struct Memory {
    seq: AtomicU64,
    frames: Mutex<Vec<Record>>,
}

impl Memory {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            seq: AtomicU64::new(0),
            frames: Mutex::new(Vec::new()),
        })
    }

    fn records(&self) -> Vec<Record> {
        lock(&self.frames).clone()
    }
}

impl Recorder for Memory {
    fn now_ns(&self) -> i64 {
        0
    }

    fn append(
        &self,
        kind: Kind,
        flags: u16,
        recv_ns: i64,
        t_ns: i64,
        r#ref: u64,
        a: &[u8],
        b: &[u8],
    ) -> u64 {
        let seq = self.seq.fetch_add(1, Ordering::Relaxed) + 1;
        lock(&self.frames).push(Record {
            kind,
            flags,
            seq,
            recv_ns,
            t_ns,
            r#ref,
            body: [a, b].concat(),
        });
        seq
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
