//! Stderr log lines (E06-D004–E06-D017).
//!
//! One JSON object per line, or zerolog's console layout. The keys and their
//! order match Go zerolog, with `sdk_version` added. This module does not use
//! the `log` or `tracing` crates.
//!
//! A JSON line is rendered into its thread's buffer and written to stderr from
//! there. Once that buffer exists, a line allocates nothing (E06-D011).
//! Integers go through `itoa` and floats through `ryu` (E06-D012). The `time`
//! text up to the second is formatted once per second on each thread
//! (E06-D013). The console layout is rendered as before (E06-D016).

use std::cell::RefCell;
use std::io::{self, Write};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// `component` on every strategy line (E06-D008).
pub const COMPONENT: &str = "strategy";

/// This crate's package version and full git commit (E06-D008).
pub const SDK_VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "+", env!("VERGEN_GIT_SHA"));

/// zerolog's `Disabled`. A logger at this minimum emits nothing.
pub const DISABLED: i8 = 7;

const HEX: &[u8; 16] = b"0123456789abcdef";

/// A thread's JSON line buffer starts at this capacity.
const LINE_CAPACITY: usize = 1024;

/// A line buffer that grew past this is freed after its line, the cap zerolog
/// puts on the events it pools.
const LINE_KEEP: usize = 64 * 1024;

/// A level this module can emit. The discriminant is zerolog's `Level` value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i8)]
pub enum Level {
    Trace = -1,
    Debug = 0,
    Info = 1,
    Warn = 2,
    Error = 3,
    Fatal = 4,
    Panic = 5,
}

/// `MFTIKO_LOG_FORMAT`: JSON unless the value is `console`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Style {
    Json,
    Console,
}

/// A field value. Callers reject every other type before it reaches here.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Str(String),
    Int(i64),
    /// Decimal text of an integer that does not fit in `i64`.
    Big(String),
    Float(f64),
    Bool(bool),
    Null,
}

/// One field, in the order the caller supplied.
#[derive(Clone, Debug, PartialEq)]
pub struct Field {
    pub name: String,
    pub value: Value,
}

/// One line. `session_id` of `None` omits the key; `Some("")` still writes it.
struct Record<'a> {
    level: Level,
    now_ns: i64,
    component: &'a str,
    version: &'a str,
    commit: &'a str,
    sdk_version: &'a str,
    session_id: Option<&'a str>,
    fields: &'a [Field],
    message: &'a str,
}

/// An explicit logger. Tests and the benchmark use it so they do not touch
/// the process-wide level, which is read once.
pub struct Logger {
    pub min: i8,
    pub style: Style,
    pub no_color: bool,
    pub component: String,
    pub version: String,
    pub commit: String,
    pub sdk_version: String,
    pub session_id: Option<String>,
}

impl Logger {
    /// Renders one line and writes it. A level below `min` writes nothing.
    /// `Disabled` writes nothing. A short write is retried until the line is
    /// out or the sink returns an error (E06-D007).
    pub fn write(
        &self,
        level: Level,
        message: &str,
        fields: &[Field],
        now_ns: i64,
        out: &mut dyn Write,
    ) {
        if !allows(self.min, level as i8) {
            return;
        }
        let record = Record {
            level,
            now_ns,
            component: &self.component,
            version: &self.version,
            commit: &self.commit,
            sdk_version: &self.sdk_version,
            session_id: self.session_id.as_deref(),
            fields,
            message,
        };
        match self.style {
            Style::Json => with_scratch(|scratch| write_line(out, scratch.json(&record))),
            Style::Console => write_line(out, &console_line(&record, self.no_color)),
        }
    }
}

/// `MFTIKO_LOG_LEVEL`, the same rules as mftiko's wrapper around zerolog.
///
/// Trimmed, lowercased. Empty, unknown, and zerolog `NoLevel` (`6`) are info.
/// A number that fits in `i8` is kept, except `6`. Anything else is info.
pub fn parse_level(raw: &str) -> i8 {
    let text = raw.trim().to_ascii_lowercase();
    match text.as_str() {
        "" => Level::Info as i8,
        "trace" => Level::Trace as i8,
        "debug" => Level::Debug as i8,
        "info" => Level::Info as i8,
        "warn" => Level::Warn as i8,
        "error" => Level::Error as i8,
        "fatal" => Level::Fatal as i8,
        "panic" => Level::Panic as i8,
        "disabled" => DISABLED,
        _ => match text.parse::<i64>() {
            Ok(6) => Level::Info as i8,
            Ok(number) if (-128..=127).contains(&number) => number as i8,
            _ => Level::Info as i8,
        },
    }
}

/// `MFTIKO_LOG_FORMAT`. Only a trimmed, case-insensitive `console` selects it.
pub fn parse_format(raw: &str) -> Style {
    if raw.trim().eq_ignore_ascii_case("console") {
        Style::Console
    } else {
        Style::Json
    }
}

/// Zerolog's `should`, without a global level or a sampler.
pub fn allows(min: i8, level: i8) -> bool {
    min != DISABLED && level >= min
}

/// The strategy's `version` and `commit`, read on each line (E06-D008).
pub fn set_strategy_identity(version: &str, commit: &str) {
    let mut guard = identity().write().unwrap_or_else(|err| err.into_inner());
    guard.version = version.to_owned();
    guard.commit = commit.to_owned();
}

/// Remembered for [`note`] once the plane has a session id.
pub(crate) fn set_note_session(id: &str) {
    let mut guard = note_session()
        .write()
        .unwrap_or_else(|err| err.into_inner());
    *guard = Some(Arc::from(id));
}

/// `plane.note`: info, the text as the message, `session_id` only when set.
pub(crate) fn note(line: &str) {
    let session = note_session()
        .read()
        .unwrap_or_else(|err| err.into_inner())
        .clone();
    emit(
        Level::Info,
        line,
        &[],
        session.as_deref().filter(|id| !id.is_empty()),
    );
}

/// Whether [`emit`] writes a line at `level`. A caller checks this before it
/// builds the fields (E06-D017).
pub fn enabled(level: Level) -> bool {
    allows(config().min, level as i8)
}

/// One line on stderr, or into the buffer installed by [`capture`].
pub fn emit(level: Level, message: &str, fields: &[Field], session_id: Option<&str>) {
    let cfg = config();
    if !allows(cfg.min, level as i8) {
        return;
    }
    let now_ns = now_ns();
    match cfg.style {
        Style::Json => with_scratch(|scratch| {
            let guard = identity().read().unwrap_or_else(|err| err.into_inner());
            let line = scratch.json(&guard.record(level, now_ns, session_id, fields, message));
            drop(guard);
            write_out(line);
        }),
        Style::Console => {
            let guard = identity().read().unwrap_or_else(|err| err.into_inner());
            let record = guard.record(level, now_ns, session_id, fields, message);
            let line = console_line(&record, cfg.no_color);
            drop(guard);
            write_out(&line);
        }
    }
}

/// Diverts [`emit`] on this thread into a buffer, then restores stderr.
pub fn capture<R>(body: impl FnOnce() -> R) -> (R, Vec<u8>) {
    CAPTURE.with(|slot| *slot.borrow_mut() = Some(Vec::new()));
    let armed = CaptureGuard;
    let result = body();
    let bytes = CAPTURE.with(|slot| slot.borrow_mut().take().unwrap_or_default());
    drop(armed);
    (result, bytes)
}

struct CaptureGuard;

impl Drop for CaptureGuard {
    fn drop(&mut self) {
        CAPTURE.with(|slot| {
            slot.borrow_mut().take();
        });
    }
}

thread_local! {
    static CAPTURE: RefCell<Option<Vec<u8>>> = const { RefCell::new(None) };
    static SCRATCH: RefCell<Scratch> = const { RefCell::new(Scratch::new()) };
}

/// One thread's JSON line, and the `time` text of the last second it wrote.
struct Scratch {
    line: Vec<u8>,
    stamp: Stamp,
}

impl Scratch {
    const fn new() -> Self {
        Self {
            line: Vec::new(),
            stamp: Stamp::new(),
        }
    }

    /// The JSON line for `record`, newline included.
    fn json(&mut self, record: &Record<'_>) -> &[u8] {
        self.line.clear();
        self.line.reserve(LINE_CAPACITY);
        render_json(record, &mut self.stamp, &mut self.line);
        &self.line
    }
}

/// Runs `body` with this thread's [`Scratch`]. A line rendered while it is in
/// use, or after the thread has dropped it, gets a new one instead.
fn with_scratch(body: impl FnOnce(&mut Scratch)) {
    let mut body = Some(body);
    let _ = SCRATCH.try_with(|cell| {
        let Ok(mut scratch) = cell.try_borrow_mut() else {
            return;
        };
        if let Some(body) = body.take() {
            body(&mut scratch);
        }
        if scratch.line.capacity() > LINE_KEEP {
            scratch.line = Vec::new();
        }
    });
    if let Some(body) = body {
        body(&mut Scratch::new());
    }
}

struct Identity {
    version: String,
    commit: String,
}

impl Identity {
    /// A strategy line under this identity (E06-D008).
    fn record<'a>(
        &'a self,
        level: Level,
        now_ns: i64,
        session_id: Option<&'a str>,
        fields: &'a [Field],
        message: &'a str,
    ) -> Record<'a> {
        Record {
            level,
            now_ns,
            component: COMPONENT,
            version: &self.version,
            commit: &self.commit,
            sdk_version: SDK_VERSION,
            session_id,
            fields,
            message,
        }
    }
}

fn identity() -> &'static RwLock<Identity> {
    static IDENTITY: RwLock<Identity> = RwLock::new(Identity {
        version: String::new(),
        commit: String::new(),
    });
    &IDENTITY
}

/// An `Arc` so that [`note`] borrows the id without copying it.
fn note_session() -> &'static RwLock<Option<Arc<str>>> {
    static SESSION: RwLock<Option<Arc<str>>> = RwLock::new(None);
    &SESSION
}

struct Config {
    min: i8,
    style: Style,
    no_color: bool,
}

/// Level, format, and `NO_COLOR` are sampled on first use. Tests that care
/// about a particular value build a [`Logger`] instead of mutating the
/// process environment.
fn config() -> &'static Config {
    static CONFIG: OnceLock<Config> = OnceLock::new();
    CONFIG.get_or_init(|| {
        let level = std::env::var("MFTIKO_LOG_LEVEL").unwrap_or_default();
        let format = std::env::var("MFTIKO_LOG_FORMAT").unwrap_or_default();
        let no_color = std::env::var("NO_COLOR").is_ok_and(|value| !value.is_empty());
        Config {
            min: parse_level(&level),
            style: parse_format(&format),
            no_color,
        }
    })
}

fn now_ns() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX),
        Err(err) => -i64::try_from(err.duration().as_nanos()).unwrap_or(i64::MAX),
    }
}

/// The console layout allocates its line, as before (E06-D016).
fn console_line(record: &Record<'_>, no_color: bool) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
    render_console(record, no_color, &mut buf);
    buf.push(b'\n');
    buf
}

fn render_json(record: &Record<'_>, stamp: &mut Stamp, buf: &mut Vec<u8>) {
    buf.extend_from_slice(level_head(record.level));
    stamp.append(buf, record.now_ns);
    buf.extend_from_slice(br#"","component":"#);
    append_json_str(buf, record.component);
    buf.extend_from_slice(br#","version":"#);
    append_json_str(buf, record.version);
    buf.extend_from_slice(br#","commit":"#);
    append_json_str(buf, record.commit);
    buf.extend_from_slice(br#","sdk_version":"#);
    append_json_str(buf, record.sdk_version);
    if let Some(session) = record.session_id {
        buf.extend_from_slice(br#","session_id":"#);
        append_json_str(buf, session);
    }
    for field in record.fields {
        buf.push(b',');
        append_json_str(buf, &field.name);
        buf.push(b':');
        append_value(buf, &field.value);
    }
    buf.extend_from_slice(br#","message":"#);
    append_json_str(buf, record.message);
    buf.extend_from_slice(b"}\n");
}

/// Every JSON line up to the `time` text.
fn level_head(level: Level) -> &'static [u8] {
    match level {
        Level::Trace => br#"{"level":"trace","time":""#,
        Level::Debug => br#"{"level":"debug","time":""#,
        Level::Info => br#"{"level":"info","time":""#,
        Level::Warn => br#"{"level":"warn","time":""#,
        Level::Error => br#"{"level":"error","time":""#,
        Level::Fatal => br#"{"level":"fatal","time":""#,
        Level::Panic => br#"{"level":"panic","time":""#,
    }
}

fn append_value(buf: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Str(text) => append_json_str(buf, text),
        Value::Int(number) => buf.extend_from_slice(itoa::Buffer::new().format(*number).as_bytes()),
        Value::Big(text) => buf.extend_from_slice(text.as_bytes()),
        Value::Float(number) => append_float(buf, *number),
        Value::Bool(true) => buf.extend_from_slice(b"true"),
        Value::Bool(false) => buf.extend_from_slice(b"false"),
        Value::Null => buf.extend_from_slice(b"null"),
    }
}

/// zerolog's string encoder: UTF-8 passes through, controls are `\u00XX`.
fn append_json_str(buf: &mut Vec<u8>, text: &str) {
    let bytes = text.as_bytes();
    buf.push(b'"');
    // No early exit, so that the scan vectorizes.
    if bytes
        .iter()
        .fold(false, |found, &byte| found | escapes(byte))
    {
        append_escaped(buf, bytes);
    } else {
        buf.extend_from_slice(bytes);
    }
    buf.push(b'"');
}

/// The bytes zerolog's `noEscapeTable` sends to its slow path, except that
/// bytes from 0x80 up are always valid UTF-8 here and pass through.
fn escapes(byte: u8) -> bool {
    (byte < 0x20) | (byte == b'"') | (byte == b'\\') | (byte == 0x7f)
}

fn append_escaped(buf: &mut Vec<u8>, bytes: &[u8]) {
    let mut start = 0;
    for (index, &byte) in bytes.iter().enumerate() {
        if !escapes(byte) {
            continue;
        }
        buf.extend_from_slice(&bytes[start..index]);
        match byte {
            b'"' | b'\\' => {
                buf.push(b'\\');
                buf.push(byte);
            }
            0x08 => buf.extend_from_slice(br"\b"),
            0x0c => buf.extend_from_slice(br"\f"),
            b'\n' => buf.extend_from_slice(br"\n"),
            b'\r' => buf.extend_from_slice(br"\r"),
            b'\t' => buf.extend_from_slice(br"\t"),
            _ => {
                buf.extend_from_slice(br"\u00");
                buf.push(HEX[(byte >> 4) as usize]);
                buf.push(HEX[(byte & 0x0f) as usize]);
            }
        }
        start = index + 1;
    }
    buf.extend_from_slice(&bytes[start..]);
}

/// zerolog `appendFloat` with precision -1 (E06-D012). NaN and the
/// infinities are JSON strings.
fn append_float(buf: &mut Vec<u8>, value: f64) {
    match non_finite(value) {
        Some(text) => {
            buf.push(b'"');
            buf.extend_from_slice(text.as_bytes());
            buf.push(b'"');
        }
        None => append_finite(buf, value),
    }
}

/// zerolog's text for NaN and the infinities.
fn non_finite(value: f64) -> Option<&'static str> {
    if value.is_nan() {
        Some("NaN")
    } else if value == f64::INFINITY {
        Some("+Inf")
    } else if value == f64::NEG_INFINITY {
        Some("-Inf")
    } else {
        None
    }
}

/// `strconv.AppendFloat(v, f, -1, 64)`: `f` is `'e'` when the magnitude is
/// below 1e-6 or at least 1e21 and `'f'` otherwise. zerolog then turns
/// `e-0X` into `e-X`.
///
/// ryu gives the same shortest digits. Its text differs from Go's in three
/// places: a whole number ends in `.0`, a positive exponent has no `+`, and
/// it switches to an exponent outside `[1e-5, 1e16)` where Go's `'f'` range
/// is `[1e-6, 1e21)`. ryu writes no leading zero in an exponent, which is
/// already zerolog's `e-X`.
fn append_finite(buf: &mut Vec<u8>, value: f64) {
    let mut ryu = ryu::Buffer::new();
    let text = ryu.format_finite(value).as_bytes();
    let Some(at) = text.iter().position(|&byte| byte == b'e') else {
        buf.extend_from_slice(text.strip_suffix(b".0").unwrap_or(text));
        return;
    };
    let (mantissa, exp) = (&text[..at], &text[at + 1..]);
    let abs = value.abs();
    if abs < 1e-6 || abs >= 1e21 {
        buf.extend_from_slice(mantissa);
        buf.push(b'e');
        if exp[0] != b'-' {
            buf.push(b'+');
        }
        buf.extend_from_slice(exp);
    } else {
        append_fixed(buf, mantissa, parse_exp(exp));
    }
}

/// ryu's `d.ddd` (or `d`) mantissa times ten to `exp`, in Go's `'f'` layout.
fn append_fixed(buf: &mut Vec<u8>, mantissa: &[u8], exp: i32) {
    let digits = match mantissa.strip_prefix(b"-") {
        Some(digits) => {
            buf.push(b'-');
            digits
        }
        None => mantissa,
    };
    let (lead, rest) = digits.split_at(1);
    let rest = rest.strip_prefix(b".").unwrap_or(rest);
    match usize::try_from(exp) {
        Err(_) => {
            buf.extend_from_slice(b"0.");
            let zeros = exp.unsigned_abs() as usize - 1;
            buf.resize(buf.len() + zeros, b'0');
            buf.extend_from_slice(lead);
            buf.extend_from_slice(rest);
        }
        Ok(whole) if whole >= rest.len() => {
            buf.extend_from_slice(lead);
            buf.extend_from_slice(rest);
            buf.resize(buf.len() + whole - rest.len(), b'0');
        }
        Ok(whole) => {
            buf.extend_from_slice(lead);
            buf.extend_from_slice(&rest[..whole]);
            buf.push(b'.');
            buf.extend_from_slice(&rest[whole..]);
        }
    }
}

fn parse_exp(text: &[u8]) -> i32 {
    let (negative, digits) = match text.strip_prefix(b"-") {
        Some(digits) => (true, digits),
        None => (false, text),
    };
    let magnitude = digits
        .iter()
        .fold(0, |acc, &digit| acc * 10 + i32::from(digit - b'0'));
    if negative { -magnitude } else { magnitude }
}

/// The console's float text, kept as it was (E06-D016). It can differ from
/// [`append_float`] in the last digit: where two shortest texts are equally
/// close, Rust's `{}` rounds away from zero and Go's `strconv` to even.
fn float_body(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_owned();
    }
    if value.is_infinite() {
        return if value.is_sign_positive() {
            "+Inf".to_owned()
        } else {
            "-Inf".to_owned()
        };
    }
    let abs = value.abs();
    if abs != 0.0 && (abs < 1e-6 || abs >= 1e21) {
        scientific(value)
    } else {
        format!("{value}")
    }
}

fn scientific(value: f64) -> String {
    let raw = format!("{value:?}");
    let mut out = String::with_capacity(raw.len() + 1);
    let bytes = raw.as_bytes();
    for (index, byte) in bytes.iter().copied().enumerate() {
        out.push(char::from(byte));
        if byte == b'e' && bytes.get(index + 1).is_some_and(u8::is_ascii_digit) {
            out.push('+');
        }
    }
    if let Some(pos) = out.rfind("e-0") {
        let rest = &out[pos..];
        if rest.len() == 4 && rest.as_bytes()[3].is_ascii_digit() {
            out.remove(pos + 2);
        }
    }
    out
}

fn render_console(record: &Record<'_>, no_color: bool, buf: &mut Vec<u8>) {
    let stamp = format_local_rfc3339(record.now_ns.div_euclid(1_000_000_000));
    write_color(buf, &stamp, 90, no_color);
    buf.push(b' ');
    let (abbrev, color) = level_console(record.level);
    write_color(buf, abbrev, color, no_color);
    if !record.message.is_empty() {
        buf.push(b' ');
        if message_bold(record.level) {
            write_color(buf, record.message, 1, no_color);
        } else {
            buf.extend_from_slice(record.message.as_bytes());
        }
    }
    let pairs = console_pairs(record);
    if !pairs.is_empty() {
        buf.push(b' ');
    }
    for (index, (name, value)) in pairs.iter().enumerate() {
        if index > 0 {
            buf.push(b' ');
        }
        let labeled = format!("{name}=");
        write_color(buf, &labeled, 36, no_color);
        if name == "error" {
            write_error_value(buf, value, no_color);
        } else {
            buf.extend_from_slice(value.as_bytes());
        }
    }
}

fn level_console(level: Level) -> (&'static str, u8) {
    match level {
        Level::Trace => ("TRC", 34),
        Level::Debug => ("DBG", 0),
        Level::Info => ("INF", 32),
        Level::Warn => ("WRN", 33),
        Level::Error => ("ERR", 31),
        Level::Fatal => ("FTL", 31),
        Level::Panic => ("PNC", 31),
    }
}

fn message_bold(level: Level) -> bool {
    matches!(
        level,
        Level::Info | Level::Warn | Level::Error | Level::Fatal | Level::Panic
    )
}

fn console_pairs(record: &Record<'_>) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    let mut push = |name: &str, text: String| pairs.push((name.to_owned(), text));
    push("component", console_text_str(record.component));
    push("version", console_text_str(record.version));
    push("commit", console_text_str(record.commit));
    push("sdk_version", console_text_str(record.sdk_version));
    if let Some(session) = record.session_id {
        push("session_id", console_text_str(session));
    }
    for field in record.fields {
        push(&field.name, console_text_value(&field.value));
    }
    pairs.sort_by(|left, right| left.0.cmp(&right.0));
    if let Some(index) = pairs.iter().position(|(name, _)| name == "error") {
        let error = pairs.remove(index);
        pairs.insert(0, error);
    }
    pairs
}

fn console_text_value(value: &Value) -> String {
    match value {
        Value::Str(text) => console_text_str(text),
        Value::Int(number) => number.to_string(),
        Value::Big(text) => text.clone(),
        Value::Float(number) => float_body(*number),
        Value::Bool(true) => "true".to_owned(),
        Value::Bool(false) => "false".to_owned(),
        Value::Null => "null".to_owned(),
    }
}

fn console_text_str(text: &str) -> String {
    if needs_quote(text) {
        go_quote(text)
    } else {
        text.to_owned()
    }
}

fn needs_quote(text: &str) -> bool {
    text.bytes()
        .any(|byte| !(0x20..=0x7e).contains(&byte) || byte == b' ' || byte == b'\\' || byte == b'"')
}

/// `strconv.Quote` for a console field that [`needs_quote`].
fn go_quote(text: &str) -> String {
    let mut buf = String::with_capacity(text.len() + 2);
    buf.push('"');
    for ch in text.chars() {
        match ch {
            '"' | '\\' => {
                buf.push('\\');
                buf.push(ch);
            }
            '\u{7}' => buf.push_str(r"\a"),
            '\u{8}' => buf.push_str(r"\b"),
            '\u{c}' => buf.push_str(r"\f"),
            '\n' => buf.push_str(r"\n"),
            '\r' => buf.push_str(r"\r"),
            '\t' => buf.push_str(r"\t"),
            '\u{b}' => buf.push_str(r"\v"),
            ch if go_print(ch) => buf.push(ch),
            ch => {
                let code = u32::from(ch);
                if code < 0x20 || code == 0x7f {
                    buf.push_str(&format!("\\x{code:02x}"));
                } else if code < 0x1_0000 {
                    buf.push_str(&format!("\\u{code:04x}"));
                } else {
                    buf.push_str(&format!("\\U{code:08x}"));
                }
            }
        }
    }
    buf.push('"');
    buf
}

fn go_print(ch: char) -> bool {
    if ch.is_ascii() {
        matches!(ch, '\u{20}'..='\u{7e}')
    } else {
        !ch.is_control()
    }
}

fn write_color(buf: &mut Vec<u8>, text: &str, code: u8, no_color: bool) {
    if no_color || code == 0 {
        buf.extend_from_slice(text.as_bytes());
        return;
    }
    buf.extend_from_slice(b"\x1b[");
    buf.extend_from_slice(code.to_string().as_bytes());
    buf.push(b'm');
    buf.extend_from_slice(text.as_bytes());
    buf.extend_from_slice(b"\x1b[0m");
}

fn write_error_value(buf: &mut Vec<u8>, text: &str, no_color: bool) {
    if no_color {
        buf.extend_from_slice(text.as_bytes());
        return;
    }
    buf.extend_from_slice(b"\x1b[31m\x1b[1m");
    buf.extend_from_slice(text.as_bytes());
    buf.extend_from_slice(b"\x1b[0m\x1b[0m");
}

/// `time` as RFC3339Nano in UTC (E06-D013). The text up to the second is
/// kept for the second it was formatted for.
struct Stamp {
    secs: Option<i64>,
    head: [u8; 32],
    len: usize,
}

impl Stamp {
    const fn new() -> Self {
        Self {
            secs: None,
            head: [0; 32],
            len: 0,
        }
    }

    fn append(&mut self, buf: &mut Vec<u8>, unix_ns: i64) {
        let secs = unix_ns.div_euclid(1_000_000_000);
        if self.secs != Some(secs) {
            let (year, month, day, hour, min, sec) = civil_hms(secs);
            let mut head = io::Cursor::new(&mut self.head[..]);
            let _ = write!(
                head,
                "{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}"
            );
            self.len = head.position() as usize;
            self.secs = Some(secs);
        }
        buf.extend_from_slice(&self.head[..self.len]);
        append_nanos(buf, unix_ns.rem_euclid(1_000_000_000) as u32);
    }
}

/// `.` and the nanoseconds without trailing zeros, then `Z`. Zero is `Z`.
fn append_nanos(buf: &mut Vec<u8>, nanos: u32) {
    if nanos == 0 {
        buf.push(b'Z');
        return;
    }
    let mut text = [b'.'; 10];
    let mut rest = nanos;
    for slot in text[1..].iter_mut().rev() {
        *slot = b'0' + (rest % 10) as u8;
        rest /= 10;
    }
    let mut end = text.len();
    while text[end - 1] == b'0' {
        end -= 1;
    }
    buf.extend_from_slice(&text[..end]);
    buf.push(b'Z');
}

fn format_local_rfc3339(secs: i64) -> String {
    let mut broken = unsafe { std::mem::zeroed::<libc::tm>() };
    let clock = secs as libc::time_t;
    // SAFETY: `clock` is a live `time_t` and `broken` is a live `libc::tm`.
    // `localtime_r` writes the civil time into `broken` and keeps neither pointer.
    let ptr = unsafe { libc::localtime_r(&clock, &mut broken) };
    if ptr.is_null() {
        return format_utc_secs(secs);
    }
    // SAFETY: `localtime_r` returned `broken`.
    let broken = unsafe { &*ptr };
    let year = i64::from(broken.tm_year) + 1900;
    let month = broken.tm_mon + 1;
    let day = broken.tm_mday;
    let hour = broken.tm_hour;
    let min = broken.tm_min;
    let sec = broken.tm_sec;
    format!(
        "{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}{}",
        zone_suffix(broken.tm_gmtoff as i64)
    )
}

fn format_utc_secs(secs: i64) -> String {
    let (year, month, day, hour, min, sec) = civil_hms(secs);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}

fn zone_suffix(offset: i64) -> String {
    if offset == 0 {
        return "Z".to_owned();
    }
    let sign = if offset < 0 { '-' } else { '+' };
    let offset = offset.unsigned_abs();
    let hours = offset / 3600;
    let mins = (offset % 3600) / 60;
    format!("{sign}{hours:02}:{mins:02}")
}

/// Howard Hinnant's `civil_from_days`, then the time of day.
fn civil_hms(secs: i64) -> (i64, u32, u32, u32, u32, u32) {
    let days = secs.div_euclid(86_400);
    let tod = secs.rem_euclid(86_400) as u32;
    let (year, month, day) = civil_from_days(days);
    let hour = tod / 3600;
    let min = (tod % 3600) / 60;
    let sec = tod % 60;
    (year, month, day, hour, min, sec)
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = u64::try_from(z - era * 146_097).unwrap_or(0);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let mut year = i64::try_from(yoe).unwrap_or(0) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    if month <= 2 {
        year += 1;
    }
    (
        year,
        u32::try_from(month).unwrap_or(0),
        u32::try_from(day).unwrap_or(0),
    )
}

fn write_line(out: &mut dyn Write, line: &[u8]) {
    let _ = out.write_all(line);
}

fn write_out(line: &[u8]) {
    let captured = CAPTURE.with(|slot| {
        if let Some(buf) = slot.borrow_mut().as_mut() {
            let _ = buf.write_all(line);
            true
        } else {
            false
        }
    });
    if captured {
        return;
    }
    let mut stderr = io::stderr().lock();
    let _ = stderr.write_all(line);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn logger(style: Style, no_color: bool) -> Logger {
        Logger {
            min: Level::Info as i8,
            style,
            no_color,
            component: COMPONENT.to_owned(),
            version: String::new(),
            commit: String::new(),
            sdk_version: SDK_VERSION.to_owned(),
            session_id: None,
        }
    }

    fn line(logger: &Logger, level: Level, message: &str, fields: &[Field], now_ns: i64) -> String {
        let mut buf = Vec::new();
        logger.write(level, message, fields, now_ns, &mut buf);
        String::from_utf8(buf).expect("utf-8")
    }

    fn json_line(message: &str, fields: &[Field], now_ns: i64, session: Option<&str>) -> String {
        let mut logger = logger(Style::Json, true);
        logger.session_id = session.map(str::to_owned);
        line(&logger, Level::Info, message, fields, now_ns)
    }

    #[test]
    fn sdk_version_is_package_version_plus_full_sha() {
        let (version, sha) = SDK_VERSION.split_once('+').expect("version and commit");
        assert_eq!(version, env!("CARGO_PKG_VERSION"));
        assert_eq!(sha.len(), 40);
        assert!(sha.chars().all(|ch| ch.is_ascii_hexdigit()));
    }

    #[test]
    fn json_key_order_keeps_empty_identity_and_omits_session() {
        let text = json_line("hello", &[], 0, None);
        let expect = format!(
            "{{\"level\":\"info\",\"time\":\"1970-01-01T00:00:00Z\",\"component\":\"strategy\",\"version\":\"\",\"commit\":\"\",\"sdk_version\":\"{SDK_VERSION}\",\"message\":\"hello\"}}\n"
        );
        assert_eq!(text, expect);
        assert!(!text.contains("session_id"));
        assert!(!text.contains("mftik sdk:"));
    }

    #[test]
    fn json_writes_session_fields_then_message() {
        let fields = [
            Field {
                name: "k".to_owned(),
                value: Value::Str("v1".to_owned()),
            },
            Field {
                name: "n".to_owned(),
                value: Value::Int(7),
            },
        ];
        let text = json_line("hello", &fields, 0, Some("s1"));
        let tail = format!(
            "\"sdk_version\":\"{SDK_VERSION}\",\"session_id\":\"s1\",\"k\":\"v1\",\"n\":7,\"message\":\"hello\"}}\n"
        );
        assert!(text.ends_with(&tail), "{text}");
    }

    #[test]
    fn empty_session_id_is_still_written() {
        let text = json_line("hello", &[], 0, Some(""));
        assert!(text.contains("\"session_id\":\"\""), "{text}");
    }

    #[test]
    fn json_scalars_and_escapes() {
        let fields = [
            Field {
                name: "on".to_owned(),
                value: Value::Bool(true),
            },
            Field {
                name: "off".to_owned(),
                value: Value::Bool(false),
            },
            Field {
                name: "missing".to_owned(),
                value: Value::Null,
            },
            Field {
                name: "wide".to_owned(),
                value: Value::Big("18446744073709551616".to_owned()),
            },
            Field {
                name: "low".to_owned(),
                value: Value::Int(i64::MIN),
            },
            Field {
                name: "high".to_owned(),
                value: Value::Int(i64::MAX),
            },
            Field {
                name: "a\"b".to_owned(),
                value: Value::Float(1e21),
            },
        ];
        let message = "a\"b\\c\n\u{7f}<café>";
        let text = json_line(message, &fields, 0, None);
        assert!(
            text.contains(concat!(
                r#""on":true,"off":false,"missing":null,"wide":18446744073709551616,"#,
                r#""low":-9223372036854775808,"high":9223372036854775807,"a\"b":1e+21,"#,
            )),
            "{text}"
        );
        assert!(
            text.contains(r#""message":"a\"b\\c\n\u007f<café>""#),
            "{text}"
        );
    }

    /// Expected text from Go zerolog v1.35.1 `Str`.
    #[test]
    fn strings_match_zerolog_append_string() {
        let ascii: String = (0u8..0x80).map(char::from).collect();
        let cases = [
            ("", r#""""#),
            ("hello", r#""hello""#),
            ("a\"b", r#""a\"b""#),
            ("a\\b", r#""a\\b""#),
            ("\u{8}\u{c}\n\r\t", r#""\b\f\n\r\t""#),
            ("\u{0}\u{1}\u{1f}", r#""\u0000\u0001\u001f""#),
            ("\u{7f}", r#""\u007f""#),
            ("café 日本 😀", r#""café 日本 😀""#),
            (
                "0123456789abcdef0123456789abcdef\"",
                r#""0123456789abcdef0123456789abcdef\"""#,
            ),
            (
                "\"0123456789abcdef0123456789abcdef",
                r#""\"0123456789abcdef0123456789abcdef""#,
            ),
            (
                ascii.as_str(),
                concat!(
                    r#""\u0000\u0001\u0002\u0003\u0004\u0005\u0006\u0007\b\t\n\u000b\f\r"#,
                    r#"\u000e\u000f\u0010\u0011\u0012\u0013\u0014\u0015\u0016\u0017\u0018"#,
                    r##"\u0019\u001a\u001b\u001c\u001d\u001e\u001f !\"#$%&'()*+,-./"##,
                    r#"0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`"#,
                    r#"abcdefghijklmnopqrstuvwxyz{|}~\u007f""#,
                ),
            ),
        ];
        for (text, expect) in cases {
            let mut buf = Vec::new();
            append_json_str(&mut buf, text);
            assert_eq!(String::from_utf8(buf).expect("utf-8"), expect, "{text:?}");
        }
    }

    /// Expected text from Go `time.Time.Format(time.RFC3339Nano)` in UTC.
    /// One [`Stamp`] runs through all of them, so a second is reused and
    /// replaced in both directions.
    #[test]
    fn times_match_rfc3339_nano() {
        let cases = [
            (0, "1970-01-01T00:00:00Z"),
            (1, "1970-01-01T00:00:00.000000001Z"),
            (-1, "1969-12-31T23:59:59.999999999Z"),
            (-1_000_000_000, "1969-12-31T23:59:59Z"),
            (1_700_000_000_123_456_789, "2023-11-14T22:13:20.123456789Z"),
            (1_700_000_000_100_000_000, "2023-11-14T22:13:20.1Z"),
            (1_700_000_000_120_000_000, "2023-11-14T22:13:20.12Z"),
            (1_700_000_001_000_000_000, "2023-11-14T22:13:21Z"),
            (951_782_400_000_000_000, "2000-02-29T00:00:00Z"),
            (i64::MAX, "2262-04-11T23:47:16.854775807Z"),
            (i64::MIN, "1677-09-21T00:12:43.145224192Z"),
        ];
        let mut stamp = Stamp::new();
        for (unix_ns, expect) in cases {
            let mut buf = Vec::new();
            stamp.append(&mut buf, unix_ns);
            assert_eq!(String::from_utf8(buf).expect("utf-8"), expect, "{unix_ns}");
        }
    }

    /// Expected text from Go zerolog v1.35.1 `Float64` (E06-D012).
    #[test]
    #[allow(clippy::excessive_precision)]
    fn floats_match_zerolog_append_float() {
        let cases = [
            (0.0, "0"),
            (-0.0, "-0"),
            (1.0, "1"),
            (-1.0, "-1"),
            (0.5, "0.5"),
            (1.5, "1.5"),
            (-2.25, "-2.25"),
            (100.0, "100"),
            (1234.5678, "1234.5678"),
            (123_456_789.0, "123456789"),
            (0.1, "0.1"),
            (0.2, "0.2"),
            (0.3, "0.3"),
            (0.1 + 0.2, "0.30000000000000004"),
            (1.0 / 3.0, "0.3333333333333333"),
            (-2.0 / 3.0, "-0.6666666666666666"),
            (std::f64::consts::PI, "3.141592653589793"),
            (std::f64::consts::E, "2.718281828459045"),
            (1.0 + f64::EPSILON, "1.0000000000000002"),
            (f64::EPSILON, "2.220446049250313e-16"),
            (4.35, "4.35"),
            (2f64.powi(53), "9007199254740992"),
            (0.000123, "0.000123"),
            (0.001, "0.001"),
            (0.0001, "0.0001"),
            (1e-5, "0.00001"),
            (1.234e-5, "0.00001234"),
            (
                f64::from_bits(1e-5f64.to_bits() - 1),
                "0.000009999999999999999",
            ),
            (1.5e-6, "0.0000015"),
            (1e-6, "0.000001"),
            (-1e-6, "-0.000001"),
            (
                f64::from_bits(1e-6f64.to_bits() + 1),
                "0.0000010000000000000002",
            ),
            (
                f64::from_bits(1e-6f64.to_bits() - 1),
                "9.999999999999997e-7",
            ),
            (9.9e-7, "9.9e-7"),
            (5e-7, "5e-7"),
            (2.5e-7, "2.5e-7"),
            (1.2e-7, "1.2e-7"),
            (1e-7, "1e-7"),
            (-1e-7, "-1e-7"),
            (1e-8, "1e-8"),
            (1e-9, "1e-9"),
            (1e-10, "1e-10"),
            (1e-300, "1e-300"),
            (f64::MIN_POSITIVE, "2.2250738585072014e-308"),
            (
                f64::from_bits(f64::MIN_POSITIVE.to_bits() - 1),
                "2.225073858507201e-308",
            ),
            (1e-308, "1e-308"),
            (1e-320, "1e-320"),
            (f64::from_bits(1), "5e-324"),
            (-f64::from_bits(1), "-5e-324"),
            (1e15, "1000000000000000"),
            (1e15 + 0.3, "1000000000000000.2"),
            (1e16, "10000000000000000"),
            (-1e16, "-10000000000000000"),
            (1.5e16, "15000000000000000"),
            (12_345_678_901_234_567.0, "12345678901234568"),
            (1e17, "100000000000000000"),
            (2f64.powi(63), "9223372036854776000"),
            (2f64.powi(64), "18446744073709552000"),
            (1e20, "100000000000000000000"),
            (
                f64::from_bits(1e21f64.to_bits() - 1),
                "999999999999999900000",
            ),
            (1e21, "1e+21"),
            (-1e21, "-1e+21"),
            (1.23e21, "1.23e+21"),
            (1e22, "1e+22"),
            (1e100, "1e+100"),
            (f64::MAX, "1.7976931348623157e+308"),
            (-f64::MAX, "-1.7976931348623157e+308"),
            (f64::NAN, r#""NaN""#),
            (-f64::NAN, r#""NaN""#),
            (f64::INFINITY, r#""+Inf""#),
            (f64::NEG_INFINITY, r#""-Inf""#),
            // Exact binary values on a decimal tie: Go rounds them half to even.
            (1_245_315_403_051_669.25, "1245315403051669.2"),
            (135_836_305_628_408.625, "135836305628408.62"),
        ];
        for (value, expect) in cases {
            let mut buf = Vec::new();
            append_float(&mut buf, value);
            assert_eq!(String::from_utf8(buf).expect("utf-8"), expect, "{value:?}");
        }
    }

    #[test]
    fn a_thread_keeps_its_line_buffer_unless_a_line_outgrew_the_cap() {
        let logger = logger(Style::Json, true);
        let capacity = || SCRATCH.with(|cell| cell.borrow().line.capacity());
        let mut sink = Vec::new();
        logger.write(Level::Info, "hello", &[], 0, &mut sink);
        let kept = capacity();
        assert!((LINE_CAPACITY..=LINE_KEEP).contains(&kept), "{kept}");
        logger.write(Level::Info, "hello", &[], 0, &mut sink);
        assert_eq!(capacity(), kept);

        sink.clear();
        let long = "x".repeat(LINE_KEEP);
        logger.write(Level::Info, &long, &[], 0, &mut sink);
        assert!(sink.ends_with(format!("\"message\":\"{long}\"}}\n").as_bytes()));
        assert_eq!(capacity(), 0);

        sink.clear();
        logger.write(Level::Info, "hello", &[], 0, &mut sink);
        assert_eq!(sink, json_line("hello", &[], 0, None).as_bytes());
    }

    #[test]
    fn a_line_logged_from_inside_a_sink_gets_its_own_buffer() {
        struct Nested<'a> {
            logger: &'a Logger,
            inner: Vec<u8>,
            outer: Vec<u8>,
        }

        impl Write for Nested<'_> {
            fn write(&mut self, data: &[u8]) -> io::Result<usize> {
                if self.inner.is_empty() {
                    self.logger
                        .write(Level::Info, "inner", &[], 0, &mut self.inner);
                }
                self.outer.extend_from_slice(data);
                Ok(data.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let logger = logger(Style::Json, true);
        let mut sink = Nested {
            logger: &logger,
            inner: Vec::new(),
            outer: Vec::new(),
        };
        logger.write(Level::Info, "outer", &[], 0, &mut sink);
        assert_eq!(sink.outer, json_line("outer", &[], 0, None).as_bytes());
        assert_eq!(sink.inner, json_line("inner", &[], 0, None).as_bytes());
    }

    #[test]
    fn parse_level_and_format() {
        let levels = [
            ("", 1),
            ("  ", 1),
            ("info", 1),
            (" INFO ", 1),
            ("debug", 0),
            ("trace", -1),
            ("warn", 2),
            ("error", 3),
            ("fatal", 4),
            ("panic", 5),
            ("disabled", 7),
            ("nope", 1),
            ("6", 1),
            ("7", 7),
            ("-1", -1),
            ("8", 8),
            ("128", 1),
            ("-129", 1),
            ("nolevel", 1),
        ];
        for (raw, expect) in levels {
            assert_eq!(parse_level(raw), expect, "{raw:?}");
        }
        assert_eq!(parse_format(""), Style::Json);
        assert_eq!(parse_format("json"), Style::Json);
        assert_eq!(parse_format(" Console "), Style::Console);
        assert!(allows(1, 1));
        assert!(!allows(1, 0));
        assert!(!allows(DISABLED, 3));
        assert!(!allows(4, Level::Error as i8));
        assert!(allows(4, Level::Fatal as i8));
        assert!(!allows(8, Level::Error as i8));
    }

    #[test]
    fn a_filtered_level_does_not_write() {
        let mut sink = CountWriter::full();
        logger(Style::Json, true).write(Level::Debug, "hello", &[], 0, &mut sink);
        assert_eq!(sink.writes, 0);
        assert!(sink.buf.is_empty());
    }

    #[test]
    fn one_full_write_and_a_short_write_both_deliver_the_line() {
        let mut once = CountWriter::full();
        let expect = json_line("hello", &[], 0, None);
        logger(Style::Json, true).write(Level::Info, "hello", &[], 0, &mut once);
        assert_eq!(once.writes, 1);
        assert_eq!(once.buf, expect.as_bytes());

        let mut short = CountWriter::limited(3);
        logger(Style::Json, true).write(Level::Info, "hello", &[], 0, &mut short);
        assert!(short.writes > 1, "{}", short.writes);
        assert_eq!(short.buf, expect.as_bytes());
    }

    #[test]
    fn console_sorts_fields_and_colors_unless_disabled() {
        let fields = [
            Field {
                name: "z".to_owned(),
                value: Value::Int(1),
            },
            Field {
                name: "a".to_owned(),
                value: Value::Int(2),
            },
            Field {
                name: "error".to_owned(),
                value: Value::Str("e1".to_owned()),
            },
        ];
        let mut plain = logger(Style::Console, true);
        plain.session_id = Some("s1".to_owned());
        let text = line(
            &plain,
            Level::Info,
            "hello",
            &fields,
            1_700_000_000_000_000_000,
        );
        assert!(!text.starts_with('{'), "{text}");
        assert!(!text.as_bytes().contains(&0x1b), "{text}");
        assert!(text.contains("2023-11-14T22:13:20Z INF hello "), "{text}");
        let fields_at = text.find(" error=").expect("error field");
        let rest = &text[fields_at + 1..];
        assert!(
            rest.starts_with("error=e1 a=2 commit= component=strategy sdk_version="),
            "{rest}"
        );
        assert!(rest.contains(" session_id=s1 "), "{rest}");
        assert!(rest.contains(" version= z=1\n"), "{rest}");

        let colored = line(
            &logger(Style::Console, false),
            Level::Info,
            "hello",
            &[],
            1_700_000_000_000_000_000,
        );
        assert!(colored.contains("\u{1b}[90m"), "{colored}");
        assert!(colored.contains("\u{1b}[32mINF\u{1b}[0m"), "{colored}");
        assert!(colored.contains("\u{1b}[1mhello\u{1b}[0m"), "{colored}");
        assert!(
            colored.contains("\u{1b}[36mcomponent=\u{1b}[0m"),
            "{colored}"
        );

        let debug = line(
            &logger(Style::Console, false),
            Level::Debug,
            "hello",
            &[],
            0,
        );
        assert!(debug.is_empty(), "debug is below info");
        let mut debug_logger = logger(Style::Console, false);
        debug_logger.min = Level::Debug as i8;
        let debug = line(&debug_logger, Level::Debug, "hello", &[], 0);
        assert!(debug.contains("DBG hello"), "{debug}");
        assert!(!debug.contains("\u{1b}[1mhello"), "{debug}");
        assert!(!debug.contains("\u{1b}[0mDBG"), "{debug}");
    }

    #[test]
    fn console_quotes_like_strconv_and_prints_special_floats() {
        assert_eq!(console_text_str("hello"), "hello");
        assert_eq!(console_text_str("hello world"), "\"hello world\"");
        assert_eq!(console_text_str(""), "");
        assert_eq!(console_text_str("a\nb"), "\"a\\nb\"");
        assert_eq!(console_text_str("café"), "\"café\"");
        assert_eq!(console_text_str("\u{7f}"), "\"\\x7f\"");
        assert_eq!(float_body(f64::NAN), "NaN");
        assert_eq!(float_body(f64::INFINITY), "+Inf");
        assert_eq!(float_body(f64::NEG_INFINITY), "-Inf");
    }

    #[test]
    fn note_message_has_no_sdk_prefix() {
        let (_result, bytes) = capture(|| note("td buffer full"));
        let text = String::from_utf8(bytes).expect("utf-8");
        assert!(text.contains("\"message\":\"td buffer full\""), "{text}");
        assert!(!text.contains("mftik sdk:"), "{text}");
    }

    struct CountWriter {
        buf: Vec<u8>,
        writes: usize,
        limit: Option<usize>,
    }

    impl CountWriter {
        fn full() -> Self {
            Self {
                buf: Vec::new(),
                writes: 0,
                limit: None,
            }
        }

        fn limited(limit: usize) -> Self {
            Self {
                buf: Vec::new(),
                writes: 0,
                limit: Some(limit),
            }
        }
    }

    impl Write for CountWriter {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            let count = self
                .limit
                .map(|limit| data.len().min(limit))
                .unwrap_or(data.len());
            self.buf.extend_from_slice(&data[..count]);
            Ok(count)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
}
