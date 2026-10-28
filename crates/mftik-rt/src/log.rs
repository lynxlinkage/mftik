//! Stderr log lines (E06-D004–E06-D010).
//!
//! One JSON object per line, or zerolog's console layout. The keys and their
//! order match Go zerolog, with `sdk_version` added. This module does not use
//! the `log` or `tracing` crates.

use std::cell::RefCell;
use std::io::{self, Write};
use std::sync::{OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// `component` on every strategy line (E06-D008).
pub const COMPONENT: &str = "strategy";

/// This crate's package version and full git commit (E06-D008).
pub const SDK_VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "+", env!("VERGEN_GIT_SHA"));

/// zerolog's `Disabled`. A logger at this minimum emits nothing.
pub const DISABLED: i8 = 7;

const HEX: &[u8; 16] = b"0123456789abcdef";

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
        let line = render(
            &Record {
                level,
                now_ns,
                component: &self.component,
                version: &self.version,
                commit: &self.commit,
                sdk_version: &self.sdk_version,
                session_id: self.session_id.as_deref(),
                fields,
                message,
            },
            self.style,
            self.no_color,
        );
        write_line(out, &line);
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
    *guard = id.to_owned();
}

/// `plane.note`: info, the text as the message, `session_id` only when set.
pub(crate) fn note(line: &str) {
    let session = {
        let guard = note_session().read().unwrap_or_else(|err| err.into_inner());
        if guard.is_empty() {
            None
        } else {
            Some(guard.clone())
        }
    };
    emit(Level::Info, line, &[], session.as_deref());
}

/// One line on stderr, or into the buffer installed by [`capture`].
pub fn emit(level: Level, message: &str, fields: &[Field], session_id: Option<&str>) {
    let cfg = config();
    if !allows(cfg.min, level as i8) {
        return;
    }
    let guard = identity().read().unwrap_or_else(|err| err.into_inner());
    let line = render(
        &Record {
            level,
            now_ns: now_ns(),
            component: COMPONENT,
            version: &guard.version,
            commit: &guard.commit,
            sdk_version: SDK_VERSION,
            session_id,
            fields,
            message,
        },
        cfg.style,
        cfg.no_color,
    );
    drop(guard);
    write_out(&line);
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
}

struct Identity {
    version: String,
    commit: String,
}

fn identity() -> &'static RwLock<Identity> {
    static IDENTITY: RwLock<Identity> = RwLock::new(Identity {
        version: String::new(),
        commit: String::new(),
    });
    &IDENTITY
}

fn note_session() -> &'static RwLock<String> {
    static SESSION: RwLock<String> = RwLock::new(String::new());
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

fn render(record: &Record<'_>, style: Style, no_color: bool) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
    match style {
        Style::Json => render_json(record, &mut buf),
        Style::Console => render_console(record, no_color, &mut buf),
    }
    buf.push(b'\n');
    buf
}

fn render_json(record: &Record<'_>, buf: &mut Vec<u8>) {
    buf.push(b'{');
    append_key(buf, "level", true);
    append_json_str(buf, level_name(record.level));
    append_key(buf, "time", false);
    append_json_str(buf, &format_utc_nano(record.now_ns));
    append_key(buf, "component", false);
    append_json_str(buf, record.component);
    append_key(buf, "version", false);
    append_json_str(buf, record.version);
    append_key(buf, "commit", false);
    append_json_str(buf, record.commit);
    append_key(buf, "sdk_version", false);
    append_json_str(buf, record.sdk_version);
    if let Some(session) = record.session_id {
        append_key(buf, "session_id", false);
        append_json_str(buf, session);
    }
    for field in record.fields {
        append_key(buf, &field.name, false);
        append_value(buf, &field.value);
    }
    append_key(buf, "message", false);
    append_json_str(buf, record.message);
    buf.push(b'}');
}

fn append_key(buf: &mut Vec<u8>, name: &str, first: bool) {
    if !first {
        buf.push(b',');
    }
    append_json_str(buf, name);
    buf.push(b':');
}

fn append_value(buf: &mut Vec<u8>, value: &Value) {
    match value {
        Value::Str(text) => append_json_str(buf, text),
        Value::Int(number) => buf.extend_from_slice(number.to_string().as_bytes()),
        Value::Big(text) => buf.extend_from_slice(text.as_bytes()),
        Value::Float(number) => append_float(buf, *number),
        Value::Bool(true) => buf.extend_from_slice(b"true"),
        Value::Bool(false) => buf.extend_from_slice(b"false"),
        Value::Null => buf.extend_from_slice(b"null"),
    }
}

fn level_name(level: Level) -> &'static str {
    match level {
        Level::Trace => "trace",
        Level::Debug => "debug",
        Level::Info => "info",
        Level::Warn => "warn",
        Level::Error => "error",
        Level::Fatal => "fatal",
        Level::Panic => "panic",
    }
}

/// zerolog's string encoder: UTF-8 passes through, controls are `\u00XX`.
fn append_json_str(buf: &mut Vec<u8>, text: &str) {
    buf.push(b'"');
    let bytes = text.as_bytes();
    let mut index = 0;
    let mut start = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        if (0x20..0x7f).contains(&byte) && byte != b'"' && byte != b'\\' {
            index += 1;
            continue;
        }
        if byte >= 0x80 {
            let ch = text[index..].chars().next().expect("utf-8");
            index += ch.len_utf8();
            continue;
        }
        if start < index {
            buf.extend_from_slice(&bytes[start..index]);
        }
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
        index += 1;
        start = index;
    }
    if start < bytes.len() {
        buf.extend_from_slice(&bytes[start..]);
    }
    buf.push(b'"');
}

/// zerolog `appendFloat`: `strconv.AppendFloat` prec -1, `'f'` unless the
/// magnitude is outside `[1e-6, 1e21)`, then `'e'` with a `+` on the exponent.
fn append_float(buf: &mut Vec<u8>, value: f64) {
    if value.is_nan() {
        buf.extend_from_slice(br#""NaN""#);
        return;
    }
    if value.is_infinite() {
        buf.extend_from_slice(if value.is_sign_positive() {
            br#""+Inf""#
        } else {
            br#""-Inf""#
        });
        return;
    }
    buf.extend_from_slice(float_body(value).as_bytes());
}

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

fn format_utc_nano(unix_ns: i64) -> String {
    let secs = unix_ns.div_euclid(1_000_000_000);
    let nanos = unix_ns.rem_euclid(1_000_000_000) as u32;
    let (year, month, day, hour, min, sec) = civil_hms(secs);
    if nanos == 0 {
        format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
    } else {
        format!(
            "{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}.{}Z",
            trim_nanos(nanos)
        )
    }
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

fn trim_nanos(nanos: u32) -> String {
    let mut text = format!("{nanos:09}");
    while text.ends_with('0') {
        text.pop();
    }
    text
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
        ];
        let message = "a\"b\\c\n\u{7f}<café>";
        let text = json_line(message, &fields, 0, None);
        assert!(
            text.contains(r#""on":true,"off":false,"missing":null,"wide":18446744073709551616"#),
            "{text}"
        );
        assert!(
            text.contains(r#""message":"a\"b\\c\n\u007f<café>""#),
            "{text}"
        );
    }

    #[test]
    fn times_match_rfc3339_nano() {
        assert_eq!(format_utc_nano(0), "1970-01-01T00:00:00Z");
        assert_eq!(
            format_utc_nano(1_700_000_000_123_456_789),
            "2023-11-14T22:13:20.123456789Z"
        );
        assert_eq!(
            format_utc_nano(1_700_000_000_100_000_000),
            "2023-11-14T22:13:20.1Z"
        );
    }

    #[test]
    fn floats_match_zerolog_append_float() {
        let cases = [
            (0.0, "0"),
            (-0.0, "-0"),
            (1.0, "1"),
            (1.5, "1.5"),
            (1e-6, "0.000001"),
            (1e-7, "1e-7"),
            (9.9e-7, "9.9e-7"),
            (1e20, "100000000000000000000"),
            (1e21, "1e+21"),
            (1.23e21, "1.23e+21"),
            (1e-10, "1e-10"),
            (std::f64::consts::PI, "3.141592653589793"),
            (2.5e-7, "2.5e-7"),
            (123_456_789.0, "123456789"),
            (0.1, "0.1"),
            (0.3, "0.3"),
            (f64::MIN_POSITIVE, "2.2250738585072014e-308"),
            (f64::MAX, "1.7976931348623157e+308"),
            (5e-7, "5e-7"),
            (-1e21, "-1e+21"),
            (f64::NAN, "\"NaN\""),
            (f64::INFINITY, "\"+Inf\""),
            (f64::NEG_INFINITY, "\"-Inf\""),
        ];
        for (value, expect) in cases {
            let mut buf = Vec::new();
            append_float(&mut buf, value);
            let text = String::from_utf8(buf).expect("utf-8");
            assert_eq!(text, expect, "{value:?}");
        }
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
