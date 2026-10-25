//! Instrument identity and the SYM row (mftiko `contract/symbol`, PROTOCOL §7.3).
//!
//! [`Info`] is one instrument as SYM serves it. A strategy keeps the UID and
//! reads the current row with [`crate::symclient::Client::info`] (E08-D004,
//! E08-D025). [`UniversalTicker`] is the human-facing name. It appears only
//! where a strategy resolves its own config.
//!
//! [`UniversalTicker::parse`] is the strict parser: it consults no registry and
//! rejects any spelling it would have had to change (E02-D003). The lenient
//! spec parser, listings, and the asset alias table stay in Go. The SDK does
//! not use them.

use std::collections::BTreeMap;
use std::fmt;

#[cfg(test)]
mod tests;

/// Joins venue, category, and symbol. None of the three may contain it.
pub const SEPARATOR: char = '_';

/// How a fractional strike writes its decimal point: 6.4 is `6D4`.
pub const STRIKE_DECIMAL: char = 'D';

/// Lowest valid UID. Zero is invalid (PROTOCOL §10 P13).
pub const MIN_UID: u32 = 1;

/// Highest valid UID, `2^32 - 1`.
pub const MAX_UID: u32 = u32::MAX;

/// Tick size, in `price_ccy`. [`crate::bookpath::Path::resolve`] takes it.
pub const PRICE_TICK: &str = "price_tick";
/// Quantity step, in `contract_value_ccy`.
pub const QTY_STEP: &str = "qty_step";
pub const MIN_QTY: &str = "min_qty";
pub const MAX_QTY: &str = "max_qty";
pub const MIN_NOTIONAL: &str = "min_notional";
pub const MAX_NOTIONAL: &str = "max_notional";
pub const MIN_PRICE: &str = "min_price";
pub const MAX_PRICE: &str = "max_price";

/// Longest strike literal or exponent [`UniversalTicker::parse`] expands. A
/// guard against hostile input, not a product limit.
const SPELL_DIGITS: usize = 10_000;

/// Whether `uid` may identify an instrument: `MIN_UID..=MAX_UID`.
pub fn valid_uid(uid: u32) -> bool {
    uid >= MIN_UID
}

/// Market inside a venue. The spelling goes into the ticker as is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Category {
    Spot,
    Perp,
    /// Coin-margined, a product of its own: `BinanceCM_Inverse_BTCUSD`.
    Inverse,
    Future,
    Option,
}

impl Category {
    pub const ALL: [Self; 5] = [
        Self::Spot,
        Self::Perp,
        Self::Inverse,
        Self::Future,
        Self::Option,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "Spot",
            Self::Perp => "Perp",
            Self::Inverse => "Inverse",
            Self::Future => "Future",
            Self::Option => "Option",
        }
    }

    /// The exact spelling. Case is not folded.
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|category| category.as_str() == name)
    }

    /// Dated contracts, whose symbol keeps `-` between fields.
    fn structured(self) -> bool {
        matches!(self, Self::Future | Self::Option)
    }
}

impl fmt::Display for Category {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Trading state (E02-D003). The set is still provisional. Only
/// [`Status::Tradable`] may trade.
///
/// `Prelisting`, `Tradable`, `Halted`, `Settling`, and `Unknown` stay on the
/// active table. `Expired` and `Delisted` are the terminal table; a resolved
/// row with one of them is the inactive mark of E08-D026.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum Status {
    /// Proto zero. Invalid, not stored, not tradable.
    #[default]
    Unspecified,
    Prelisting,
    Tradable,
    Halted,
    Settling,
    Expired,
    Delisted,
    /// A native venue state with no mapping. Stored, not tradable.
    Unknown,
}

impl Status {
    /// The TEXT the symbol tables check. Empty for [`Status::Unspecified`].
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Unspecified => "",
            Self::Prelisting => "PRELISTING",
            Self::Tradable => "TRADABLE",
            Self::Halted => "HALTED",
            Self::Settling => "SETTLING",
            Self::Expired => "EXPIRED",
            Self::Delisted => "DELISTED",
            Self::Unknown => "UNKNOWN",
        }
    }

    /// Whether the status may be stored. Only [`Status::Unspecified`] may not.
    pub fn valid(self) -> bool {
        self != Self::Unspecified
    }

    pub fn is_tradable(self) -> bool {
        self == Self::Tradable
    }

    /// On the active table.
    pub fn active(self) -> bool {
        matches!(
            self,
            Self::Prelisting | Self::Tradable | Self::Halted | Self::Settling | Self::Unknown
        )
    }

    /// On the terminal table. Such a row no longer changes.
    pub fn terminal(self) -> bool {
        matches!(self, Self::Expired | Self::Delisted)
    }
}

impl fmt::Display for Status {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// `<Venue>_<Category>_<SYMBOL>`, for example `Bybit_Perp_BTCUSDT`,
/// `BinanceUM_Future_BTCUSDT-250926`, or `Okx_Option_BTCUSD-261225-6D4-C`.
///
/// `Display` is the canonical spelling, the only form that goes on the wire.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UniversalTicker {
    pub venue: String,
    pub category: Category,
    pub symbol: String,
}

/// The text is not a canonical [`UniversalTicker`]. The message names the part.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TickerError(String);

impl TickerError {
    fn new(detail: impl fmt::Display) -> Self {
        Self(format!("symbol: invalid universal ticker: {detail}"))
    }
}

impl fmt::Display for TickerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for TickerError {}

impl UniversalTicker {
    /// Strict parse. Venue case, pair punctuation, a glued date, and a strike
    /// such as `6.4` or `6400.0` are all refused, because each would be a
    /// second spelling of one instrument.
    pub fn parse(text: &str) -> Result<Self, TickerError> {
        let parts: Vec<&str> = text.split(SEPARATOR).collect();
        if parts.len() != 3 || parts.iter().any(|part| part.is_empty()) {
            return Err(TickerError::new(format_args!("{text:?}")));
        }
        let (venue, category, symbol) = (parts[0], parts[1], parts[2]);
        if !canonical_venue(venue) {
            return Err(TickerError::new(format_args!("venue {venue:?}")));
        }
        let Some(category) = Category::from_name(category) else {
            return Err(TickerError::new(format_args!("category {category:?}")));
        };
        check_symbol(symbol, category)?;
        Ok(Self {
            venue: venue.to_owned(),
            category,
            symbol: symbol.to_owned(),
        })
    }
}

impl fmt::Display for UniversalTicker {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}{SEPARATOR}{}{SEPARATOR}{}",
            self.venue, self.category, self.symbol
        )
    }
}

/// Restrictions by name. A NaN bound is a restriction the venue publishes
/// with no limit, which is not the same as a missing name (E00-D008).
pub type Filters = BTreeMap<String, f64>;

/// One instrument as SYM serves it (`sym.v1.SymbolInfo`, E02-D003).
///
/// Unset optional numbers are zero and unset strings are empty, as in Go.
/// Derived equality compares NaN filter bounds as unequal.
#[derive(Clone, Debug, PartialEq)]
pub struct Info {
    pub uid: u32,
    pub ticker: UniversalTicker,
    pub exch_ticker: String,
    pub base: String,
    pub quote: String,
    /// One contract's face value. Zero on spot.
    pub contract_size: f64,
    /// Unit of qty: base on linear and options, quote on inverse.
    pub contract_value_ccy: String,
    /// Unit of price, `price_tick`, and premium.
    pub price_ccy: String,
    /// PnL settlement asset, not the margin asset. Empty on spot.
    pub settle: String,
    /// Zero when the instrument does not expire.
    pub expiry_ns: i64,
    /// Zero unless an option.
    pub strike: f64,
    /// `C`, `P`, or empty.
    pub option_type: String,
    pub status: Status,
    pub filters: Filters,
    pub updated_at_ns: i64,
}

impl Info {
    /// Only [`Status::Tradable`] may trade.
    pub fn is_tradable(&self) -> bool {
        self.status.is_tradable()
    }

    /// The `price_tick` filter, the value [`crate::bookpath::Path::resolve`]
    /// takes. Zero when SYM sent no such filter and NaN when it sent one with
    /// no bound, the same as Go's `Filters[PriceTick]`. bookpath refuses both.
    pub fn price_tick(&self) -> f64 {
        self.filters.get(PRICE_TICK).copied().unwrap_or(0.0)
    }

    /// Display grouping from the currency fields (E02-D003): contract value
    /// base, price quote, settlement quote. Category is not read.
    pub fn linear(&self) -> bool {
        self.margin().linear
    }

    /// Contract value quote, price quote, settlement base.
    pub fn inverse(&self) -> bool {
        self.margin().inverse
    }

    /// Settlement is neither base nor quote. Not stored in phase 1.
    pub fn quanto(&self) -> bool {
        self.margin().quanto
    }

    /// 幣本位: settlement equals base. Spot is false.
    pub fn coin_margined(&self) -> bool {
        self.margin().coin
    }

    /// U 本位: settlement is set, is not base, and the row is not quanto.
    pub fn u_margined(&self) -> bool {
        self.margin().u
    }

    fn margin(&self) -> Margin {
        classify(
            &self.base,
            &self.quote,
            &self.contract_value_ccy,
            &self.price_ccy,
            &self.settle,
        )
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct Margin {
    linear: bool,
    inverse: bool,
    quanto: bool,
    coin: bool,
    u: bool,
}

fn classify(base: &str, quote: &str, value_ccy: &str, price_ccy: &str, settle: &str) -> Margin {
    let mut margin = Margin::default();
    if !settle.is_empty() && settle != base && settle != quote {
        margin.quanto = true;
        return margin;
    }
    if !settle.is_empty() && settle == base {
        margin.coin = true;
    } else if !settle.is_empty() {
        margin.u = true;
    }
    if !base.is_empty()
        && !quote.is_empty()
        && settle == quote
        && value_ccy == base
        && price_ccy == quote
    {
        margin.linear = true;
    }
    if !base.is_empty()
        && !quote.is_empty()
        && base != quote
        && settle == base
        && value_ccy == quote
        && price_ccy == quote
    {
        margin.inverse = true;
    }
    margin
}

/// CamelCase ASCII with no separator. `OKX` passes; the registry spelling
/// `Okx` is not checked here.
fn canonical_venue(name: &str) -> bool {
    let bytes = name.as_bytes();
    let Some((first, rest)) = bytes.split_first() else {
        return false;
    };
    first.is_ascii_uppercase() && rest.iter().all(u8::is_ascii_alphanumeric)
}

fn check_symbol(symbol: &str, category: Category) -> Result<(), TickerError> {
    if symbol.is_empty() {
        return Err(TickerError::new("empty symbol"));
    }
    if let Some(bad) = symbol
        .chars()
        .find(|c| matches!(c, '/' | '_' | '.') || c.is_whitespace())
    {
        let why = if bad == '.' {
            format!(
                "a fractional strike spells its decimal point \"{STRIKE_DECIMAL}\" (6.4 is \"6{STRIKE_DECIMAL}4\"), because '.' separates a feed key's topic from its ticker"
            )
        } else {
            format!(
                "\"{SEPARATOR}\" separates the ticker's three parts, and '-' separates a dated or option symbol's fields"
            )
        };
        return Err(TickerError::new(format_args!(
            "{:?} cannot appear inside symbol {symbol:?}; {why}",
            bad.to_string()
        )));
    }
    if normalize_symbol(symbol, category) != symbol {
        return Err(TickerError::new(format_args!(
            "symbol {symbol:?} is not canonical"
        )));
    }
    Ok(())
}

/// The platform spelling of `symbol` on `category`'s book. Parse accepts a
/// symbol only when this leaves it unchanged.
fn normalize_symbol(symbol: &str, category: Category) -> String {
    let raw = upper(symbol.trim());
    if raw.is_empty() {
        return String::new();
    }
    if !category.structured() {
        return canonical_pair(&raw);
    }
    let parts: Vec<&str> = raw.split('-').collect();
    let n = parts.len();
    if option_parts(&parts) {
        let pair = canonical_pair(&parts[..n - 3].join("-"));
        let date = parts[n - 3];
        let strike = parts[n - 2];
        let opt = parts[n - 1];
        if !pair.is_empty() {
            // A strike that cannot be folded stays verbatim.
            let strike = spell_strike(strike).unwrap_or_else(|| strike.to_owned());
            return format!("{pair}-{date}-{strike}-{opt}");
        }
    }
    if n >= 2 && date_code(parts[n - 1]) {
        let pair = canonical_pair(&parts[..n - 1].join("-"));
        if !pair.is_empty() {
            return format!("{pair}-{}", parts[n - 1]);
        }
    }
    let compact = canonical_pair(&raw);
    let bytes = compact.as_bytes();
    if bytes.len() > 6 && bytes[bytes.len() - 6..].iter().all(u8::is_ascii_digit) {
        // The last six bytes are ASCII, so the cut is on a char boundary.
        let (pair, date) = compact.split_at(compact.len() - 6);
        return format!("{pair}-{date}");
    }
    compact
}

fn option_parts(parts: &[&str]) -> bool {
    let n = parts.len();
    n >= 4
        && matches!(parts[n - 1], "C" | "P")
        && date_code(parts[n - 3])
        && !parts[n - 2].is_empty()
}

/// YYMMDD as a venue writes it: six ASCII digits, not a calendar check.
fn date_code(text: &str) -> bool {
    text.len() == 6 && text.bytes().all(|b| b.is_ascii_digit())
}

/// Drops pair punctuation and uppercases. It does not split base from quote.
fn canonical_pair(symbol: &str) -> String {
    let kept: String = symbol
        .trim()
        .chars()
        .filter(|&c| !matches!(c, '/' | '-' | '_') && !c.is_whitespace())
        .collect();
    upper(&kept)
}

/// Uppercase one char at a time, like Go's `strings.ToUpper`. Go uses the
/// simple case mapping and std has only the full one, so a char whose full
/// uppercase is several chars stays as it is. That matches Go for `ß` and the
/// ligatures; a few Greek letters with ypogegrammeni differ.
fn upper(text: &str) -> String {
    text.chars()
        .map(|c| {
            let mut mapped = c.to_uppercase();
            match (mapped.next(), mapped.next()) {
                (Some(one), None) => one,
                _ => c,
            }
        })
        .collect()
}

/// Strike in platform form (E02-D003): 6400.0 and 6400 are `6400`, and 6.4,
/// `6d4`, and `6D4` are `6D4`. `7E4` and `10_000` fold too. `None` is a
/// strike that does not fold.
fn spell_strike(value: &str) -> Option<String> {
    let raw = value.trim();
    if raw.is_empty() {
        return None;
    }
    let folded = strip_numeric_underscores(&upper(raw))?;
    let folded = folded.replace(STRIKE_DECIMAL, ".");
    let (negative, digits, scale) = parse_decimal(&folded)?;
    Some(format_strike(negative, &digits, scale))
}

/// Removes `_` between two digits. Any other `_` refuses the literal.
fn strip_numeric_underscores(text: &str) -> Option<String> {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut prev_digit = false;
    for (index, c) in text.char_indices() {
        if c == '_' {
            let next_digit = bytes.get(index + 1).is_some_and(u8::is_ascii_digit);
            if !prev_digit || !next_digit {
                return None;
            }
            continue;
        }
        out.push(c);
        prev_digit = c.is_ascii_digit();
    }
    Some(out)
}

/// A base-10 literal with an optional exponent, exactly: the value is
/// `digits * 10^scale`. `digits` has no leading zero, and is `0` for zero.
fn parse_decimal(text: &str) -> Option<(bool, String, i64)> {
    let (negative, text) = match text.as_bytes().first()? {
        b'+' => (false, &text[1..]),
        b'-' => (true, &text[1..]),
        _ => (false, text),
    };
    if text.is_empty() {
        return None;
    }
    let (mantissa, exponent) = match text.split_once('E') {
        Some((mantissa, exponent)) => {
            let exponent: i64 = exponent.parse().ok()?;
            if exponent.unsigned_abs() > SPELL_DIGITS as u64 {
                return None;
            }
            (mantissa, exponent)
        }
        None => (text, 0),
    };
    if mantissa.is_empty() {
        return None;
    }
    let (whole, frac) = match mantissa.split_once('.') {
        Some((whole, frac)) => {
            if frac.contains('.') {
                return None;
            }
            (whole, frac)
        }
        None => (mantissa, ""),
    };
    let whole = if whole.is_empty() { "0" } else { whole };
    let digits_only = |part: &str| part.bytes().all(|b| b.is_ascii_digit());
    if !digits_only(whole) || !digits_only(frac) {
        return None;
    }
    if whole.len() > SPELL_DIGITS || frac.len() > SPELL_DIGITS {
        return None;
    }
    let joined = format!("{whole}{frac}");
    let digits = joined.trim_start_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    let scale = exponent - i64::try_from(frac.len()).ok()?;
    if scale.unsigned_abs() > SPELL_DIGITS as u64 {
        return None;
    }
    Some((negative, digits.to_owned(), scale))
}

fn format_strike(negative: bool, digits: &str, scale: i64) -> String {
    if digits == "0" {
        return "0".to_owned();
    }
    let sign = if negative { "-" } else { "" };
    let shift = usize::try_from(scale.unsigned_abs()).unwrap_or(usize::MAX);
    if scale >= 0 {
        return format!("{sign}{digits}{}", "0".repeat(shift));
    }
    let padded = if digits.len() <= shift {
        format!("{}{digits}", "0".repeat(shift - digits.len() + 1))
    } else {
        digits.to_owned()
    };
    let (int_part, frac) = padded.split_at(padded.len() - shift);
    let frac = frac.trim_end_matches('0');
    if frac.is_empty() {
        return format!("{sign}{int_part}");
    }
    format!("{sign}{int_part}{STRIKE_DECIMAL}{frac}")
}
