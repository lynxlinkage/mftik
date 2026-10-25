//! Strict ticker parse, strike spelling, status sets, and the margin grouping.

use super::{
    Category, Filters, Info, MAX_UID, MIN_UID, PRICE_TICK, Status, UniversalTicker, spell_strike,
    valid_uid,
};

fn parse(text: &str) -> UniversalTicker {
    UniversalTicker::parse(text).unwrap_or_else(|err| panic!("parse {text:?}: {err}"))
}

fn refused(text: &str) -> String {
    match UniversalTicker::parse(text) {
        Ok(ticker) => panic!("{text:?} parsed as {ticker:?}"),
        Err(err) => err.to_string(),
    }
}

#[test]
fn parse_splits_the_three_parts() {
    let ticker = parse("Bybit_Perp_BTCUSDT");
    assert_eq!(ticker.venue, "Bybit");
    assert_eq!(ticker.category, Category::Perp);
    assert_eq!(ticker.symbol, "BTCUSDT");
    assert_eq!(ticker.to_string(), "Bybit_Perp_BTCUSDT");
    assert_ne!(ticker, parse("Bybit_Spot_BTCUSDT"));
}

#[test]
fn canonical_tickers_round_trip() {
    for text in [
        "Gate_Spot_BTCUSDT",
        "BinanceCM_Inverse_BTCUSD",
        "BinanceUM_Future_BTCUSDT-250926",
        "BinanceUM_Option_BTCUSDT-260905-100000-C",
        "Bybit_Option_AVAXUSDC-260905-6D4-P",
        "Okx_Option_BTCUSD-261225-0D25-C",
        "Gate_Spot_龙虾USDT",
        // Six digits, not a calendar date.
        "Bybit_Future_BTCUSDT-261399",
        // Venue case is not folded here; the registry spelling is not checked.
        "OKX_Spot_BTCUSDT",
        "Venue9_Spot_A1",
    ] {
        let ticker = parse(text);
        assert_eq!(ticker.to_string(), text);
        assert_eq!(parse(&ticker.to_string()), ticker);
    }
}

#[test]
fn parse_refuses_the_wrong_shape() {
    for text in [
        "",
        "Gate_Spot",
        "Gate_Spot_BTC_USDT",
        "Gate__BTCUSDT",
        "_Spot_BTCUSDT",
        "Gate_Spot_",
    ] {
        assert_eq!(
            refused(text),
            format!("symbol: invalid universal ticker: {text:?}")
        );
    }
}

#[test]
fn parse_refuses_a_venue_or_category_spelling() {
    assert_eq!(
        refused("gate_Spot_BTCUSDT"),
        "symbol: invalid universal ticker: venue \"gate\""
    );
    assert_eq!(
        refused("Ga-te_Spot_BTCUSDT"),
        "symbol: invalid universal ticker: venue \"Ga-te\""
    );
    assert_eq!(
        refused("Gate_spot_BTCUSDT"),
        "symbol: invalid universal ticker: category \"spot\""
    );
    assert_eq!(
        refused("Gate_Bogus_BTCUSDT"),
        "symbol: invalid universal ticker: category \"Bogus\""
    );
}

#[test]
fn parse_refuses_a_second_spelling_of_the_symbol() {
    for text in [
        "Gate_Spot_btcusdt",
        "Gate_Spot_BTC-USDT",
        // A date belongs on a dated book only.
        "Gate_Spot_BTCUSDT-250926",
        // The glued date must be hyphenated.
        "BinanceUM_Future_BTCUSDT250926",
        "BinanceUM_Future_BTC-USDT-250926",
        "Bybit_Option_AVAXUSDC-260905-6d4-C",
        "Bybit_Option_AVAXUSDC-260905-6D40-C",
        "Bybit_Option_AVAXUSDC-260905-6D0-C",
        "Bybit_Option_BTCUSDT-260905-06400-C",
        "Bybit_Option_BTCUSDT-260905-7E4-C",
        "Bybit_Option_btcusdt-260905-6400-C",
    ] {
        assert_eq!(
            refused(text),
            format!(
                "symbol: invalid universal ticker: symbol {:?} is not canonical",
                text.splitn(3, '_').nth(2).unwrap()
            ),
            "{text}"
        );
    }
}

#[test]
fn parse_names_the_forbidden_char() {
    assert!(refused("Bybit_Perp_BTC/USDT").contains("\"/\" cannot appear inside symbol"));
    assert!(refused("Bybit_Perp_BTC USDT").contains("\" \" cannot appear inside symbol"));
    assert!(refused(" Bybit_Perp_BTCUSDT").contains("venue"));
    assert!(refused("Bybit_Perp_BTCUSDT\t").contains("\"\\t\" cannot appear"));
    // A '.' in a strike points at the D spelling.
    let dot = refused("Bybit_Option_AVAXUSDC-260905-6.4-C");
    assert!(dot.contains("\".\" cannot appear inside symbol"), "{dot}");
    assert!(dot.contains("6D4"), "{dot}");
    let space = refused("Bybit_Option_BTCUSDT-260905-10 000-C");
    assert!(space.contains("\" \" cannot appear"), "{space}");
}

#[test]
fn strike_spelling_folds_one_value_to_one_text() {
    for (input, want) in [
        ("6400", "6400"),
        ("6400.0", "6400"),
        ("6400.", "6400"),
        ("06400", "6400"),
        ("6.4", "6D4"),
        ("6d4", "6D4"),
        ("6D4", "6D4"),
        ("6.40", "6D4"),
        (".5", "0D5"),
        ("0.50", "0D5"),
        ("7E4", "70000"),
        ("7e+4", "70000"),
        ("6.4E-1", "0D64"),
        ("64E-1", "6D4"),
        ("10_000", "10000"),
        ("1_0.2_5", "10D25"),
        ("+5", "5"),
        ("-5", "-5"),
        ("-0.5", "-0D5"),
        ("-0", "0"),
        ("0", "0"),
        (" 6400 ", "6400"),
    ] {
        assert_eq!(spell_strike(input).as_deref(), Some(want), "{input}");
    }
}

#[test]
fn strike_spelling_refuses_what_is_not_a_number() {
    for input in [
        "", " ", "_1", "1_", "1__0", "1._5", "6.4.1", "E4", "7E", "7E4E1", "7E1.5", "1/2",
        "10 000", "+", "-", "+-5", "abc", "７",
    ] {
        assert_eq!(spell_strike(input), None, "{input:?}");
    }
}

#[test]
fn strike_spelling_bounds_the_expansion() {
    let long = "1".repeat(10_001);
    assert_eq!(spell_strike(&long), None);
    assert_eq!(spell_strike("1E10001"), None);
    assert_eq!(spell_strike("1E-10001"), None);
    let wide = spell_strike("1E10000").expect("the bound itself is accepted");
    assert_eq!(wide.len(), 10_001);
    let narrow = spell_strike("1E-10000").expect("the bound itself is accepted");
    assert_eq!(narrow.len(), 10_002);
    assert!(narrow.starts_with("0D") && narrow.ends_with('1'));
    // The exponent and the fraction together may not pass the bound.
    let frac = format!("0.{}1E-1", "0".repeat(9_999));
    assert_eq!(spell_strike(&frac), None);
}

#[test]
fn uid_range_excludes_zero() {
    assert!(!valid_uid(0));
    assert!(valid_uid(MIN_UID));
    assert!(valid_uid(MAX_UID));
}

#[test]
fn category_spelling_is_exact() {
    for category in Category::ALL {
        assert_eq!(Category::from_name(category.as_str()), Some(category));
        assert_eq!(category.to_string(), category.as_str());
    }
    assert_eq!(Category::from_name("spot"), None);
    assert_eq!(Category::from_name("Futures"), None);
}

#[test]
fn status_sets() {
    let all = [
        Status::Unspecified,
        Status::Prelisting,
        Status::Tradable,
        Status::Halted,
        Status::Settling,
        Status::Expired,
        Status::Delisted,
        Status::Unknown,
    ];
    for status in all {
        assert_eq!(
            status.is_tradable(),
            status == Status::Tradable,
            "{status:?}"
        );
        assert_eq!(status.valid(), status != Status::Unspecified, "{status:?}");
        assert!(!(status.active() && status.terminal()), "{status:?}");
        assert_eq!(
            status.active() || status.terminal(),
            status.valid(),
            "{status:?}"
        );
    }
    assert!(Status::Expired.terminal() && Status::Delisted.terminal());
    assert!(Status::Unknown.active() && !Status::Unknown.is_tradable());
    assert_eq!(Status::Delisted.as_str(), "DELISTED");
    assert_eq!(Status::Unspecified.as_str(), "");
    assert_eq!(Status::default(), Status::Unspecified);
}

fn row(base: &str, quote: &str, value: &str, price: &str, settle: &str) -> Info {
    Info {
        uid: 1,
        ticker: parse("Bybit_Perp_BTCUSDT"),
        exch_ticker: "BTCUSDT".to_owned(),
        base: base.to_owned(),
        quote: quote.to_owned(),
        contract_size: 1.0,
        contract_value_ccy: value.to_owned(),
        price_ccy: price.to_owned(),
        settle: settle.to_owned(),
        expiry_ns: 0,
        strike: 0.0,
        option_type: String::new(),
        status: Status::Tradable,
        filters: Filters::new(),
        updated_at_ns: 0,
    }
}

#[test]
fn margin_grouping_follows_the_currency_table() {
    // linear, inverse, U-margined option, coin-margined option, spot, quanto.
    let linear = row("BTC", "USDT", "BTC", "USDT", "USDT");
    assert!(linear.linear() && linear.u_margined());
    assert!(!linear.inverse() && !linear.coin_margined() && !linear.quanto());

    let inverse = row("BTC", "USD", "USD", "USD", "BTC");
    assert!(inverse.inverse() && inverse.coin_margined());
    assert!(!inverse.linear() && !inverse.u_margined() && !inverse.quanto());

    let u_option = row("BTC", "USDT", "BTC", "USDT", "USDT");
    assert!(u_option.linear() && u_option.u_margined());

    let coin_option = row("BTC", "USD", "BTC", "BTC", "BTC");
    assert!(coin_option.coin_margined());
    assert!(!coin_option.linear() && !coin_option.inverse());

    let spot = row("BTC", "USDT", "", "", "");
    assert!(!spot.coin_margined() && !spot.u_margined() && !spot.quanto());
    assert!(!spot.linear() && !spot.inverse());

    let quanto = row("BTC", "USD", "BTC", "USD", "USDC");
    assert!(quanto.quanto());
    assert!(!quanto.u_margined() && !quanto.coin_margined() && !quanto.linear());
}

#[test]
fn price_tick_reads_the_filter() {
    let mut info = row("BTC", "USDT", "BTC", "USDT", "USDT");
    assert_eq!(info.price_tick(), 0.0);
    info.filters.insert(PRICE_TICK.to_owned(), f64::NAN);
    assert!(info.price_tick().is_nan());
    info.filters.insert(PRICE_TICK.to_owned(), 0.5);
    assert_eq!(info.price_tick(), 0.5);
    assert!(info.is_tradable());
    info.status = Status::Halted;
    assert!(!info.is_tradable());
}
