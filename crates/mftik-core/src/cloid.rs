//! `client_order_id` layout `ver(4) | algo(10) | ts_ms(40) | seq(10)`.
//!
//! The id on the wire is the decimal of that `u64`, with no sign and no
//! leading zero. `ver` stays in 1..=7 so the top bit is clear.
//! Milliseconds are counted from [`EPOCH_MS`] (2026-01-01T00:00:00Z).
//! This is PROTOCOL §2 and the Go package `contract/wire/cloid`.

use std::fmt;

/// Bits of the version field, from the top of the `u64`.
pub const VER_BITS: u32 = 4;
/// Bits of the algo field.
pub const ALGO_BITS: u32 = 10;
/// Bits of the millisecond offset from [`EPOCH_MS`].
pub const TS_BITS: u32 = 40;
/// Bits of the per-millisecond sequence.
pub const SEQ_BITS: u32 = 10;

/// Version this SDK writes. Readers still accept 1..=[`VER_MAX`].
pub const VERSION: u8 = 1;
/// Highest version that keeps the top bit clear.
pub const VER_MAX: u8 = 7;

/// `2026-01-01T00:00:00Z` in Unix milliseconds. Forty bits of milliseconds
/// from here run through 2060-11.
pub const EPOCH_MS: i64 = 1_767_225_600_000;

/// Lowest algo id a platform id may carry. Zero is reserved.
pub const ALGO_MIN: u32 = 1;
/// Highest algo id that fits in [`ALGO_BITS`].
pub const ALGO_MAX: u32 = (1 << ALGO_BITS) - 1;
/// Last sequence number in one millisecond.
pub const SEQ_MAX: u32 = (1 << SEQ_BITS) - 1;

/// Length of the 40-bit millisecond window.
pub const TS_SPAN: i64 = 1_i64 << TS_BITS;

/// Longest decimal spelling. The top bit is clear, so the value is below
/// `2^63` and has at most 19 digits.
pub const MAX_LEN: usize = 19;

const SEQ_SHIFT: u32 = 0;
const TS_SHIFT: u32 = SEQ_BITS;
const ALGO_SHIFT: u32 = SEQ_BITS + TS_BITS;
const VER_SHIFT: u32 = SEQ_BITS + TS_BITS + ALGO_BITS;

/// One decoded platform `client_order_id`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Id {
    pub ver: u8,
    pub algo: u32,
    /// Unix milliseconds.
    pub ts_ms: i64,
    pub seq: u32,
}

impl Id {
    /// Decimal spelling `Encode` produces. No sign and no leading zero.
    pub fn decimal(self) -> String {
        let offset = self.ts_ms.wrapping_sub(EPOCH_MS) as u64;
        let packed = (u64::from(self.ver) << VER_SHIFT)
            | (u64::from(self.algo) << ALGO_SHIFT)
            | (offset << TS_SHIFT)
            | (u64::from(self.seq) << SEQ_SHIFT);
        packed.to_string()
    }

    fn check(self) -> Result<(), CloidError> {
        if !(1..=VER_MAX).contains(&self.ver) {
            return Err(CloidError::Version { ver: self.ver });
        }
        if !(ALGO_MIN..=ALGO_MAX).contains(&self.algo) {
            return Err(CloidError::Algo { algo: self.algo });
        }
        if self.ts_ms < EPOCH_MS || self.ts_ms.wrapping_sub(EPOCH_MS) >= TS_SPAN {
            return Err(CloidError::Ts { ts_ms: self.ts_ms });
        }
        if self.seq > SEQ_MAX {
            return Err(CloidError::Seq { seq: self.seq });
        }
        Ok(())
    }
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.decimal())
    }
}

/// Why a value is not a platform `client_order_id`, or why it does not fit
/// the layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CloidError {
    Version {
        ver: u8,
    },
    Algo {
        algo: u32,
    },
    Ts {
        ts_ms: i64,
    },
    Seq {
        seq: u32,
    },
    /// The string is not an id this platform numbered. A hand-placed order
    /// or another system's id looks like this; it belongs to no algo.
    NotPlatform,
}

impl fmt::Display for CloidError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CloidError::Version { ver } => {
                write!(f, "cloid: version {ver} outside 1..{VER_MAX}")
            }
            CloidError::Algo { algo } => {
                write!(f, "cloid: algo {algo} outside {ALGO_MIN}..{ALGO_MAX}")
            }
            CloidError::Ts { ts_ms } => {
                write!(
                    f,
                    "cloid: ts_ms {ts_ms} outside the 40-bit window from {EPOCH_MS}"
                )
            }
            CloidError::Seq { seq } => write!(f, "cloid: seq {seq} above {SEQ_MAX}"),
            CloidError::NotPlatform => f.write_str("cloid: not a platform client_order_id"),
        }
    }
}

impl std::error::Error for CloidError {}

/// Decimal `client_order_id` of `algo` at `ts_ms` with `seq`, version [`VERSION`].
pub fn encode(algo: u32, ts_ms: i64, seq: u32) -> Result<String, CloidError> {
    let id = Id {
        ver: VERSION,
        algo,
        ts_ms,
        seq,
    };
    id.check()?;
    Ok(id.decimal())
}

/// Read a platform id. Only the spelling [`encode`] produces is accepted:
/// decimal digits, no sign, no leading zero, version 1..=7, algo not zero.
/// Anything else is [`CloidError::NotPlatform`].
pub fn decode(s: &str) -> Result<Id, CloidError> {
    let bytes = s.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_LEN || bytes[0] == b'0' {
        return Err(CloidError::NotPlatform);
    }
    if !bytes.iter().all(|b| b.is_ascii_digit()) {
        return Err(CloidError::NotPlatform);
    }
    let packed: u64 = match s.parse() {
        Ok(v) => v,
        Err(_) => return Err(CloidError::NotPlatform),
    };
    let id = Id {
        ver: (packed >> VER_SHIFT) as u8,
        algo: ((packed >> ALGO_SHIFT) & u64::from(ALGO_MAX)) as u32,
        ts_ms: ((packed >> TS_SHIFT) & (TS_SPAN as u64 - 1)) as i64 + EPOCH_MS,
        seq: (packed & u64::from(SEQ_MAX)) as u32,
    };
    if id.check().is_err() {
        return Err(CloidError::NotPlatform);
    }
    Ok(id)
}

/// Algo a platform id belongs to. `None` means the string is not a platform
/// id, so the order belongs to no algo. This is a guard against mistakes:
/// a hand-placed order that happens to be a valid platform id is taken as
/// that algo's.
pub fn algo_of(s: &str) -> Option<u32> {
    decode(s).ok().map(|id| id.algo)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_algo_one_is_the_layout_word() {
        let s = encode(1, EPOCH_MS, 0).unwrap();
        let word = (1_u64 << VER_SHIFT) | (1_u64 << ALGO_SHIFT);
        assert_eq!(s, word.to_string());
        assert!(s.len() <= MAX_LEN);
        assert!(s.bytes().all(|b| b.is_ascii_digit()));
        assert_ne!(s.as_bytes()[0], b'0');

        let id = decode(&s).unwrap();
        assert_eq!(
            id,
            Id {
                ver: VERSION,
                algo: 1,
                ts_ms: EPOCH_MS,
                seq: 0,
            }
        );
        assert_eq!(id.to_string(), s);
        assert_eq!(algo_of(&s), Some(1));
    }

    #[test]
    fn full_fields_fill_sixty_one_bits() {
        let ts = EPOCH_MS + TS_SPAN - 1;
        let s = encode(ALGO_MAX, ts, SEQ_MAX).unwrap();
        let word: u64 = s.parse().unwrap();
        assert_eq!(word, (1_u64 << 61) - 1);
        assert!(s.len() <= MAX_LEN);
        assert_eq!(
            decode(&s).unwrap(),
            Id {
                ver: VERSION,
                algo: ALGO_MAX,
                ts_ms: ts,
                seq: SEQ_MAX,
            }
        );
    }

    #[test]
    fn round_trip_a_few_ids() {
        let samples = [
            (1, 0_i64, 0_u32),
            (2, 1, 3),
            (ALGO_MAX, TS_SPAN - 1, SEQ_MAX),
            (8, 20, 7),
        ];
        for (algo, off, seq) in samples {
            let ts = EPOCH_MS + off;
            let s = encode(algo, ts, seq).unwrap();
            let id = decode(&s).unwrap();
            assert_eq!(
                id,
                Id {
                    ver: VERSION,
                    algo,
                    ts_ms: ts,
                    seq,
                }
            );
            assert_eq!(algo_of(&s), Some(algo));
            assert_eq!(id.decimal(), s);
        }
    }

    #[test]
    fn layout_word_one_second_after_the_epoch() {
        // ver 1, algo 1, 1000 ms after the epoch, seq 0.
        let id = encode(1, EPOCH_MS + 1000, 0).unwrap();
        assert_eq!(id, "1154047404514713600");
        assert_eq!(algo_of(&id), Some(1));
        assert_eq!(algo_of(&encode(2, EPOCH_MS + 1000, 0).unwrap()), Some(2));
        assert_eq!(
            algo_of(&encode(ALGO_MAX, EPOCH_MS + (TS_SPAN - 1), SEQ_MAX).unwrap()),
            Some(ALGO_MAX)
        );
        for raw in ["manual", "", "0", "01", "12x"] {
            assert_eq!(algo_of(raw), None, "{raw}");
        }
        assert!(encode(0, EPOCH_MS, 0).is_err());
        assert!(encode(ALGO_MAX + 1, EPOCH_MS, 0).is_err());
        assert!(encode(1, EPOCH_MS - 1, 0).is_err());
        assert!(encode(1, EPOCH_MS + TS_SPAN, 0).is_err());
        assert!(encode(1, EPOCH_MS, SEQ_MAX + 1).is_err());
    }

    #[test]
    fn encode_rejects_a_field_that_does_not_fit() {
        let cases = [
            (0, EPOCH_MS, 0),
            (ALGO_MAX + 1, EPOCH_MS, 0),
            (1, EPOCH_MS - 1, 0),
            (1, EPOCH_MS + TS_SPAN, 0),
            (1, EPOCH_MS, SEQ_MAX + 1),
        ];
        for (algo, ts, seq) in cases {
            assert!(encode(algo, ts, seq).is_err(), "{algo} {ts} {seq}");
        }
    }

    #[test]
    fn strings_that_are_not_platform_ids() {
        let valid = encode(5, EPOCH_MS + 4, 7).unwrap();
        let algo_zero = (1_u64 << VER_SHIFT).to_string();
        let ver_eight = (8_u64 << VER_SHIFT).to_string();
        let leading_zero = format!("0{valid}");
        let trailing_space = format!("{valid} ");
        let too_long = "1".repeat(MAX_LEN + 1);
        let samples = [
            "",
            "0",
            "42",
            "abc-123",
            "+1",
            "x",
            leading_zero.as_str(),
            trailing_space.as_str(),
            too_long.as_str(),
            algo_zero.as_str(),
            ver_eight.as_str(),
        ];
        for s in samples {
            assert!(matches!(decode(s), Err(CloidError::NotPlatform)), "{s}");
            assert_eq!(algo_of(s), None, "{s}");
        }
        assert_eq!(algo_of(&valid), Some(5));
    }

    #[test]
    fn versions_two_through_seven_decode() {
        let ver_shift = SEQ_BITS + TS_BITS + ALGO_BITS;
        let algo_shift = SEQ_BITS + TS_BITS;
        let word = (7_u64 << ver_shift) | (2_u64 << algo_shift) | 9;
        let id = decode(&word.to_string()).unwrap();
        assert_eq!(id.ver, 7);
        assert_eq!(id.algo, 2);
        assert_eq!(id.seq, 9);
        assert_eq!(id.ts_ms, EPOCH_MS);
        assert_eq!(id.decimal(), word.to_string());
    }
}
