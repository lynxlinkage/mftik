//! Platform `client_order_id` layout (PROTOCOL §2, §9.4).
//!
//! `ver(4) | algo(10) | ts_ms(40) | seq(10)`, sent as the decimal of that
//! `u64`. `ts_ms` is unix milliseconds. The 40-bit window starts at
//! 2026-01-01T00:00:00Z. Version stays in 1..=7 so the top bit is clear.
//!
//! OMS filtering needs [`algo_of`]. The process-wide counter that mints ids
//! is `cloidgen` (E02-05), not this module.

pub(super) const EPOCH_MS: i64 = 1_767_225_600_000;

const VER_BITS: u32 = 4;
const ALGO_BITS: u32 = 10;
const TS_BITS: u32 = 40;
const SEQ_BITS: u32 = 10;

const VERSION: u8 = 1;
const ALGO_MIN: u32 = 1;
const ALGO_MAX: u32 = (1 << ALGO_BITS) - 1;
const SEQ_MAX: u32 = (1 << SEQ_BITS) - 1;
const MAX_LEN: usize = 19;
const TS_SPAN: i64 = 1 << TS_BITS;
const VER_MAX: u8 = 7;

const TS_SHIFT: u32 = SEQ_BITS;
const ALGO_SHIFT: u32 = SEQ_BITS + TS_BITS;
const VER_SHIFT: u32 = SEQ_BITS + TS_BITS + ALGO_BITS;

const _: () = assert!(VER_BITS + ALGO_BITS + TS_BITS + SEQ_BITS == 64);

pub fn encode(algo: u32, ts_ms: i64, seq: u32) -> Result<String, String> {
    if !(ALGO_MIN..=ALGO_MAX).contains(&algo) {
        return Err(format!("cloid: algo {algo} outside {ALGO_MIN}..{ALGO_MAX}"));
    }
    if ts_ms < EPOCH_MS || ts_ms.wrapping_sub(EPOCH_MS) >= TS_SPAN {
        return Err(format!(
            "cloid: ts_ms {ts_ms} outside the 40-bit window from {EPOCH_MS}"
        ));
    }
    if seq > SEQ_MAX {
        return Err(format!("cloid: seq {seq} above {SEQ_MAX}"));
    }
    let Ok(ts_off) = u64::try_from(ts_ms - EPOCH_MS) else {
        return Err(format!(
            "cloid: ts_ms {ts_ms} outside the 40-bit window from {EPOCH_MS}"
        ));
    };
    let packed = (u64::from(VERSION) << VER_SHIFT)
        | (u64::from(algo) << ALGO_SHIFT)
        | (ts_off << TS_SHIFT)
        | u64::from(seq);
    Ok(packed.to_string())
}

/// Algo field of a platform id. `None` means the string is not one this
/// platform minted: a hand-placed order belongs to no algo (E06-D002).
pub(super) fn algo_of(raw: &str) -> Option<u32> {
    decode(raw).map(|id| id.algo)
}

struct Id {
    algo: u32,
}

fn decode(raw: &str) -> Option<Id> {
    if raw.is_empty() || raw.len() > MAX_LEN || raw.starts_with('0') {
        return None;
    }
    if !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    let packed: u64 = raw.parse().ok()?;
    let ver = (packed >> VER_SHIFT) as u8;
    let algo = ((packed >> ALGO_SHIFT) & u64::from(ALGO_MAX)) as u32;
    let ts_off = (packed >> TS_SHIFT) & (TS_SPAN as u64 - 1);
    let ts_ms = EPOCH_MS + i64::try_from(ts_off).ok()?;
    // The bit masks already force seq and the timestamp into range. ver 0 and
    // algo 0 are the values `valid` rejects after that (E06-D002).
    if !(1..=VER_MAX).contains(&ver) || !(ALGO_MIN..=ALGO_MAX).contains(&algo) {
        return None;
    }
    if ts_ms < EPOCH_MS || ts_ms - EPOCH_MS >= TS_SPAN {
        return None;
    }
    Some(Id { algo })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_matches_the_protocol_word() {
        // ver 1, algo 1, 1000 ms after the epoch, seq 0.
        let id = encode(1, EPOCH_MS + 1000, 0).unwrap();
        assert_eq!(id, "1154047404514713600");
        assert_eq!(algo_of(&id), Some(1));
        assert_eq!(algo_of(&encode(2, EPOCH_MS + 1000, 0).unwrap()), Some(2));
        assert_eq!(
            algo_of(&encode(1023, EPOCH_MS + ((1_i64 << 40) - 1), 1023).unwrap()),
            Some(1023)
        );
        assert_eq!(algo_of("manual"), None);
        assert_eq!(algo_of(""), None);
        assert_eq!(algo_of("0"), None);
        assert_eq!(algo_of("01"), None);
        assert_eq!(algo_of("12x"), None);
        assert!(encode(0, EPOCH_MS, 0).is_err());
        assert!(encode(1024, EPOCH_MS, 0).is_err());
        assert!(encode(1, EPOCH_MS - 1, 0).is_err());
        assert!(encode(1, EPOCH_MS + (1_i64 << 40), 0).is_err());
        assert!(encode(1, EPOCH_MS, 1024).is_err());
    }
}
