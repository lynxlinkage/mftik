//! v1 `md.f` envelope checks used by the prehook.
//!
//! The order is length, then version, then the reserved bytes (PROTOCOL §7.1,
//! E08-D049). The digest is not checked.

use std::fmt;

pub(crate) const SIZE: usize = 40;
const VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EnvelopeError {
    Short(usize),
    Version(u8),
    Reserved { offset: usize, byte: u8 },
}

impl fmt::Display for EnvelopeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EnvelopeError::Short(len) => {
                write!(
                    formatter,
                    "envelope: message shorter than 40 bytes: {len} bytes"
                )
            }
            EnvelopeError::Version(version) => {
                write!(formatter, "envelope: unknown version: {version}")
            }
            EnvelopeError::Reserved { offset, byte } => {
                write!(
                    formatter,
                    "envelope: reserved byte is not zero: offset {offset} is {byte:#x}"
                )
            }
        }
    }
}

impl std::error::Error for EnvelopeError {}

pub(crate) fn md_ts_ns(msg: &[u8]) -> Result<i64, EnvelopeError> {
    reject(msg)?;
    let bytes: [u8; 8] = msg[8..16].try_into().expect("40-byte header has md_ts_ns");
    Ok(i64::from_le_bytes(bytes))
}

fn reject(msg: &[u8]) -> Result<(), EnvelopeError> {
    if msg.len() < SIZE {
        return Err(EnvelopeError::Short(msg.len()));
    }
    if msg[0] != VERSION {
        return Err(EnvelopeError::Version(msg[0]));
    }
    for (offset, byte) in msg.iter().enumerate().skip(1).take(7) {
        if *byte != 0 {
            return Err(EnvelopeError::Reserved {
                offset,
                byte: *byte,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn encode_v1(md_ts_ns: i64, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![0u8; SIZE + payload.len()];
    out[0] = VERSION;
    out[8..16].copy_from_slice(&md_ts_ns.to_le_bytes());
    out[16..24].copy_from_slice(&1u64.to_le_bytes());
    out[24..32].copy_from_slice(&1u64.to_le_bytes());
    out[SIZE..].copy_from_slice(payload);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn length_is_checked_before_version_and_reserved_bytes() {
        let short = [2u8, 0xff, 0x01];
        assert_eq!(
            md_ts_ns(&short),
            Err(EnvelopeError::Short(3)),
            "a short buffer is rejected before its version byte is read"
        );

        let mut bad_version = vec![0u8; SIZE];
        bad_version[0] = 2;
        bad_version[3] = 0x4;
        assert_eq!(md_ts_ns(&bad_version), Err(EnvelopeError::Version(2)));

        let mut reserved = vec![0u8; SIZE];
        reserved[0] = VERSION;
        reserved[1] = 0x2;
        reserved[3] = 0x4;
        assert_eq!(
            md_ts_ns(&reserved),
            Err(EnvelopeError::Reserved {
                offset: 1,
                byte: 0x2
            })
        );
    }

    #[test]
    fn readable_header_returns_md_ts_ns() {
        let msg = encode_v1(1_700_000_000_000_000_000, b"a");
        assert_eq!(md_ts_ns(&msg).expect("header"), 1_700_000_000_000_000_000);
        assert_eq!(md_ts_ns(&encode_v1(0, b"")).expect("empty payload"), 0);
    }

    #[test]
    fn error_text_matches_the_log_line_the_host_records() {
        let err = md_ts_ns(&[0x7f]).expect_err("short");
        assert_eq!(
            err.to_string(),
            "envelope: message shorter than 40 bytes: 1 bytes"
        );
        let mut reserved = vec![0u8; SIZE];
        reserved[0] = VERSION;
        reserved[4] = 0xab;
        assert_eq!(
            md_ts_ns(&reserved).expect_err("reserved").to_string(),
            "envelope: reserved byte is not zero: offset 4 is 0xab"
        );
    }
}
