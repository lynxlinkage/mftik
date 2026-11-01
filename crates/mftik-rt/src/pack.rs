//! What `mftiko-pack -bin` reads out of a strategy binary (E08-D001,
//! E08-D005).
//!
//! mftiko's `artifact.PackStrategy` takes the manifest's `contracts` from the
//! `mftiko.contracts=` marker the Go SDK embeds (mftiko E09-D035, PROTOCOL
//! §8.2), and `runtime` and `sdk` from a `mftiko.sdk=` record (mftiko
//! E09-D099). It refuses a binary without the marker. Without the record it
//! falls back to Go build info, which a Rust binary does not have, so it
//! refuses that too. Every binary that calls [`crate::run`] carries both:
//!
//! - [`CONTRACTS`]: `mftiko.contracts=`, the majors of [`SESSION_CONTRACTS`]
//!   joined with commas, and a newline, as Go `sdk.contractsMarker`. It is
//!   built from the list `Hello.contracts` sends, so the two cannot drift.
//! - [`SDK`]: `mftiko.sdk=rust `, this crate's version, and a newline. The
//!   manifest's `runtime` and `sdk.lang` are then `rust`, and `sdk.version` is
//!   the version as written, without a `v`.
//!
//! The packer has to be mftiko v0.5.1 or later; an older one does not read the
//! record. Both are `#[used]` statics, and [`keep`] reads them from `run`, as
//! Go's `init` reads `embeddedContracts`: `#[used]` alone still lets the linker
//! drop an unreferenced static.

use mftik_core::c4::SESSION_CONTRACTS;

/// The prefix mftiko's `artifact.embeddedContracts` scans for.
const CONTRACTS_PREFIX: &[u8] = b"mftiko.contracts=";

/// The prefix, the majors, a comma between each two, and the newline.
const CONTRACTS_LEN: usize = {
    let mut len = CONTRACTS_PREFIX.len() + SESSION_CONTRACTS.len();
    let mut i = 0;
    while i < SESSION_CONTRACTS.len() {
        len += SESSION_CONTRACTS[i].len();
        i += 1;
    }
    len
};

/// `mftiko.contracts=worker.v1,stsctl.v1,md.v1,td.v1` and a newline. The
/// newline ends the list, so the bytes after it cannot lengthen a name.
#[used]
static CONTRACTS: [u8; CONTRACTS_LEN] = {
    let mut out = [0; CONTRACTS_LEN];
    let mut at = put(&mut out, 0, CONTRACTS_PREFIX);
    let mut i = 0;
    while i < SESSION_CONTRACTS.len() {
        if i > 0 {
            at = put(&mut out, at, b",");
        }
        at = put(&mut out, at, SESSION_CONTRACTS[i].as_bytes());
        i += 1;
    }
    put(&mut out, at, b"\n");
    out
};

const SDK_RECORD: &str = concat!("mftiko.sdk=rust ", env!("CARGO_PKG_VERSION"), "\n");

/// `mftiko.sdk=rust ` and the crate version, e.g. `1.3.0`, then a newline, as
/// mftiko's `artifact.embeddedSDK` reads it. Embedded on every platform, like
/// [`CONTRACTS`].
#[used]
static SDK: [u8; SDK_RECORD.len()] = {
    let mut out = [0; SDK_RECORD.len()];
    put(&mut out, 0, SDK_RECORD.as_bytes());
    out
};

/// Reads the records so the linker keeps them. [`crate::run`] calls it.
pub(crate) fn keep() {
    std::hint::black_box(&CONTRACTS);
    std::hint::black_box(&SDK);
}

const fn put(out: &mut [u8], at: usize, bytes: &[u8]) -> usize {
    let mut i = 0;
    while i < bytes.len() {
        out[at + i] = bytes[i];
        i += 1;
    }
    at + bytes.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The prefix mftiko's `artifact.embeddedSDK` scans for.
    const SDK_PREFIX: &str = "mftiko.sdk=";

    #[test]
    fn contracts_marker_matches_go_and_hello() {
        assert_eq!(
            &CONTRACTS,
            b"mftiko.contracts=worker.v1,stsctl.v1,md.v1,td.v1\n"
        );
        let list = CONTRACTS
            .strip_prefix(CONTRACTS_PREFIX)
            .and_then(|rest| rest.strip_suffix(b"\n"))
            .unwrap();
        let hello = mftik_core::c4::Config::session("r", 1).contracts;
        assert_eq!(list, hello.join(",").as_bytes());
    }

    /// mftiko `parseSDKRecord`: a lower-case language, one space, a version
    /// that starts with a digit or `v`, and a newline.
    #[test]
    fn sdk_record_is_rust_and_the_crate_version() {
        assert!(SDK_RECORD.starts_with(SDK_PREFIX));
        assert_eq!(&SDK, SDK_RECORD.as_bytes());
        let record = SDK
            .strip_prefix(SDK_PREFIX.as_bytes())
            .and_then(|rest| rest.strip_suffix(b"\n"))
            .unwrap();
        let want = format!("rust {}", crate::RELEASE);
        assert_eq!(record, want.as_bytes());
        let version = &record[b"rust ".len()..];
        assert!(version[0].is_ascii_digit());
        assert!(version.len() <= 64);
        assert!(
            version
                .iter()
                .all(|&b| b.is_ascii_alphanumeric() || b"+-.".contains(&b))
        );
    }
}
