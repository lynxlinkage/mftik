//! What `mftiko-pack -bin` reads out of a strategy binary (E08-D001,
//! E08-D005).
//!
//! mftiko's `artifact.PackStrategy` takes the manifest's `contracts` from the
//! `mftiko.contracts=` marker the Go SDK embeds (mftiko E09-D035, PROTOCOL
//! §8.2), and `sdk.version` from the binary's Go build info. It refuses a
//! binary without either. Every binary that calls [`crate::run`] carries both:
//!
//! - [`CONTRACTS`]: `mftiko.contracts=`, the majors of [`SESSION_CONTRACTS`]
//!   joined with commas, and a newline, as Go `sdk.contractsMarker`. It is
//!   built from the list `Hello.contracts` sends, so the two cannot drift.
//! - [`BUILD_INFO`] (Linux only): a record in the inline format Go's linker
//!   writes since 1.18, in section `.go.buildinfo`, where `debug/buildinfo`
//!   looks first. It says the module `github.com/BullionBear/mftiko` is
//!   replaced by `github.com/lynxlinkage/mftik` at this crate's version, so
//!   `sdk.version` is `v` and [`crate::RELEASE`]. Its version string, where
//!   Go puts the toolchain's, is `mftik-rt v` and the same version.
//!
//! mftiko-pack still writes `runtime` and `sdk.lang` as `go`. PROTOCOL §8.2
//! uses `runtime` only for display and statistics, and mftiko reads neither.
//!
//! Both are `#[used]` statics, and [`keep`] reads them from `run`, as Go's
//! `init` reads `embeddedContracts` and its linker references the build info:
//! `#[used]` alone still lets the linker drop an unreferenced section.

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

/// `runtime.buildVersion`'s place. No Go toolchain built the binary.
#[cfg(target_os = "linux")]
const GO_VERSION: &str = concat!("mftik-rt v", env!("CARGO_PKG_VERSION"));

/// `runtime.modinfo`'s lines, as `BuildInfo.String` writes a replaced
/// dependency, blank line included. The required version is Go's placeholder
/// for none.
#[cfg(target_os = "linux")]
const MODINFO: &str = concat!(
    "dep\tgithub.com/BullionBear/mftiko\tv0.0.0-00010101000000-000000000000\n",
    "=>\tgithub.com/lynxlinkage/mftik\tv",
    env!("CARGO_PKG_VERSION"),
    "\t\n\n",
);

/// cmd/go's `modload.infoStart` and `infoEnd`. `debug/buildinfo` strips 16
/// bytes from each end of the module string.
#[cfg(target_os = "linux")]
const INFO_START: [u8; 16] = [
    0x30, 0x77, 0xaf, 0x0c, 0x92, 0x74, 0x08, 0x02, 0x41, 0xe1, 0xc1, 0x07, 0xe6, 0xd6, 0x18, 0xe6,
];
#[cfg(target_os = "linux")]
const INFO_END: [u8; 16] = [
    0xf9, 0x32, 0x43, 0x31, 0x86, 0x18, 0x20, 0x72, 0x00, 0x82, 0x42, 0x10, 0x41, 0x16, 0xd8, 0xf2,
];

/// The 14 bytes `debug/buildinfo` searches for, at a 16-byte boundary.
#[cfg(target_os = "linux")]
const BUILD_INFO_MAGIC: &[u8] = b"\xff Go buildinf:";

/// The header: the magic, the pointer size, the flags, and 16 bytes the
/// inline format leaves zero.
#[cfg(target_os = "linux")]
const BUILD_INFO_HEADER: usize = 32;

#[cfg(target_os = "linux")]
const MODINFO_LEN: usize = INFO_START.len() + MODINFO.len() + INFO_END.len();

/// The header, two strings each after its uvarint length, and zeros up to a
/// multiple of 16, as Go's linker pads it.
#[cfg(target_os = "linux")]
const BUILD_INFO_LEN: usize = (BUILD_INFO_HEADER
    + uvarint_len(GO_VERSION.len())
    + GO_VERSION.len()
    + uvarint_len(MODINFO_LEN)
    + MODINFO_LEN)
    .next_multiple_of(16);

#[cfg(target_os = "linux")]
#[repr(C, align(16))]
struct BuildInfo([u8; BUILD_INFO_LEN]);

#[cfg(target_os = "linux")]
#[used]
#[unsafe(link_section = ".go.buildinfo")]
static BUILD_INFO: BuildInfo = BuildInfo({
    let mut out = [0; BUILD_INFO_LEN];
    put(&mut out, 0, BUILD_INFO_MAGIC);
    out[BUILD_INFO_MAGIC.len()] = size_of::<usize>() as u8;
    // Bit 1: the strings follow the header. Bit 0: big-endian.
    out[BUILD_INFO_MAGIC.len() + 1] = 2 | cfg!(target_endian = "big") as u8;
    let mut at = put_uvarint(&mut out, BUILD_INFO_HEADER, GO_VERSION.len());
    at = put(&mut out, at, GO_VERSION.as_bytes());
    at = put_uvarint(&mut out, at, MODINFO_LEN);
    at = put(&mut out, at, &INFO_START);
    at = put(&mut out, at, MODINFO.as_bytes());
    put(&mut out, at, &INFO_END);
    out
});

/// Reads both records so the linker keeps them. [`crate::run`] calls it.
pub(crate) fn keep() {
    std::hint::black_box(&CONTRACTS);
    #[cfg(target_os = "linux")]
    std::hint::black_box(&BUILD_INFO);
}

const fn put(out: &mut [u8], at: usize, bytes: &[u8]) -> usize {
    let mut i = 0;
    while i < bytes.len() {
        out[at + i] = bytes[i];
        i += 1;
    }
    at + bytes.len()
}

/// Go `binary.PutUvarint`.
#[cfg(target_os = "linux")]
const fn put_uvarint(out: &mut [u8], mut at: usize, mut n: usize) -> usize {
    while n >= 0x80 {
        out[at] = n as u8 | 0x80;
        n >>= 7;
        at += 1;
    }
    out[at] = n as u8;
    at + 1
}

#[cfg(target_os = "linux")]
const fn uvarint_len(mut n: usize) -> usize {
    let mut len = 1;
    while n >= 0x80 {
        n >>= 7;
        len += 1;
    }
    len
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[cfg(target_os = "linux")]
    #[test]
    fn uvarint_matches_go() {
        for (n, want) in [
            (0, &[0x00][..]),
            (1, &[0x01]),
            (127, &[0x7f]),
            (128, &[0x80, 0x01]),
            (300, &[0xac, 0x02]),
            (16384, &[0x80, 0x80, 0x01]),
        ] {
            let mut out = [0xff; 4];
            assert_eq!(put_uvarint(&mut out, 1, n), 1 + want.len(), "{n}");
            assert_eq!(&out[1..1 + want.len()], want, "{n}");
            assert_eq!(uvarint_len(n), want.len(), "{n}");
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn build_info_header_and_padding() {
        let record = &BUILD_INFO.0;
        assert_eq!(std::ptr::from_ref(&BUILD_INFO).addr() % 16, 0);
        assert_eq!(record.len() % 16, 0);
        assert_eq!(&record[..14], b"\xff Go buildinf:");
        assert_eq!(record[14], size_of::<usize>() as u8);
        assert_eq!(record[15], 2);
        assert_eq!(&record[16..32], &[0; 16]);
        let tail = 32 + 1 + GO_VERSION.len() + uvarint_len(MODINFO_LEN) + MODINFO_LEN;
        assert!(record.len() - tail < 16);
        assert!(record[tail..].iter().all(|&b| b == 0));
    }
}
