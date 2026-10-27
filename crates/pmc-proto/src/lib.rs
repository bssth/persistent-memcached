//! memcached text protocol: classic commands and meta commands (`mg ms md ma mn me`).
//!
//! [`Parser`] turns bytes from a connection into [`Frame`]s; [`encode`] writes replies.
//! The parser is incremental (returns `None` until a whole request is buffered) and zero-copy:
//! keys and values are slices of the received buffer.

pub mod encode;
mod meta;
mod parse;

use bytes::Bytes;
pub use meta::MetaOpts;
pub use parse::{Limits, Parser};
pub use pmc_store::StoreMode;

pub const MAX_KEY_LEN: usize = 250;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    Get {
        keys: Vec<Bytes>,
        with_cas: bool,
    },
    Gat {
        exptime: i64,
        keys: Vec<Bytes>,
        with_cas: bool,
    },
    Store {
        mode: StoreMode,
        key: Bytes,
        flags: u32,
        exptime: i64,
        value: Bytes,
        cas: Option<u64>,
        noreply: bool,
    },
    /// A storage command whose value exceeds the item size limit. The value is swallowed;
    /// memcached replies `SERVER_ERROR object too large for cache` and, for `set` and `ms`,
    /// drops the old item so stale data does not linger.
    TooLarge {
        mode: StoreMode,
        key: Bytes,
        noreply: bool,
    },
    Delete {
        key: Bytes,
        noreply: bool,
    },
    Arith {
        key: Bytes,
        incr: bool,
        delta: u64,
        noreply: bool,
    },
    Touch {
        key: Bytes,
        exptime: i64,
        noreply: bool,
    },
    FlushAll {
        delay: i64,
        noreply: bool,
    },
    Stats {
        args: Vec<Bytes>,
    },
    Version,
    Verbosity {
        noreply: bool,
    },
    CacheMemlimit {
        megabytes: u64,
        noreply: bool,
    },
    Quit,
    MetaGet {
        key: Bytes,
        opts: MetaOpts,
    },
    MetaSet {
        key: Bytes,
        value: Bytes,
        opts: MetaOpts,
    },
    MetaDelete {
        key: Bytes,
        opts: MetaOpts,
    },
    MetaArith {
        key: Bytes,
        opts: MetaOpts,
    },
    MetaNoop,
    MetaDebug {
        key: Bytes,
        opts: MetaOpts,
    },
}

// One frame per request, consumed immediately: boxing the request would only add an allocation.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Frame {
    Request(Request),
    /// Reply with this line and keep the connection.
    Error(&'static str),
    /// A failed command with `noreply`: nothing is sent.
    Skip,
    /// Close the connection without a reply.
    Close,
}
