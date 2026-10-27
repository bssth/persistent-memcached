//! Storage core of pmcd.
//!
//! * [`Store`] — in-memory items with a deterministic `apply`: every replica that applies the
//!   same sequence of [`Write`]s ends up with the same items, CAS values and expirations.
//! * [`wal::Wal`] — segmented append-only log with CRC32C framing (the "binlog").
//! * [`snapshot`] — streaming point-in-time dumps of a [`Store`].

pub mod item;
pub mod numeric;
pub mod snapshot;
pub mod store;
pub mod wal;

pub use item::{Item, Op, Reply, StoreMode, Write};
pub use store::{Store, StoreConfig, StoreStats};

/// Exptime values above this many seconds are absolute unix timestamps (memcached rule).
pub const REALTIME_MAXDELTA: i64 = 60 * 60 * 24 * 30;

/// Resolves a memcached exptime against `now`.
///
/// Returns `Some(0)` for "never expires", `Some(t)` for an absolute expiry time and `None`
/// when the item is already expired (negative exptime or an absolute time in the past).
pub fn resolve_exptime(exptime: i64, now: u64) -> Option<u64> {
    if exptime == 0 {
        Some(0)
    } else if exptime < 0 {
        None
    } else if exptime > REALTIME_MAXDELTA {
        let t = exptime as u64;
        if t <= now { None } else { Some(t) }
    } else {
        Some(now + exptime as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exptime_rules() {
        assert_eq!(resolve_exptime(0, 1000), Some(0));
        assert_eq!(resolve_exptime(-1, 1000), None);
        assert_eq!(resolve_exptime(10, 1000), Some(1010));
        assert_eq!(resolve_exptime(REALTIME_MAXDELTA, 1000), Some(1000 + REALTIME_MAXDELTA as u64));
        assert_eq!(resolve_exptime(REALTIME_MAXDELTA + 1, 1_000_000_000), None);
        assert_eq!(resolve_exptime(2_000_000_000, 1_000_000_000), Some(2_000_000_000));
    }
}
