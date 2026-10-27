use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Item bit: the item was invalidated (`md I` / `ms I`) and is served as stale.
pub const BIT_STALE: u8 = 1;
/// Item bit: a client already received the "win" token to recache this item.
pub const BIT_WON: u8 = 2;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Item {
    pub value: Bytes,
    pub flags: u32,
    /// Absolute unix time in seconds; 0 means "never expires".
    pub exptime: u64,
    /// CAS unique: index of the log entry that last modified the item.
    pub cas: u64,
    /// Apply time (unix seconds) of the last modification; used by delayed `flush_all`.
    pub stored_at: u64,
    /// Meta-protocol state, see `BIT_*`.
    pub bits: u8,
}

impl Item {
    pub fn is_stale(&self) -> bool {
        self.bits & BIT_STALE != 0
    }

    pub fn is_won(&self) -> bool {
        self.bits & BIT_WON != 0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoreMode {
    Set,
    Add,
    Replace,
    Append,
    Prepend,
}

/// A state-changing operation. Everything that changes items goes through the log as an `Op`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Op {
    Store {
        mode: StoreMode,
        key: Bytes,
        value: Bytes,
        flags: u32,
        exptime: i64,
        /// Compare-and-swap: store only if the current CAS equals this value.
        cas: Option<u64>,
        /// `ms I`: when the CAS is older than the item's, store the value but mark it stale.
        invalidate: bool,
    },
    Delete {
        key: Bytes,
        cas: Option<u64>,
        /// `md I`: mark the item stale instead of removing it.
        invalidate: bool,
        /// `md T`: new TTL for an invalidated item.
        exptime: Option<i64>,
        /// `md x`: drop the value but keep the item.
        remove_value: bool,
    },
    Arith {
        key: Bytes,
        incr: bool,
        delta: u64,
        cas: Option<u64>,
        /// `ma N`: create the item with `initial` value and `exptime` on miss.
        vivify: Option<(u64, i64)>,
        /// `ma T`: update the TTL on hit.
        exptime: Option<i64>,
    },
    Touch {
        key: Bytes,
        exptime: i64,
    },
    /// `mg` with flags that change state on read (`T`, `N`, win-token logic).
    MetaGet {
        key: Bytes,
        /// `T`: update TTL.
        touch: Option<i64>,
        /// `N`: autovivify on miss with this TTL.
        vivify: Option<i64>,
        /// `R`: hand out the win token if the remaining TTL is below this.
        recache: Option<u64>,
    },
    Flush {
        delay: u64,
    },
    /// Advances the replicated clock; written periodically by the leader.
    Tick,
}

impl Op {
    pub fn key(&self) -> Option<&Bytes> {
        match self {
            Op::Store { key, .. }
            | Op::Delete { key, .. }
            | Op::Arith { key, .. }
            | Op::Touch { key, .. }
            | Op::MetaGet { key, .. } => Some(key),
            Op::Flush { .. } | Op::Tick => None,
        }
    }
}

/// An operation stamped with the proposer's wall clock (unix seconds).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Write {
    pub now: u64,
    pub op: Op,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Reply {
    Stored {
        cas: u64,
    },
    NotStored,
    /// CAS mismatch.
    Exists,
    NotFound,
    Deleted,
    /// Touch / GAT / state-changing meta get. `item` is the item after the change.
    /// `won`, `stale` and `already_won` drive the `W`, `X` and `Z` meta flags.
    Item {
        item: Item,
        won: bool,
        already_won: bool,
    },
    Arith {
        value: u64,
        item: Item,
    },
    NonNumeric,
    TooLarge,
    Ok,
}
