use std::hash::BuildHasher;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering::Relaxed};

use bytes::{Bytes, BytesMut};
use hashbrown::HashMap;
use parking_lot::RwLock;

use crate::item::{BIT_STALE, BIT_WON, Item, Op, Reply, StoreMode, Write};
use crate::numeric::safe_strtoull;
use crate::{REALTIME_MAXDELTA, resolve_exptime};

/// Rough per-item overhead used for memory accounting and the item size limit.
pub const ITEM_OVERHEAD: usize = 56;

const FETCHED: u64 = 1 << 63;

#[derive(Clone, Debug)]
pub struct StoreConfig {
    /// Number of lock stripes; rounded up to a power of two.
    pub stripes: usize,
    /// Maximum `key + value + overhead` size of one item, like memcached's `-I`.
    pub max_item_size: usize,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self { stripes: 256, max_item_size: 1024 * 1024 }
    }
}

struct Slot {
    item: Item,
    /// Local, non-replicated access info: bit 63 = fetched at least once, rest = unix secs.
    access: AtomicU64,
}

impl Slot {
    fn new(item: Item, now: u64) -> Self {
        Self { item, access: AtomicU64::new(now) }
    }
}

/// Result of a local read.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Hit {
    pub item: Item,
    /// The item had been fetched before this read (`mg h`).
    pub fetched_before: bool,
    /// Unix time of the previous access (`mg l`).
    pub last_access: u64,
}

/// Replicated clocks of the state machine. Part of every snapshot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Clocks {
    pub applied_index: u64,
    pub apply_time: u64,
    /// Items with `cas < flush_index` were removed by an immediate `flush_all`.
    pub flush_index: u64,
    /// Items with `stored_at < flush_floor` were removed by a delayed `flush_all` that fired.
    pub flush_floor: u64,
    /// Pending delayed `flush_all` deadline (0 = none).
    pub flush_at: u64,
}

/// A point-in-time copy of the live items, used to write snapshots.
pub struct Image {
    pub clocks: Clocks,
    pub items: Vec<(Bytes, Item)>,
}

#[derive(Clone, Copy, Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct StoreStats {
    pub curr_items: u64,
    pub bytes: u64,
    pub total_items: u64,
}

pub struct Store {
    cfg: StoreConfig,
    stripes: Box<[RwLock<HashMap<Bytes, Slot>>]>,
    hasher: hashbrown::DefaultHashBuilder,
    applied_index: AtomicU64,
    apply_time: AtomicU64,
    flush_index: AtomicU64,
    flush_floor: AtomicU64,
    flush_at: AtomicU64,
    used_bytes: AtomicUsize,
    count: AtomicUsize,
    total_items: AtomicU64,
}

fn cost(key: &[u8], item: &Item) -> usize {
    key.len() + item.value.len() + ITEM_OVERHEAD
}

impl Store {
    pub fn new(cfg: StoreConfig) -> Self {
        let n = cfg.stripes.max(1).next_power_of_two();
        let stripes = (0..n).map(|_| RwLock::new(HashMap::new())).collect();
        Self {
            cfg,
            stripes,
            hasher: Default::default(),
            applied_index: AtomicU64::new(0),
            apply_time: AtomicU64::new(0),
            flush_index: AtomicU64::new(0),
            flush_floor: AtomicU64::new(0),
            flush_at: AtomicU64::new(0),
            used_bytes: AtomicUsize::new(0),
            count: AtomicUsize::new(0),
            total_items: AtomicU64::new(0),
        }
    }

    pub fn config(&self) -> &StoreConfig {
        &self.cfg
    }

    /// Whether an item with this key and value length fits into the item size limit.
    pub fn fits(&self, key_len: usize, value_len: usize) -> bool {
        key_len + value_len + ITEM_OVERHEAD <= self.cfg.max_item_size
    }

    fn stripe(&self, key: &[u8]) -> &RwLock<HashMap<Bytes, Slot>> {
        let h = self.hasher.hash_one(key);
        &self.stripes[(h as usize) & (self.stripes.len() - 1)]
    }

    pub fn clocks(&self) -> Clocks {
        Clocks {
            applied_index: self.applied_index.load(Relaxed),
            apply_time: self.apply_time.load(Relaxed),
            flush_index: self.flush_index.load(Relaxed),
            flush_floor: self.flush_floor.load(Relaxed),
            flush_at: self.flush_at.load(Relaxed),
        }
    }

    pub fn applied_index(&self) -> u64 {
        self.applied_index.load(Relaxed)
    }

    pub fn apply_time(&self) -> u64 {
        self.apply_time.load(Relaxed)
    }

    pub fn stats(&self) -> StoreStats {
        StoreStats {
            curr_items: self.count.load(Relaxed) as u64,
            bytes: self.used_bytes.load(Relaxed) as u64,
            total_items: self.total_items.load(Relaxed),
        }
    }

    pub fn used_bytes(&self) -> usize {
        self.used_bytes.load(Relaxed)
    }

    fn alive(&self, it: &Item, now: u64) -> bool {
        if it.exptime != 0 && it.exptime <= now {
            return false;
        }
        if it.cas < self.flush_index.load(Relaxed) || it.stored_at < self.flush_floor.load(Relaxed) {
            return false;
        }
        let fa = self.flush_at.load(Relaxed);
        !(fa != 0 && now >= fa && it.stored_at < fa)
    }

    fn read_now(&self, wall_now: u64) -> u64 {
        wall_now.max(self.apply_time.load(Relaxed))
    }

    /// Local read. `bump` records the access (fetched flag and last access time).
    pub fn get(&self, key: &[u8], wall_now: u64, bump: bool) -> Option<Hit> {
        let now = self.read_now(wall_now);
        let map = self.stripe(key).read();
        let slot = map.get(key)?;
        if !self.alive(&slot.item, now) {
            return None;
        }
        let prev = if bump { slot.access.swap(FETCHED | now, Relaxed) } else { slot.access.load(Relaxed) };
        Some(Hit { item: slot.item.clone(), fetched_before: prev & FETCHED != 0, last_access: prev & !FETCHED })
    }

    fn insert(&self, map: &mut HashMap<Bytes, Slot>, key: &Bytes, mut item: Item, now: u64) {
        // Keys and values usually arrive as slices of a big network or log buffer; copy them
        // so a small item does not pin that whole buffer in memory.
        item.value = Bytes::copy_from_slice(&item.value);
        let add = cost(key, &item);
        self.used_bytes.fetch_add(add, Relaxed);
        self.total_items.fetch_add(1, Relaxed);
        match map.insert(Bytes::copy_from_slice(key), Slot::new(item, now)) {
            Some(old) => {
                self.used_bytes.fetch_sub(cost(key, &old.item), Relaxed);
            }
            None => {
                self.count.fetch_add(1, Relaxed);
            }
        }
    }

    fn remove(&self, map: &mut HashMap<Bytes, Slot>, key: &[u8]) {
        if let Some(old) = map.remove(key) {
            self.used_bytes.fetch_sub(cost(key, &old.item), Relaxed);
            self.count.fetch_sub(1, Relaxed);
        }
    }

    /// Replaces the value of an existing slot and fixes the memory accounting.
    fn set_value(&self, key: &[u8], slot: &mut Slot, value: Bytes) {
        let before = cost(key, &slot.item);
        slot.item.value = value;
        let after = cost(key, &slot.item);
        if after >= before {
            self.used_bytes.fetch_add(after - before, Relaxed);
        } else {
            self.used_bytes.fetch_sub(before - after, Relaxed);
        }
    }

    /// Applies one replicated write. Must be called in log order from a single thread.
    /// The result depends only on the store state and `w`, never on local clocks.
    pub fn apply(&self, index: u64, w: &Write) -> Reply {
        let now = self.apply_time.load(Relaxed).max(w.now);
        self.apply_time.store(now, Relaxed);
        self.applied_index.store(index, Relaxed);
        match &w.op {
            Op::Store { mode, key, value, flags, exptime, cas, invalidate } => {
                self.apply_store(index, now, *mode, key, value, *flags, *exptime, *cas, *invalidate)
            }
            Op::Delete { key, cas, invalidate, exptime, remove_value } => {
                self.apply_delete(index, now, key, *cas, *invalidate, *exptime, *remove_value)
            }
            Op::Arith { key, incr, delta, cas, vivify, exptime } => {
                self.apply_arith(index, now, key, *incr, *delta, *cas, *vivify, *exptime)
            }
            Op::Touch { key, exptime } => self.apply_touch(now, key, *exptime),
            Op::MetaGet { key, touch, vivify, recache } => {
                self.apply_meta_get(index, now, key, *touch, *vivify, *recache)
            }
            Op::Flush { delay } => {
                // Like exptime: a delay above 30 days is an absolute unix time.
                let deadline = match *delay {
                    0 => now,
                    d if d > REALTIME_MAXDELTA as u64 => d,
                    d => now + d,
                };
                if deadline <= now {
                    self.flush_index.store(index, Relaxed);
                    self.flush_at.store(0, Relaxed);
                } else {
                    let fa = self.flush_at.load(Relaxed);
                    if fa != 0 && now >= fa {
                        let floor = self.flush_floor.load(Relaxed).max(fa);
                        self.flush_floor.store(floor, Relaxed);
                    }
                    self.flush_at.store(deadline, Relaxed);
                }
                Reply::Ok
            }
            Op::Tick => Reply::Ok,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_store(
        &self,
        index: u64,
        now: u64,
        mode: StoreMode,
        key: &Bytes,
        value: &Bytes,
        flags: u32,
        exptime: i64,
        cas: Option<u64>,
        invalidate: bool,
    ) -> Reply {
        let mut map = self.stripe(key).write();
        let cur = map.get(key).map(|s| &s.item).filter(|it| self.alive(it, now));
        let mut stale = false;
        if let Some(c) = cas {
            match cur {
                None => return Reply::NotFound,
                Some(it) if it.cas != c => {
                    if invalidate && c < it.cas {
                        stale = true;
                    } else {
                        return Reply::Exists;
                    }
                }
                Some(_) => {}
            }
        }
        let (value, flags, exptime) = match mode {
            StoreMode::Set => (value.clone(), flags, resolve_exptime(exptime, now)),
            StoreMode::Add => {
                if cur.is_some() {
                    return Reply::NotStored;
                }
                (value.clone(), flags, resolve_exptime(exptime, now))
            }
            StoreMode::Replace => {
                if cur.is_none() {
                    return Reply::NotStored;
                }
                (value.clone(), flags, resolve_exptime(exptime, now))
            }
            StoreMode::Append | StoreMode::Prepend => {
                let Some(it) = cur else { return Reply::NotStored };
                let mut v = BytesMut::with_capacity(it.value.len() + value.len());
                if mode == StoreMode::Append {
                    v.extend_from_slice(&it.value);
                    v.extend_from_slice(value);
                } else {
                    v.extend_from_slice(value);
                    v.extend_from_slice(&it.value);
                }
                (v.freeze(), it.flags, Some(it.exptime))
            }
        };
        if !self.fits(key.len(), value.len()) {
            return Reply::TooLarge;
        }
        let size = value.len() as u64;
        match exptime {
            None => self.remove(&mut map, key),
            Some(exptime) => {
                let bits = if stale { BIT_STALE } else { 0 };
                let item = Item { value, flags, exptime, cas: index, stored_at: now, bits };
                self.insert(&mut map, key, item, now);
            }
        }
        Reply::Stored { cas: index, size }
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_delete(
        &self,
        index: u64,
        now: u64,
        key: &Bytes,
        cas: Option<u64>,
        invalidate: bool,
        exptime: Option<i64>,
        remove_value: bool,
    ) -> Reply {
        let mut map = self.stripe(key).write();
        let alive = map.get(key).is_some_and(|s| self.alive(&s.item, now));
        if !alive {
            self.remove(&mut map, key);
            return Reply::NotFound;
        }
        let slot = map.get_mut(key).expect("checked above");
        if let Some(c) = cas {
            if slot.item.cas != c {
                return Reply::Exists;
            }
        }
        if remove_value {
            // `md x` leaves an empty tombstone: flags reset, TTL from `T` (default never),
            // CAS unchanged.
            self.set_value(key, slot, Bytes::new());
            slot.item.flags = 0;
            if !invalidate {
                match exptime.map(|t| resolve_exptime(t, now)) {
                    None => slot.item.exptime = 0,
                    Some(Some(e)) => slot.item.exptime = e,
                    Some(None) => self.remove(&mut map, key),
                }
                return Reply::Deleted;
            }
        }
        if !invalidate {
            if !remove_value {
                self.remove(&mut map, key);
            }
            return Reply::Deleted;
        }
        slot.item.bits = (slot.item.bits | BIT_STALE) & !BIT_WON;
        slot.item.cas = index;
        if let Some(t) = exptime {
            match resolve_exptime(t, now) {
                Some(e) => slot.item.exptime = e,
                None => self.remove(&mut map, key),
            }
        }
        Reply::Deleted
    }

    #[allow(clippy::too_many_arguments)]
    fn apply_arith(
        &self,
        index: u64,
        now: u64,
        key: &Bytes,
        incr: bool,
        delta: u64,
        cas: Option<u64>,
        vivify: Option<(u64, i64)>,
        exptime: Option<i64>,
    ) -> Reply {
        let mut map = self.stripe(key).write();
        let alive = map.get(key).is_some_and(|s| self.alive(&s.item, now));
        if !alive {
            self.remove(&mut map, key);
            let Some((initial, ttl)) = vivify else { return Reply::NotFound };
            let value = Bytes::from(initial.to_string());
            let item = Item {
                value,
                flags: 0,
                exptime: resolve_exptime(ttl, now).unwrap_or(now),
                cas: index,
                stored_at: now,
                bits: 0,
            };
            if resolve_exptime(ttl, now).is_some() {
                self.insert(&mut map, key, item.clone(), now);
            }
            return Reply::Arith { value: initial, item };
        }
        let slot = map.get_mut(key).expect("checked above");
        if let Some(c) = cas {
            if slot.item.cas != c {
                return Reply::Exists;
            }
        }
        let Some(cur) = safe_strtoull(&slot.item.value) else { return Reply::NonNumeric };
        let new = if incr { cur.wrapping_add(delta) } else { cur.saturating_sub(delta) };
        // memcached sometimes pads a shorter result with spaces (it depends on refcounts);
        // storing the exact digits is the deterministic choice.
        self.set_value(key, slot, Bytes::from(new.to_string()));
        slot.item.cas = index;
        slot.item.stored_at = now;
        if let Some(t) = exptime {
            match resolve_exptime(t, now) {
                Some(e) => slot.item.exptime = e,
                None => {
                    let item = slot.item.clone();
                    self.remove(&mut map, key);
                    return Reply::Arith { value: new, item };
                }
            }
        }
        Reply::Arith { value: new, item: slot.item.clone() }
    }

    fn apply_touch(&self, now: u64, key: &Bytes, exptime: i64) -> Reply {
        let mut map = self.stripe(key).write();
        let alive = map.get(key).is_some_and(|s| self.alive(&s.item, now));
        if !alive {
            self.remove(&mut map, key);
            return Reply::NotFound;
        }
        let slot = map.get_mut(key).expect("checked above");
        match resolve_exptime(exptime, now) {
            Some(e) => {
                slot.item.exptime = e;
                Reply::Item { item: slot.item.clone(), won: false, already_won: false }
            }
            None => {
                let item = slot.item.clone();
                self.remove(&mut map, key);
                Reply::Item { item, won: false, already_won: false }
            }
        }
    }

    fn apply_meta_get(
        &self,
        index: u64,
        now: u64,
        key: &Bytes,
        touch: Option<i64>,
        vivify: Option<i64>,
        recache: Option<u64>,
    ) -> Reply {
        let mut map = self.stripe(key).write();
        let alive = map.get(key).is_some_and(|s| self.alive(&s.item, now));
        if !alive {
            self.remove(&mut map, key);
            let Some(ttl) = vivify else { return Reply::NotFound };
            let Some(exptime) = resolve_exptime(ttl, now) else { return Reply::NotFound };
            let item = Item { value: Bytes::new(), flags: 0, exptime, cas: index, stored_at: now, bits: BIT_WON };
            self.insert(&mut map, key, item.clone(), now);
            return Reply::Item { item, won: true, already_won: false };
        }
        let slot = map.get_mut(key).expect("checked above");
        if let Some(t) = touch {
            match resolve_exptime(t, now) {
                Some(e) => slot.item.exptime = e,
                None => {
                    let item = slot.item.clone();
                    self.remove(&mut map, key);
                    return Reply::Item { item, won: false, already_won: false };
                }
            }
        }
        let wants_win = slot.item.is_stale() || needs_recache(&slot.item, now, recache);
        let (won, already_won) = if slot.item.is_won() {
            (false, true)
        } else if wants_win {
            slot.item.bits |= BIT_WON;
            (true, false)
        } else {
            (false, false)
        };
        Reply::Item { item: slot.item.clone(), won, already_won }
    }

    /// Whether a `mg` with these flags must go through the log instead of being a local read.
    pub fn meta_get_needs_write(&self, key: &[u8], wall_now: u64, recache: Option<u64>) -> bool {
        let now = self.read_now(wall_now);
        let map = self.stripe(key).read();
        match map.get(key).filter(|s| self.alive(&s.item, now)) {
            None => false,
            Some(s) => !s.item.is_won() && (s.item.is_stale() || needs_recache(&s.item, now, recache)),
        }
    }

    /// Physically removes items that are dead as of the replicated apply time.
    /// Safe to run concurrently with `apply`: such items stay dead for every later write.
    pub fn sweep(&self, start: usize, stripes: usize) -> usize {
        let now = self.apply_time.load(Relaxed);
        let n = self.stripes.len();
        let mut removed = 0;
        for i in 0..stripes.min(n) {
            let mut map = self.stripes[(start + i) % n].write();
            map.retain(|k, s| {
                let keep = self.alive(&s.item, now);
                if !keep {
                    self.used_bytes.fetch_sub(cost(k, &s.item), Relaxed);
                    self.count.fetch_sub(1, Relaxed);
                    removed += 1;
                }
                keep
            });
        }
        removed
    }

    pub fn stripe_count(&self) -> usize {
        self.stripes.len()
    }

    /// Copies all live items. Call between two `apply`s to get a consistent image.
    ///
    /// Items are copied with all their bits: the "win token handed out" bit is replicated
    /// state, so a replica restored from a snapshot must not hand the token out again.
    pub fn image(&self) -> Image {
        let clocks = self.clocks();
        let mut items = Vec::with_capacity(self.count.load(Relaxed));
        for stripe in self.stripes.iter() {
            let map = stripe.read();
            for (k, s) in map.iter() {
                if self.alive(&s.item, clocks.apply_time) {
                    items.push((k.clone(), s.item.clone()));
                }
            }
        }
        Image { clocks, items }
    }

    /// Drops everything and resets the clocks, e.g. before loading a snapshot.
    pub fn reset(&self, clocks: Clocks) {
        for stripe in self.stripes.iter() {
            stripe.write().clear();
        }
        self.used_bytes.store(0, Relaxed);
        self.count.store(0, Relaxed);
        self.applied_index.store(clocks.applied_index, Relaxed);
        self.apply_time.store(clocks.apply_time, Relaxed);
        self.flush_index.store(clocks.flush_index, Relaxed);
        self.flush_floor.store(clocks.flush_floor, Relaxed);
        self.flush_at.store(clocks.flush_at, Relaxed);
    }

    /// Inserts an item as is (snapshot loading).
    pub fn load(&self, key: Bytes, item: Item) {
        let now = self.apply_time.load(Relaxed);
        let mut map = self.stripe(&key).write();
        self.insert(&mut map, &key, item, now);
    }
}

fn needs_recache(item: &Item, now: u64, recache: Option<u64>) -> bool {
    match recache {
        Some(r) if item.exptime != 0 => item.exptime.saturating_sub(now) < r,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store() -> Store {
        Store::new(StoreConfig::default())
    }

    fn set(key: &str, value: &str, exptime: i64) -> Op {
        Op::Store {
            mode: StoreMode::Set,
            key: Bytes::copy_from_slice(key.as_bytes()),
            value: Bytes::copy_from_slice(value.as_bytes()),
            flags: 7,
            exptime,
            cas: None,
            invalidate: false,
        }
    }

    fn w(now: u64, op: Op) -> Write {
        Write { now, op }
    }

    #[test]
    fn set_get_cas_is_log_index() {
        let s = store();
        assert_eq!(s.apply(5, &w(100, set("a", "1", 0))), Reply::Stored { cas: 5, size: 1 });
        let hit = s.get(b"a", 100, true).unwrap();
        assert_eq!(&hit.item.value[..], b"1");
        assert_eq!(hit.item.cas, 5);
        assert_eq!(hit.item.flags, 7);
        assert!(!hit.fetched_before);
        assert!(s.get(b"a", 100, true).unwrap().fetched_before);
    }

    #[test]
    fn expiry_uses_replicated_time() {
        let s = store();
        s.apply(1, &w(100, set("a", "1", 10)));
        assert!(s.get(b"a", 105, true).is_some());
        assert!(s.get(b"a", 110, true).is_none());
        // add on an expired key succeeds once the replicated clock passes the exptime
        let add = Op::Store {
            mode: StoreMode::Add,
            key: Bytes::from_static(b"a"),
            value: Bytes::from_static(b"2"),
            flags: 0,
            exptime: 0,
            cas: None,
            invalidate: false,
        };
        assert_eq!(s.apply(2, &w(105, add.clone())), Reply::NotStored);
        assert_eq!(s.apply(3, &w(111, add)), Reply::Stored { cas: 3, size: 1 });
        // time never goes backwards even if a later entry carries an older timestamp
        s.apply(4, &w(50, Op::Tick));
        assert_eq!(s.apply_time(), 111);
    }

    #[test]
    fn negative_exptime_deletes() {
        let s = store();
        s.apply(1, &w(100, set("a", "1", 0)));
        assert_eq!(s.apply(2, &w(100, set("a", "2", -1))), Reply::Stored { cas: 2, size: 1 });
        assert!(s.get(b"a", 100, true).is_none());
        assert_eq!(s.stats().curr_items, 0);
    }

    #[test]
    fn cas_semantics() {
        let s = store();
        let cas = |c| Op::Store {
            mode: StoreMode::Set,
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v"),
            flags: 0,
            exptime: 0,
            cas: Some(c),
            invalidate: false,
        };
        assert_eq!(s.apply(1, &w(1, cas(1))), Reply::NotFound);
        s.apply(2, &w(1, set("k", "x", 0)));
        assert_eq!(s.apply(3, &w(1, cas(1))), Reply::Exists);
        assert_eq!(s.apply(4, &w(1, cas(2))), Reply::Stored { cas: 4, size: 1 });
    }

    #[test]
    fn append_prepend_keep_flags() {
        let s = store();
        s.apply(1, &w(1, set("k", "mid", 0)));
        let op = |mode, v: &'static str| Op::Store {
            mode,
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(v.as_bytes()),
            flags: 99,
            exptime: -1,
            cas: None,
            invalidate: false,
        };
        // The reported size is that of the combined value (`ms s` after append/prepend).
        assert_eq!(s.apply(2, &w(1, op(StoreMode::Append, ">"))), Reply::Stored { cas: 2, size: 4 });
        assert_eq!(s.apply(3, &w(1, op(StoreMode::Prepend, "<"))), Reply::Stored { cas: 3, size: 5 });
        let it = s.get(b"k", 1, false).unwrap().item;
        assert_eq!(&it.value[..], b"<mid>");
        assert_eq!(it.flags, 7);
    }

    #[test]
    fn incr_decr() {
        let s = store();
        s.apply(1, &w(1, set("n", "10", 0)));
        let arith = |incr, delta| Op::Arith {
            key: Bytes::from_static(b"n"),
            incr,
            delta,
            cas: None,
            vivify: None,
            exptime: None,
        };
        match s.apply(2, &w(1, arith(false, 1))) {
            Reply::Arith { value, item } => {
                assert_eq!(value, 9);
                assert_eq!(&item.value[..], b"9");
                assert_eq!(item.cas, 2);
            }
            r => panic!("{r:?}"),
        }
        assert!(matches!(s.apply(3, &w(1, arith(false, 100))), Reply::Arith { value: 0, .. }));
        s.apply(4, &w(1, set("n", "18446744073709551615", 0)));
        assert!(matches!(s.apply(5, &w(1, arith(true, 2))), Reply::Arith { value: 1, .. }));
        s.apply(6, &w(1, set("n", "abc", 0)));
        assert_eq!(s.apply(7, &w(1, arith(true, 1))), Reply::NonNumeric);
    }

    #[test]
    fn flush_immediate_and_delayed() {
        let s = store();
        s.apply(1, &w(100, set("a", "1", 0)));
        s.apply(2, &w(100, Op::Flush { delay: 0 }));
        assert!(s.get(b"a", 100, true).is_none());
        s.apply(3, &w(100, set("a", "2", 0)));
        assert!(s.get(b"a", 100, true).is_some());

        s.apply(4, &w(100, Op::Flush { delay: 10 }));
        assert!(s.get(b"a", 105, true).is_some());
        assert!(s.get(b"a", 110, true).is_none());
        // items stored after the deadline survive
        s.apply(5, &w(111, set("b", "1", 0)));
        assert!(s.get(b"b", 111, true).is_some());
        // a new delayed flush after the old one fired must not resurrect old items
        s.apply(6, &w(120, Op::Flush { delay: 100 }));
        assert!(s.get(b"a", 121, true).is_none());
        assert!(s.get(b"b", 121, true).is_some());
        // an absolute deadline in the past flushes immediately
        s.apply(7, &w(3_000_000, Op::Flush { delay: 2_600_000 }));
        assert!(s.get(b"b", 3_000_000, true).is_none());
    }

    #[test]
    fn sweep_reclaims_dead_items() {
        let s = store();
        s.apply(1, &w(100, set("a", "1", 5)));
        s.apply(2, &w(100, set("b", "1", 0)));
        assert_eq!(s.stats().curr_items, 2);
        s.apply(3, &w(200, Op::Tick));
        assert_eq!(s.sweep(0, s.stripe_count()), 1);
        assert_eq!(s.stats().curr_items, 1);
    }

    #[test]
    fn stale_and_win_tokens() {
        let s = store();
        s.apply(1, &w(100, set("a", "1", 0)));
        let inv = Op::Delete {
            key: Bytes::from_static(b"a"),
            cas: None,
            invalidate: true,
            exptime: Some(30),
            remove_value: false,
        };
        assert_eq!(s.apply(2, &w(100, inv)), Reply::Deleted);
        assert!(s.meta_get_needs_write(b"a", 100, None));
        let mg = Op::MetaGet { key: Bytes::from_static(b"a"), touch: None, vivify: None, recache: None };
        match s.apply(3, &w(100, mg.clone())) {
            Reply::Item { item, won, already_won } => {
                assert!(item.is_stale());
                assert!(won && !already_won);
            }
            r => panic!("{r:?}"),
        }
        assert!(matches!(s.apply(4, &w(100, mg)), Reply::Item { won: false, already_won: true, .. }));
        // vivify on miss
        let viv = Op::MetaGet { key: Bytes::from_static(b"z"), touch: None, vivify: Some(30), recache: None };
        assert!(matches!(s.apply(5, &w(100, viv)), Reply::Item { won: true, .. }));
    }

    #[test]
    fn image_roundtrip() {
        let s = store();
        s.apply(1, &w(100, set("a", "1", 0)));
        s.apply(2, &w(100, set("b", "2", 5)));
        s.apply(3, &w(200, Op::Tick));
        let img = s.image();
        assert_eq!(img.items.len(), 1);
        let t = store();
        t.reset(img.clocks);
        for (k, it) in img.items {
            t.load(k, it);
        }
        assert_eq!(t.applied_index(), 3);
        assert_eq!(&t.get(b"a", 0, false).unwrap().item.value[..], b"1");
    }

    /// A replica restored from a snapshot must answer the next `mg` exactly like a replica that
    /// applied the whole log: the win token was already handed out, so it reports `Z`, not `W`.
    #[test]
    fn win_token_survives_snapshot() {
        let s = store();
        s.apply(1, &w(100, set("a", "1", 0)));
        let inv = Op::Delete {
            key: Bytes::from_static(b"a"),
            cas: None,
            invalidate: true,
            exptime: Some(30),
            remove_value: false,
        };
        s.apply(2, &w(100, inv));
        let mg = Op::MetaGet { key: Bytes::from_static(b"a"), touch: None, vivify: None, recache: None };
        assert!(matches!(s.apply(3, &w(100, mg.clone())), Reply::Item { won: true, .. }));
        let viv = Op::MetaGet { key: Bytes::from_static(b"z"), touch: None, vivify: Some(30), recache: None };
        assert!(matches!(s.apply(4, &w(100, viv.clone())), Reply::Item { won: true, .. }));

        let restored = store();
        let img = s.image();
        restored.reset(img.clocks);
        for (k, it) in img.items {
            restored.load(k, it);
        }
        for (index, op) in [(5, mg), (6, viv)] {
            let want = s.apply(index, &w(100, op.clone()));
            assert!(matches!(want, Reply::Item { won: false, already_won: true, .. }), "{want:?}");
            assert_eq!(restored.apply(index, &w(100, op)), want);
        }
    }
}
