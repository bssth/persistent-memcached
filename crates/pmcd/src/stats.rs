//! Node-local protocol counters, reported by `stats` like memcached does.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::time::Instant;

macro_rules! counters {
    ($($name:ident),* $(,)?) => {
        #[derive(Default)]
        pub struct Counters { $(pub $name: AtomicU64,)* }

        impl Counters {
            /// `(name, value)` pairs in declaration order.
            pub fn snapshot(&self) -> Vec<(&'static str, u64)> {
                vec![$((stringify!($name), self.$name.load(Relaxed)),)*]
            }
        }
    };
}

counters!(
    curr_connections,
    total_connections,
    cmd_get,
    cmd_set,
    cmd_flush,
    cmd_touch,
    cmd_meta,
    get_hits,
    get_misses,
    get_expired,
    get_flushed,
    delete_misses,
    delete_hits,
    incr_misses,
    incr_hits,
    decr_misses,
    decr_hits,
    cas_misses,
    cas_hits,
    cas_badval,
    touch_hits,
    touch_misses,
    store_too_large,
    store_no_memory,
    bytes_read,
    bytes_written,
);

pub struct Stats {
    pub started: Instant,
    pub counters: Counters,
}

impl Default for Stats {
    fn default() -> Self {
        Self { started: Instant::now(), counters: Counters::default() }
    }
}

pub fn inc(c: &AtomicU64) {
    c.fetch_add(1, Relaxed);
}

pub fn add(c: &AtomicU64, n: u64) {
    c.fetch_add(n, Relaxed);
}

pub fn dec(c: &AtomicU64) {
    c.fetch_sub(1, Relaxed);
}
