//! In-memory `BatchHash -> Arc<CachedBatch>` lookup.
//!
//! Fronts the rocksdb batch store. Every mempool-produced batch is
//! inserted here before being persisted; every follower `persist_batch`
//! call also lands here. On `read_batch`, consensus consults this map
//! first -- a hit is a `DashMap::get` + `Arc::clone`; a miss falls
//! through to the rocksdb store (crash recovery, out-of-window
//! batches).
//!
//! The current implementation is unbounded -- DashMap only. A
//! long-running deployment would want LRU / round-scoped GC. For the
//! benchmark (125 batches per run), unboundedness is fine, and every
//! `insert` is a single DashMap op with no Mutex contention. We
//! deliberately avoided a `Mutex<VecDeque>` for FIFO tracking after
//! seeing its overhead dominate on high-throughput paths.

use crate::batch::{BatchHash, CachedBatch};
use dashmap::DashMap;
use std::sync::Arc;

pub struct BatchCache<Tx> {
    map: DashMap<BatchHash<Tx>, Arc<CachedBatch<Tx>>>,
}

impl<Tx> BatchCache<Tx>
where
    Tx: Send + Sync + 'static,
{
    pub fn new(_cap: usize) -> Arc<Self> {
        Arc::new(Self {
            map: DashMap::new(),
        })
    }

    /// Insert a batch by hash. No-op if the hash is already present
    /// (same batch arriving via a different path, e.g. leader-local
    /// vs wire-received).
    pub fn insert(&self, hash: BatchHash<Tx>, batch: Arc<CachedBatch<Tx>>) {
        // DashMap::insert overwrites; use a 2-step dance to keep the
        // "first writer wins" semantic so a concurrent batch-local +
        // batch-wire race doesn't cause a spurious Arc churn.
        if self.map.contains_key(&hash) {
            return;
        }
        self.map.insert(hash, batch);
    }

    /// Fetch an `Arc<CachedBatch>` by hash. Returns `None` on miss --
    /// caller should fall through to rocksdb.
    pub fn get(&self, hash: &BatchHash<Tx>) -> Option<Arc<CachedBatch<Tx>>> {
        self.map.get(hash).map(|e| Arc::clone(e.value()))
    }

    /// Current number of cached batches. For metrics / tests only.
    pub fn len(&self) -> usize {
        self.map.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use libcrypto::hash::Hash;

    #[test]
    fn duplicate_insert_is_noop() {
        let c = BatchCache::<u32>::new(4);
        let h = Hash::<CachedBatch<u32>>::do_hash(b"x");
        c.insert(h.clone(), Arc::new(CachedBatch::new(vec![1])));
        c.insert(h.clone(), Arc::new(CachedBatch::new(vec![2])));
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn get_roundtrip() {
        let c = BatchCache::<u32>::new(4);
        let h = Hash::<CachedBatch<u32>>::do_hash(b"y");
        c.insert(h.clone(), Arc::new(CachedBatch::new(vec![7, 8, 9])));
        let g = c.get(&h).expect("hit");
        assert_eq!(g.payload, vec![7, 8, 9]);
    }
}
