//! Nonce-keyed transaction pool — ported from leto-rs
//! (`consensus/src/server/tx_pool.rs`).
//!
//! Per-tx state machine:
//!   Unknown   – no storage
//!   Mineable  – in `mineable` BTreeMap, eligible for batching
//!   InFlight  – in `inflight` HashMap, tagged with the round it was
//!               proposed
//!   Replayed  – dropped at `add_tx`
//!
//! Replicated state: `high_committed_nonce[client]` — max nonce
//! committed on the canonical chain for that client. Bounded by
//! O(#clients), not O(#committed txs).
//!
//! The pool is the single shared place where the four protocol events
//! (add_tx / admit_proposal / commit / rollback) mutate transaction
//! state. The `RRBatcher` task owns it and serialises access.

use crate::batch::CachedBatch;
use fnv::{FnvHashMap, FnvHashSet};
use std::{collections::BTreeMap, time::Duration};
use tokio::time::Interval;
use crate::tx::{ClientId, MempoolTx};

type Nonce = u64;
type TxKey = (ClientId, Nonce);

struct InflightEntry<Tx> {
    #[allow(dead_code)]
    tx: Tx,
    size: usize,
    round: u64,
}

impl<Tx> std::fmt::Debug for InflightEntry<Tx> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "InflightEntry {{ size: {}, round: {} }}",
            self.size, self.round
        )
    }
}

#[derive(Debug)]
pub struct Txpool<Tx> {
    /// Mineable transactions sorted by (client_id, nonce).
    /// `BTreeMap` iteration order is the key order, giving deterministic,
    /// fair batches across clients.
    mineable: BTreeMap<TxKey, (Tx, usize)>,
    mineable_bytes: usize,

    /// In-flight transactions indexed by (client_id, nonce).
    inflight: FnvHashMap<TxKey, InflightEntry<Tx>>,
    /// Secondary index: round → set of (client_id, nonce) admitted at
    /// that round. Used by `rollback` for O(orphan-set) cleanup.
    inflight_by_round: FnvHashMap<u64, FnvHashSet<TxKey>>,

    /// Replay-protection high-water mark per client. Updated when a
    /// `commit` is signalled by consensus.
    high_committed_nonce: FnvHashMap<ClientId, Nonce>,

    batch_size: usize,
    timer: Interval,
}

impl<Tx> Txpool<Tx>
where
    Tx: MempoolTx,
{
    pub fn new(batch_size: usize, batch_timeout: Duration) -> Self {
        Self {
            mineable: BTreeMap::new(),
            mineable_bytes: 0,
            inflight: FnvHashMap::default(),
            inflight_by_round: FnvHashMap::default(),
            high_committed_nonce: FnvHashMap::default(),
            batch_size,
            timer: tokio::time::interval(batch_timeout),
        }
    }

    /// Add a client-submitted transaction.
    ///
    /// Dropped silently if:
    ///   - nonce ≤ high_committed_nonce[client] (replay / late RTO / Byzantine)
    ///   - (client, nonce) is already InFlight (proposal-direct LAN race)
    ///
    /// Otherwise inserted into Mineable; last-write-wins on equivocation.
    pub fn add_tx(&mut self, tx: Tx, size: usize) {
        let (c, n) = (tx.client_id(), tx.nonce());
        if n <= *self.high_committed_nonce.get(&c).unwrap_or(&0) {
            return;
        }
        if self.inflight.contains_key(&(c, n)) {
            return;
        }
        if let Some((_, old_sz)) = self.mineable.insert((c, n), (tx, size)) {
            self.mineable_bytes -= old_sz;
        }
        self.mineable_bytes += size;

        debug_assert!(self.key_sets_disjoint());
    }

    /// Mark every tx in `batch` as InFlight at `round`.
    pub fn admit_proposal(&mut self, batch: &CachedBatch<Tx>, round: u64) {
        let by_round = self.inflight_by_round.entry(round).or_default();
        for tx in &batch.payload {
            let (c, n) = (tx.client_id(), tx.nonce());
            if n <= *self.high_committed_nonce.get(&c).unwrap_or(&0) {
                continue;
            }
            if self.inflight.contains_key(&(c, n)) {
                continue;
            }
            let size = if let Some((_, sz)) = self.mineable.remove(&(c, n)) {
                self.mineable_bytes -= sz;
                sz
            } else {
                bincode::serialized_size(tx).unwrap_or(0) as usize
            };
            self.inflight.insert(
                (c, n),
                InflightEntry {
                    tx: tx.clone(),
                    size,
                    round,
                },
            );
            by_round.insert((c, n));
        }

        debug_assert!(self.key_sets_disjoint());
    }

    /// Pop up to `batch_size` bytes from Mineable, promoting them to
    /// InFlight(round). Called by the proposer task only.
    pub fn make_batch(&mut self, round: u64) -> Vec<Tx> {
        let mut payload = Vec::new();
        let mut batch_bytes = 0usize;
        let by_round = self.inflight_by_round.entry(round).or_default();

        while batch_bytes < self.batch_size {
            let key = match self.mineable.keys().next().copied() {
                Some(k) => k,
                None => break,
            };
            let (c, n) = key;
            let (tx, size) = self.mineable.remove(&key).unwrap();
            self.mineable_bytes -= size;
            batch_bytes += size;
            payload.push(tx.clone());
            self.inflight
                .insert((c, n), InflightEntry { tx, size, round });
            by_round.insert((c, n));
        }

        self.reset_timer();
        debug_assert!(self.key_sets_disjoint());
        payload
    }

    /// Advance `high_committed_nonce` for every tx in the committed
    /// batch and GC stale entries from Mineable / InFlight.
    pub fn commit(&mut self, batch: &CachedBatch<Tx>, round: u64) {
        let mut touched: FnvHashSet<ClientId> = FnvHashSet::default();
        for tx in &batch.payload {
            let (c, n) = (tx.client_id(), tx.nonce());
            touched.insert(c);

            if let Some(e) = self.inflight.remove(&(c, n)) {
                if let Some(set) = self.inflight_by_round.get_mut(&e.round) {
                    set.remove(&(c, n));
                }
            }
            if let Some((_, sz)) = self.mineable.remove(&(c, n)) {
                self.mineable_bytes -= sz;
            }

            let cur = self.high_committed_nonce.entry(c).or_insert(0);
            if n > *cur {
                *cur = n;
            }
        }

        for c in &touched {
            let hi = *self.high_committed_nonce.get(c).unwrap();
            let stale: Vec<TxKey> = self
                .mineable
                .range((*c, 0)..=(*c, hi))
                .map(|(k, _)| *k)
                .collect();
            for k in stale {
                if let Some((_, sz)) = self.mineable.remove(&k) {
                    self.mineable_bytes -= sz;
                }
            }
        }

        if let Some(set) = self.inflight_by_round.get(&round) {
            if set.is_empty() {
                self.inflight_by_round.remove(&round);
            }
        }

        debug_assert!(self.key_sets_disjoint());
    }

    /// Return orphaned InFlight entries from `rounds` to Mineable
    /// (chain-switch). Subsumed orphans (nonce ≤ hi[c]) are dropped.
    pub fn rollback(&mut self, rounds: &[u64]) {
        for r in rounds {
            if let Some(set) = self.inflight_by_round.remove(r) {
                for (c, n) in set {
                    let hi = *self.high_committed_nonce.get(&c).unwrap_or(&0);
                    if n <= hi {
                        self.inflight.remove(&(c, n));
                        continue;
                    }
                    if let Some(e) = self.inflight.remove(&(c, n)) {
                        self.mineable.insert((c, n), (e.tx, e.size));
                        self.mineable_bytes += e.size;
                    }
                }
            }
        }

        debug_assert!(self.key_sets_disjoint());
    }

    pub fn reset_timer(&mut self) {
        self.timer.reset();
    }

    /// Are there enough buffered bytes to fill a batch?
    pub fn ready(&self) -> bool {
        self.mineable_bytes > self.batch_size
    }

    /// Resolves when the batch-timeout interval ticks.
    pub async fn tick_timer(&mut self) {
        self.timer.tick().await;
    }

    #[allow(dead_code)]
    fn key_sets_disjoint(&self) -> bool {
        for key in self.mineable.keys() {
            if self.inflight.contains_key(key) {
                return false;
            }
        }
        true
    }
}

// ---------------------------------------------------------------------------
// Unit tests — race traces from leto-rs's tx_pool.rs, kept verbatim so the
// invariants are co-located with the type they verify.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use net_common::Message;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestTx {
        client: ClientId,
        nonce: u64,
        payload: u8,
    }

    impl TestTx {
        fn new(client: ClientId, nonce: u64) -> Self {
            Self {
                client,
                nonce,
                payload: 0,
            }
        }
    }

    impl Message for TestTx {
        type DeserializationError = Box<bincode::ErrorKind>;
        fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeserializationError> {
            bincode::deserialize(bytes)
        }
    }

    impl MempoolTx for TestTx {
        fn client_id(&self) -> ClientId {
            self.client
        }
        fn nonce(&self) -> u64 {
            self.nonce
        }
    }

    fn pool() -> Txpool<TestTx> {
        Txpool::new(1024 * 1024, Duration::from_secs(60))
    }

    fn batch(txs: Vec<TestTx>) -> CachedBatch<TestTx> {
        CachedBatch::new(txs)
    }

    #[tokio::test]
    async fn trace1_common_case() {
        let mut p = pool();
        let tx = TestTx::new(1, 5);
        p.add_tx(tx.clone(), 10);
        assert!(p.mineable.contains_key(&(1, 5)));
        assert_eq!(p.mineable_bytes, 10);

        p.admit_proposal(&batch(vec![tx.clone()]), 1);
        assert!(!p.mineable.contains_key(&(1, 5)));
        assert!(p.inflight.contains_key(&(1, 5)));
        assert_eq!(p.inflight_by_round[&1].len(), 1);

        p.commit(&batch(vec![tx.clone()]), 1);
        assert!(!p.inflight.contains_key(&(1, 5)));
        assert_eq!(*p.high_committed_nonce.get(&1).unwrap(), 5);
    }

    #[tokio::test]
    async fn trace2_proposal_before_client() {
        let mut p = pool();
        let tx = TestTx::new(1, 5);

        p.admit_proposal(&batch(vec![tx.clone()]), 1);
        assert!(!p.mineable.contains_key(&(1, 5)));
        assert!(p.inflight.contains_key(&(1, 5)));

        p.add_tx(tx.clone(), 10);
        assert!(!p.mineable.contains_key(&(1, 5)));
        assert!(p.inflight.contains_key(&(1, 5)));
        assert_eq!(p.mineable_bytes, 0);
    }

    #[tokio::test]
    async fn trace3_late_client_after_commit() {
        let mut p = pool();
        let tx = TestTx::new(1, 5);

        p.admit_proposal(&batch(vec![tx.clone()]), 1);
        p.commit(&batch(vec![tx.clone()]), 1);
        assert_eq!(*p.high_committed_nonce.get(&1).unwrap(), 5);

        p.add_tx(tx.clone(), 10);
        assert!(!p.mineable.contains_key(&(1, 5)));
        assert_eq!(p.mineable_bytes, 0);
    }

    #[tokio::test]
    async fn trace4_chain_switch_rollback() {
        let mut p = pool();
        let tx = TestTx::new(1, 5);
        p.add_tx(tx.clone(), 10);

        let _payload = p.make_batch(1);
        assert!(p.inflight.contains_key(&(1, 5)));
        assert!(!p.mineable.contains_key(&(1, 5)));

        p.rollback(&[1]);
        assert!(p.mineable.contains_key(&(1, 5)));
        assert!(!p.inflight.contains_key(&(1, 5)));
        assert_eq!(p.mineable_bytes, 10);
    }

    #[tokio::test]
    async fn trace5_different_proposal_at_same_round() {
        let mut p = pool();
        let tx5 = TestTx::new(1, 5);
        let tx7 = TestTx::new(1, 7);

        p.add_tx(tx5.clone(), 10);
        p.admit_proposal(&batch(vec![tx5.clone()]), 2);
        assert!(p.inflight.contains_key(&(1, 5)));

        p.commit(&batch(vec![tx7.clone()]), 2);
        assert_eq!(*p.high_committed_nonce.get(&1).unwrap(), 7);
        assert!(!p.mineable.contains_key(&(1, 5)));

        p.rollback(&[2]);
        assert!(!p.inflight.contains_key(&(1, 5)));
        assert!(!p.mineable.contains_key(&(1, 5)));
    }

    #[tokio::test]
    async fn trace6_byzantine_replay() {
        let mut p = pool();
        let tx10 = TestTx::new(1, 10);
        p.add_tx(tx10.clone(), 10);
        p.admit_proposal(&batch(vec![tx10.clone()]), 1);
        p.commit(&batch(vec![tx10.clone()]), 1);
        assert_eq!(*p.high_committed_nonce.get(&1).unwrap(), 10);

        let tx3 = TestTx::new(1, 3);
        p.add_tx(tx3, 10);
        assert!(!p.mineable.contains_key(&(1, 3)));
        assert_eq!(p.mineable_bytes, 0);
    }

    #[tokio::test]
    async fn trace8_proposer_loopback() {
        let mut p = pool();
        let tx = TestTx::new(1, 5);
        p.add_tx(tx.clone(), 10);

        let payload = p.make_batch(1);
        assert_eq!(payload.len(), 1);
        assert!(p.inflight.contains_key(&(1, 5)));

        p.admit_proposal(&batch(payload), 1);
        assert!(p.inflight.contains_key(&(1, 5)));
        assert!(!p.mineable.contains_key(&(1, 5)));
        assert_eq!(p.inflight_by_round[&1].len(), 1);
    }

    #[tokio::test]
    async fn trace9_equivocation_last_write_wins() {
        let mut p = pool();
        let tx_a = TestTx {
            client: 1,
            nonce: 5,
            payload: 0xAA,
        };
        let tx_b = TestTx {
            client: 1,
            nonce: 5,
            payload: 0xBB,
        };

        p.add_tx(tx_a.clone(), 10);
        assert_eq!(p.mineable_bytes, 10);

        p.add_tx(tx_b.clone(), 10);
        assert_eq!(p.mineable.len(), 1);
        assert_eq!(p.mineable_bytes, 10);

        let stored = &p.mineable[&(1, 5)].0;
        assert_eq!(stored.payload, 0xBB);
    }

    #[tokio::test]
    async fn gc_sweeps_stale_mineable() {
        let mut p = pool();
        for n in 1u64..=3 {
            p.add_tx(TestTx::new(1, n), 10);
        }
        assert_eq!(p.mineable.len(), 3);
        assert_eq!(p.mineable_bytes, 30);

        p.commit(&batch(vec![TestTx::new(1, 3)]), 1);
        assert!(!p.mineable.contains_key(&(1, 1)));
        assert!(!p.mineable.contains_key(&(1, 2)));
        assert!(!p.mineable.contains_key(&(1, 3)));
        assert_eq!(p.mineable_bytes, 0);
        assert_eq!(*p.high_committed_nonce.get(&1).unwrap(), 3);
    }

    #[tokio::test]
    async fn rollback_subsumed_orphan_dropped() {
        let tx5 = TestTx::new(1, 5);
        let tx7 = TestTx::new(1, 7);

        let mut p2 = pool();
        p2.add_tx(tx5.clone(), 10);
        p2.admit_proposal(&batch(vec![tx5.clone()]), 2);
        p2.commit(&batch(vec![tx7.clone()]), 3);
        p2.rollback(&[2]);
        assert!(!p2.mineable.contains_key(&(1, 5)));
        assert!(!p2.inflight.contains_key(&(1, 5)));
    }
}
