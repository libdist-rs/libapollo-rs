//! `CachedBatch<Tx>` -- the core batch type.
//!
//! The shape is `Vec<Tx>` on the wire, identical to the byte-layout of
//! libmempool-rs's `Batch { payload: Vec<Tx> }`, but with two cached
//! fields populated on the side:
//!
//! * `hash` -- the `BatchHash` (SHA256 of the bincode-serialized payload).
//!   Populated eagerly during `Deserialize` (so `check_batch_hash` on
//!   a follower is a free `OnceLock` compare), and lazily on the leader
//!   via `hash()` when the proposal is built.
//! * `tx_hashes` -- per-tx hashes, populated by the intake path on the
//!   leader (each tx arrives off the wire, gets hashed once, and the
//!   hash rides alongside the `Tx` into the batcher). Followers fill
//!   this lazily on first commit-time hydrate.
//!
//! Wire format: bincode emits a single-field struct / newtype / bare
//! `Vec<T>` identically (length prefix + elements), so all three round-
//! trip bit-identical bytes. The custom `Serialize`/`Deserialize`
//! implementations delegate to `Vec<Tx>` directly to make that explicit.

use libcrypto::hash::Hash;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::{Arc, OnceLock};

/// The content-hash of a `CachedBatch<Tx>`, produced by SHA256 over
/// `bincode::serialize(&cached_batch)` -- which, because of the
/// `Serialize` impl below, is identical to
/// `bincode::serialize(&cached_batch.payload)`.
pub type BatchHash<Tx> = Hash<CachedBatch<Tx>>;

/// A batch of transactions plus lazily-populated content hashes.
///
/// The `OnceLock` fields are populated at most once across the lifetime
/// of the batch; subsequent calls are free reads.
pub struct CachedBatch<Tx> {
    pub payload: Vec<Tx>,
    hash: OnceLock<BatchHash<Tx>>,
    tx_hashes: OnceLock<Arc<[Hash<Tx>]>>,
}

impl<Tx> CachedBatch<Tx> {
    /// Empty cache, no precomputed hashes.
    pub fn new(payload: Vec<Tx>) -> Self {
        Self {
            payload,
            hash: OnceLock::new(),
            tx_hashes: OnceLock::new(),
        }
    }

    /// Build a batch with tx hashes already known (leader path -- the
    /// intake receiver hashes each tx and the batcher collects them).
    pub fn new_with_tx_hashes(payload: Vec<Tx>, tx_hashes: Arc<[Hash<Tx>]>) -> Self {
        debug_assert_eq!(payload.len(), tx_hashes.len(), "tx count / hash count mismatch");
        let b = Self::new(payload);
        // OnceLock::set returns Err if already set -- impossible here.
        let _ = b.tx_hashes.set(tx_hashes);
        b
    }
}

impl<Tx: Serialize> CachedBatch<Tx> {
    /// Get the batch hash, computing + caching on first call.
    pub fn hash(&self) -> BatchHash<Tx> {
        self.hash
            .get_or_init(|| {
                // Hash the same bytes the wire format carries. See
                // `Serialize` impl -- this matches `&self.payload`
                // serialized bytes exactly.
                let bytes = bincode::serialize(self).expect("CachedBatch serialize");
                Hash::<CachedBatch<Tx>>::do_hash(&bytes)
            })
            .clone()
    }

    /// Get per-tx hashes, computing + caching on first call. This is
    /// the fallback path for followers that received a batch off the
    /// wire without tx-hash precomputation -- leaders populate the
    /// cache at intake via `new_with_tx_hashes`, so `tx_hashes()` is
    /// free on the leader's propose path.
    pub fn tx_hashes(&self) -> Arc<[Hash<Tx>]> {
        self.tx_hashes
            .get_or_init(|| {
                let v: Vec<Hash<Tx>> = self
                    .payload
                    .iter()
                    .map(|tx| Hash::<Tx>::ser_and_hash(tx))
                    .collect();
                Arc::from(v.into_boxed_slice())
            })
            .clone()
    }
}

impl<Tx: Clone> Clone for CachedBatch<Tx> {
    fn clone(&self) -> Self {
        // Propagate any cached values so the clone doesn't re-compute.
        let out = Self::new(self.payload.clone());
        if let Some(h) = self.hash.get() {
            let _ = out.hash.set(h.clone());
        }
        if let Some(th) = self.tx_hashes.get() {
            let _ = out.tx_hashes.set(th.clone());
        }
        out
    }
}

impl<Tx: std::fmt::Debug> std::fmt::Debug for CachedBatch<Tx> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedBatch")
            .field("payload_len", &self.payload.len())
            .field("hash_cached", &self.hash.get().is_some())
            .field("tx_hashes_cached", &self.tx_hashes.get().is_some())
            .finish()
    }
}

// Wire format = `bincode::serialize(&payload)`. Bincode emits a
// one-field struct, a newtype, and a bare Vec identically, so this
// matches libmempool-rs's `Batch { payload }` layout bit-for-bit.
impl<Tx: Serialize> Serialize for CachedBatch<Tx> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.payload.serialize(s)
    }
}

// Deserialize populates the `hash` cache from the just-received bytes,
// so downstream `check_batch_hash` is a free `OnceLock::get()` compare.
// `tx_hashes` is left empty -- filled lazily if/when hydrate fires.
impl<'de, Tx> Deserialize<'de> for CachedBatch<Tx>
where
    Tx: Serialize + Deserialize<'de>,
{
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let payload = Vec::<Tx>::deserialize(d)?;
        let batch = CachedBatch::new(payload);
        // Prepopulate hash so followers' `check_batch_hash` is free.
        let _ = batch.hash();
        Ok(batch)
    }
}

impl<Tx> net_common::Message for CachedBatch<Tx>
where
    Tx: Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type DeserializationError = bincode::Error;

    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeserializationError> {
        bincode::deserialize(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
    struct Dummy(u64);

    #[test]
    fn wire_format_matches_bare_vec() {
        let v = vec![Dummy(1), Dummy(2), Dummy(3)];
        let batch = CachedBatch::new(v.clone());
        let batch_bytes = bincode::serialize(&batch).unwrap();
        let vec_bytes = bincode::serialize(&v).unwrap();
        assert_eq!(batch_bytes, vec_bytes);
    }

    #[test]
    fn roundtrip_preserves_payload_and_populates_hash() {
        let v = vec![Dummy(1), Dummy(2), Dummy(3)];
        let batch = CachedBatch::new(v.clone());
        let bytes = bincode::serialize(&batch).unwrap();
        let got: CachedBatch<Dummy> = bincode::deserialize(&bytes).unwrap();
        assert_eq!(got.payload, v);
        // Deserialize must have populated the hash.
        assert!(got.hash.get().is_some());
        // And it must match what the sender would compute.
        assert_eq!(got.hash(), batch.hash());
    }

    #[test]
    fn tx_hashes_cached() {
        let v = vec![Dummy(1), Dummy(2)];
        let batch = CachedBatch::new(v);
        let a = batch.tx_hashes();
        let b = batch.tx_hashes();
        // Both handles point at the same Arc.
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn new_with_tx_hashes_skips_recompute() {
        let v = vec![Dummy(1), Dummy(2)];
        let precomputed: Arc<[Hash<Dummy>]> = Arc::from(
            v.iter()
                .map(|t| Hash::<Dummy>::ser_and_hash(t))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        );
        let batch = CachedBatch::new_with_tx_hashes(v, precomputed.clone());
        assert!(Arc::ptr_eq(&batch.tx_hashes(), &precomputed));
    }
}
