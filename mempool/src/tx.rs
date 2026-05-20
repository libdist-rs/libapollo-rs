//! Mempool transaction trait and dedup identifiers.
//!
//! Defined inside libapollo-mempool (not in `types`) because `types`
//! already depends on this crate (for `CachedBatch`), and a back-
//! reference would create a Cargo dependency cycle.
//!
//! Consensus-level transaction types (e.g. `types::Transaction`)
//! implement `MempoolTx` to opt into the nonce-keyed `Txpool`.

use libcrypto::hash::Hash;
use net_common::Message;
use serde::Serialize;

/// Identifies a client. Distinct family from `Replica` so a multi-
/// client deployment can address clients independently of consensus
/// replicas. Matches `config::ClientId` and `types::Replica` width
/// for compact dedup keys.
pub type ClientId = u16;

/// Replica/leader id, kept local to libapollo-mempool so this crate
/// doesn't need a back-reference to `types`. Same width as
/// `types::Replica` and `config::ClientId`.
pub type Replica = u16;

/// Contract a tx type must satisfy to flow through the keyed
/// `Txpool`. `client_id() + nonce()` form the dedup key the pool uses
/// for Mineable/InFlight bookkeeping and replay protection.
pub trait MempoolTx: Serialize + Message + Clone + Send + Sync + 'static {
    /// The client that originated this transaction.
    fn client_id(&self) -> ClientId;

    /// Per-client monotonically-increasing sequence number. The pool
    /// rejects any nonce ≤ that client's `high_committed_nonce` watermark.
    fn nonce(&self) -> u64;

    /// Convenience: SHA-256 over the bincode-serialised tx. Default
    /// implementation suffices for any `Serialize` type; protocols can
    /// override if they keep a precomputed hash on the tx itself.
    fn tx_hash(&self) -> Hash<Self> {
        Hash::<Self>::ser_and_hash(self)
    }
}
