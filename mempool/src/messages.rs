//! Mempool control messages.
//!
//! Two enums live here:
//!
//! * `ConsensusMempoolMsg<Id, Round, Tx>` — legacy, used by synchs and
//!   optsync. Kept as no-op variants so the existing call sites still
//!   compile; the legacy `Mempool::spawn` drains them on a background
//!   task.
//!
//! * `BatcherConsensusMsg<Round, Tx>` — the new nonce-keyed contract,
//!   ported from leto-rs's `rr_batcher.rs`. Used by apollo and artemis.
//!   `Round` is `u64` for both protocols.

use crate::batch::{BatchHash, CachedBatch};
use crate::tx::Replica;
use libcrypto::hash::Hash;
use net_common::Message;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub enum ConsensusMempoolMsg<Id, Round, Tx> {
    /// Round advanced; mempool may garbage-collect any state that was
    /// round-scoped (we currently have none).
    End(Round),
    /// Consensus observed a referenced batch hash it doesn't have.
    UnknownBatch(Id, Vec<BatchHash<Tx>>),
}

/// Messages from the consensus engine to the keyed batcher.
///
/// Carrying `Arc<CachedBatch<Tx>>` (instead of a bare `Vec<Tx>`) lets
/// the consumer (`Txpool`) iterate `&batch.payload` directly and lets
/// the leader's own-batch loopback path keep the same `Arc` across the
/// `Proposed` round-trip — no re-allocation or rehash.
#[derive(Debug)]
pub enum BatcherConsensusMsg<Tx> {
    /// Entering a new round; the batcher may now propose if it is the
    /// leader. `round` is the consensus round/height that will be
    /// proposed in.
    NewRound { leader: Replica, round: u64 },
    /// A proposal carrying `batch` was admitted at `round`. Idempotent
    /// if (client, nonce) pairs are already InFlight.
    Proposed {
        batch: Arc<CachedBatch<Tx>>,
        round: u64,
    },
    /// `batch` committed at `round` on the canonical chain. Advances
    /// `high_committed_nonce` per client and GCs Mineable/InFlight.
    Committed {
        batch: Arc<CachedBatch<Tx>>,
        round: u64,
    },
    /// Chain switched away from `rounds`; orphan those InFlight
    /// entries. Subsumed orphans (nonce ≤ hi[c]) are dropped.
    Rollback { rounds: Vec<u64> },
}

/// Wire-level client ↔ server message used by the keyed mempool path
/// (apollo / artemis). Mirrors leto-rs's `ClientMsg`.
///
/// `reply_to` is the client's confirmation listener address (a
/// `TcpReceiver<ClientMsg<Tx>>` bound to a known port). The server
/// records `tx_hash → reply_to` for each tx the batch ingests, and
/// sends `Confirmation(tx_hash)` back at commit time. Per-tx (not per-
/// batch) so that batches reshuffled by the server-side `Txpool`
/// don't lose the back-channel.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ClientMsg<Tx> {
    /// Single-tx submission.
    NewTx {
        tx: Tx,
        reply_to: std::net::SocketAddr,
    },
    /// Batch submission. The server splits the batch into individual
    /// txs for the Txpool; client-side burst batching reduces TCP
    /// frame overhead but doesn't bypass server-side scheduling.
    NewBatch {
        batch: Vec<Tx>,
        reply_to: std::net::SocketAddr,
    },
    /// Server → client: this tx has been committed on the canonical
    /// chain. Sent over plaintext TCP from the confirmation router
    /// directly to the client's `reply_to` address.
    Confirmation(Hash<Tx>),
}

impl<Tx> Message for ClientMsg<Tx>
where
    Self: serde::de::DeserializeOwned,
{
    type DeserializationError = Box<bincode::ErrorKind>;
    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeserializationError> {
        bincode::deserialize(bytes)
    }
}
