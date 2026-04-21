use libcrypto::hash::Hash;
use libmempool::CachedBatch;
use net_common::Message;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::{Block, Propose, Replica, Transaction, Vote};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[repr(u8)]
pub enum ProtocolMsg {
    /// Leader's new proposal: propose metadata, the block (which
    /// references the batch by hash), and the batch itself so
    /// followers can persist it without a separate sync round-trip.
    /// Sent only by the leader, so `sig.origin` identifies the sender.
    ///
    /// The batch rides as `Arc<CachedBatch>` to keep in-memory clones
    /// cheap (refcount bump, no deep copy). serde serializes/
    /// deserializes `Arc<T>` transparently as `T`, so the wire format
    /// is identical to an inline `CachedBatch`.
    NewProposal(Propose, Block, Arc<CachedBatch<Transaction>>),

    /// Non-leader forwarding a proposal to the next leader. The block
    /// is looked up from storage on the receiving side (or requested
    /// via `Request` if missing). Carries `from` so the recipient can
    /// request missing blocks from the forwarder (who provably has
    /// them). Relays intentionally exclude the batch -- hash-only.
    Relay(Replica, Propose),

    /// Ask a peer to resend the block with the given hash. Carries the
    /// requester's id so the responder knows where to send `Response`.
    Request(Replica, u64, Hash<Block>),
    /// Reply to a `Request`: propose metadata, block, and batch. Carries
    /// `from` so the requester can follow up for missing parents
    /// against the responder (who just satisfied this request).
    Response(Replica, u64, Propose, Block, Arc<CachedBatch<Transaction>>),

    /// Blame a misbehaving leader.
    Blame(Vote),
}

impl Message for ProtocolMsg {
    type DeserializationError = bincode::Error;

    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeserializationError> {
        bincode::deserialize(bytes)
    }
}
