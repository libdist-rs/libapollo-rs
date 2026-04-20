use libcrypto::hash::Hash;
use net_common::Message;
use serde::{Deserialize, Serialize};

use super::{Block, Propose, Replica, Vote};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[repr(u8)]
pub enum ProtocolMsg {
    /// Leader's new proposal: propose metadata + the proposed block.
    /// Sent only by the leader, so `sig.origin` identifies the sender.
    NewProposal(Propose, Block),

    /// Non-leader forwarding a proposal to the next leader. The block is
    /// looked up from storage on the receiving side (or requested via
    /// `Request` if missing). Carries `from` so the recipient can request
    /// missing blocks from the forwarder (who provably has them).
    Relay(Replica, Propose),

    /// Ask a peer to resend the block with the given hash. Carries the
    /// requester's id so the responder knows where to send `Response`.
    Request(Replica, u64, Hash<Block>),
    /// Reply to a `Request`: propose metadata + block. Carries `from` so
    /// the requester can follow up for missing parents against the
    /// responder (who just satisfied this request).
    Response(Replica, u64, Propose, Block),

    /// Blame a misbehaving leader.
    Blame(Vote),
}

impl Message for ProtocolMsg {
    type DeserializationError = bincode::Error;

    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeserializationError> {
        bincode::deserialize(bytes)
    }
}
