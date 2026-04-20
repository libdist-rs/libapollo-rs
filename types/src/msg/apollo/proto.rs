use libcrypto::hash::Hash;
use serde::{Deserialize, Serialize};

use super::{Block, Propose, Replica, Vote};
use crate::WireReady;

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

impl WireReady for ProtocolMsg {
    fn from_bytes(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).expect("failed to decode the protocol message")
    }

    fn init(self) -> Self {
        self
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to serialize protocol message")
    }
}
