use libcrypto::hash::Hash;
use serde::{Deserialize, Serialize};

use super::{Block, Replica, UCRVote, Vote};
use crate::WireReady;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[repr(u8)]
pub enum ProtocolMsg {
    NewBlock(Block),

    /// Round leader's vote. Sent directly by the round leader, so
    /// `sig.origin` identifies the sender.
    UCRVote(UCRVote),

    /// Forward a vote from a non-leader. Carries `from` so the recipient
    /// can request missing blocks from the forwarder.
    Relay(Replica, UCRVote),

    Blame(Vote),

    /// Ask a peer to resend the block with the given content hash.
    /// Carries the requester's id so the responder can reply.
    Request(Replica, u64, Hash<Block>),
    /// Reply carrying the requested block. Carries `from` so the
    /// requester can request the block's parent from the responder.
    Response(Replica, u64, Block),

    Invalid,
}

impl WireReady for ProtocolMsg {
    fn from_bytes(bytes: &[u8]) -> Self {
        // `Block`'s custom Deserialize populates its hash; no post-process.
        bincode::deserialize(bytes).expect("failed to decode the protocol message")
    }

    fn init(self) -> Self {
        self
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to serialize protocol message")
    }
}
