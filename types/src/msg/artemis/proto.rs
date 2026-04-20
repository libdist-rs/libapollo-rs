use libcrypto::hash::Hash;
use serde::{Deserialize, Serialize};

use super::{Block, UCRVote, Vote};
use crate::WireReady;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[repr(u8)]
pub enum ProtocolMsg {
    NewBlock(Block),

    /// Round leader's vote.
    UCRVote(UCRVote),

    /// Forward a vote from the round leader.
    Relay(UCRVote),

    Blame(Vote),

    /// Ask a peer to resend the block with the given content hash.
    Request(u64, Hash<Block>),
    Response(u64, Block),

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
