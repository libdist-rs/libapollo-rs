use libcrypto::hash::Hash;
use serde::{Deserialize, Serialize};

use super::{Block, Payload, UCRVote};
use crate::WireReady;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ClientMsg {
    /// Leader push: a UCRVote plus the series of blocks it commits to.
    /// The client validates that the last block's hash matches the vote
    /// before acting on it.
    NewBlock(UCRVote, Vec<(Block, Payload)>),
    /// Client asks a node to resend the block with the given hash.
    RequestBlock(Hash<Block>),
    /// Reply: requested hash + block. Client validates the returned
    /// block's hash matches the requested hash.
    ResponseBlock(Hash<Block>, Block),
}

impl WireReady for ClientMsg {
    fn from_bytes(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).expect("failed to decode the client message")
    }

    fn init(self) -> Self {
        self
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to serialize client message")
    }
}
