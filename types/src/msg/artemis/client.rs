use libcrypto::hash::Hash;
use net_common::Message;
use serde::{Deserialize, Serialize};

use super::{Block, Payload, Transaction, UCRVote};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ClientMsg {
    /// Leader push: a UCRVote plus the series of blocks it commits to,
    /// each paired with the tx hashes the node hydrated from that
    /// block's referenced batch. The client validates that the last
    /// block's hash matches the vote before acting on it.
    NewBlock(UCRVote, Vec<(Block, Vec<Hash<Transaction>>, Payload)>),
    /// Client asks a node to resend the block with the given hash.
    RequestBlock(Hash<Block>),
    /// Reply: requested hash + block. Client validates the returned
    /// block's hash matches the requested hash.
    ResponseBlock(Hash<Block>, Block),
}

impl Message for ClientMsg {
    type DeserializationError = bincode::Error;

    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeserializationError> {
        bincode::deserialize(bytes)
    }
}
