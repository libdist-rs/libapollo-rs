use libcrypto::hash::Hash;
use net_common::Message;
use serde::{Deserialize, Serialize};

use super::*;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ClientMsg {
    /// Leader push: propose metadata + block + payload.
    NewBlock(Propose, Block, Payload),
    /// Client asks a node to resend the block with the given hash.
    Request(Hash<Block>),
    /// Reply to a `Request`: expected hash + propose metadata + block.
    Response(Hash<Block>, Propose, Block),
}

impl Message for ClientMsg {
    type DeserializationError = bincode::Error;

    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeserializationError> {
        bincode::deserialize(bytes)
    }
}
