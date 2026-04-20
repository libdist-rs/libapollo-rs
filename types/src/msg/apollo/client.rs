use libcrypto::hash::Hash;
use serde::{Deserialize, Serialize};

use super::*;
use crate::WireReady;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ClientMsg {
    /// Leader push: propose metadata + block + payload.
    NewBlock(Propose, Block, Payload),
    /// Client asks a node to resend the block with the given hash.
    Request(Hash<Block>),
    /// Reply to a `Request`: expected hash + propose metadata + block.
    Response(Hash<Block>, Propose, Block),
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
