use libcrypto::hash::Hash;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::*;
use crate::WireReady;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ClientMsg {
    /// Leader push of a new block; re-packaged into `NewBlock` during `init`.
    RawNewBlock(Propose, Block, Payload),
    /// A processed message the client actually sees.
    NewBlock(Propose, Payload),
    /// Client asks a node to resend the block with the given hash.
    Request(Hash<Block>),
    RawResponse(Hash<Block>, Propose, Block),
    Response(Hash<Block>, Propose),
}

impl WireReady for ClientMsg {
    fn from_bytes(bytes: &[u8]) -> Self {
        let c: Self =
            bincode::deserialize(bytes).expect("failed to decode the client message");
        c.init()
    }

    fn init(self) -> Self {
        match self {
            ClientMsg::RawNewBlock(mut prop, mut block, payload) => {
                block.hash = block.compute_hash();
                prop.block = Some(Arc::new(block));
                ClientMsg::NewBlock(prop, payload)
            }
            ClientMsg::RawResponse(h, mut prop, mut block) => {
                block.hash = block.compute_hash();
                prop.block = Some(Arc::new(block));
                ClientMsg::Response(h, prop)
            }
            other => other,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to serialize client message")
    }
}
