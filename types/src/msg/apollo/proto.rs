use libcrypto::hash::Hash;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::{Block, Propose, Vote};
use crate::WireReady;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[repr(u8)]
pub enum ProtocolMsg {
    /// Leader's raw proposal -- re-packaged into `NewProposal` during `init`.
    RawNewProposal(Propose, Block),
    NewProposal(Propose),

    /// A non-leader forwarding a received proposal to the next leader.
    Relay(Propose),

    /// A request to re-send the block with the given hash (+ request id for dedup).
    Request(u64, Hash<Block>),
    RawResponse(u64, Propose, Block),
    Response(u64, Propose),

    /// Blame a misbehaving leader.
    Blame(Vote),
}

impl WireReady for ProtocolMsg {
    fn from_bytes(bytes: &[u8]) -> Self {
        let c: Self =
            bincode::deserialize(bytes).expect("failed to decode the protocol message");
        c.init()
    }

    fn init(self) -> Self {
        match self {
            ProtocolMsg::RawNewProposal(mut prop, block) => {
                // Block's Deserialize impl has already populated `block.hash`.
                prop.block = Some(Arc::new(block));
                ProtocolMsg::NewProposal(prop)
            }
            ProtocolMsg::RawResponse(i, mut prop, block) => {
                prop.block = Some(Arc::new(block));
                ProtocolMsg::Response(i, prop)
            }
            other => other,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to serialize protocol message")
    }
}
