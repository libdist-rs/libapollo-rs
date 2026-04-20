use libcrypto::hash::Hash;
use serde::{Deserialize, Serialize};

use super::{Block, Payload, UCRVote};
use crate::{BlockTrait, WireReady};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ClientMsg {
    /// Leader push: a UCRVote plus the series of blocks it commits to.
    /// `init` validates the final block's hash matches the vote and
    /// collapses to `NewBlock`.
    RawNewBlock(UCRVote, Vec<(Block, Payload)>),
    NewBlock(UCRVote, Vec<(Block, Payload)>),
    /// Client asks a node to resend the block with the given hash.
    RequestBlock(Hash<Block>),
    RawResponseBlock(Hash<Block>, Block),
    ResponseBlock(Hash<Block>, Block),
    /// Invalid / wire-validation failure.
    Invalid,
}

impl WireReady for ClientMsg {
    fn from_bytes(bytes: &[u8]) -> Self {
        let c: Self =
            bincode::deserialize(bytes).expect("failed to decode the client message");
        c.init()
    }

    fn init(self) -> Self {
        match self {
            ClientMsg::RawNewBlock(vote, block_vec) => {
                if block_vec.is_empty() {
                    log::warn!("Got a vote with 0 blocks");
                    return ClientMsg::Invalid;
                }
                let block_vec: Vec<_> = block_vec
                    .into_iter()
                    .map(|(block, pl)| (block.init(), pl))
                    .collect();
                if block_vec.last().unwrap().0.get_hash() != vote.hash {
                    log::warn!("The hash of the last block does not match the vote's hash");
                    return ClientMsg::Invalid;
                }
                ClientMsg::NewBlock(vote, block_vec)
            }
            ClientMsg::RawResponseBlock(h, block) => {
                let block = block.init();
                if block.get_hash() == h {
                    ClientMsg::ResponseBlock(h, block)
                } else {
                    ClientMsg::Invalid
                }
            }
            other => other,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to serialize client message")
    }
}
