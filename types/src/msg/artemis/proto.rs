use libcrypto::hash::Hash;
use serde::{Deserialize, Serialize};

use super::{Block, UCRVote, Vote};
use crate::WireReady;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[repr(u8)]
pub enum ProtocolMsg {
    /// New block over the wire; re-packaged during `init`.
    RawNewBlock(Block),
    NewBlock(Block),

    RawUCRVote(UCRVote),
    /// UCRVote with a verified signature over (hash, round, view).
    UCRVote(UCRVote),

    /// Forward a vote from the round leader.
    Relay(UCRVote),

    Blame(Vote),

    /// Ask a peer to resend the block with the given content hash.
    Request(u64, Hash<Block>),
    RawResponse(u64, Block),
    Response(u64, Block),

    Invalid,
}

impl WireReady for ProtocolMsg {
    fn from_bytes(bytes: &[u8]) -> Self {
        let c: Self =
            bincode::deserialize(bytes).expect("failed to decode the protocol message");
        c.init()
    }

    fn init(self) -> Self {
        match self {
            ProtocolMsg::RawResponse(i, block) => ProtocolMsg::Response(i, block.init()),
            ProtocolMsg::RawNewBlock(b) => ProtocolMsg::NewBlock(b.init()),
            ProtocolMsg::RawUCRVote(v) => ProtocolMsg::UCRVote(v),
            other => other,
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to serialize protocol message")
    }
}
