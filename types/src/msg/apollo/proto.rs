use libcrypto::hash::Hash;
use serde::{Deserialize, Serialize};

use super::{Block, Propose, Vote};
use crate::WireReady;

#[derive(Debug, Serialize, Deserialize, Clone)]
#[repr(u8)]
pub enum ProtocolMsg {
    /// Leader's new proposal: propose metadata + the proposed block.
    NewProposal(Propose, Block),

    /// Non-leader forwarding a proposal to the next leader. The block is
    /// looked up from storage on the receiving side (or requested via
    /// `Request` if missing).
    Relay(Propose),

    /// Ask a peer to resend the block with the given hash (+ request id).
    Request(u64, Hash<Block>),
    /// Reply to a `Request`: propose metadata + block.
    Response(u64, Propose, Block),

    /// Blame a misbehaving leader.
    Blame(Vote),
}

impl WireReady for ProtocolMsg {
    fn from_bytes(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).expect("failed to decode the protocol message")
    }

    fn init(self) -> Self {
        self
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to serialize protocol message")
    }
}
