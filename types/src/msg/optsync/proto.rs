use libcrypto::hash::Hash;
use serde::{Deserialize, Serialize};

use super::{Block, Certificate, Payload, Propose, View};
use crate::WireReady;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ProtocolMsg {
    /// Leader's new proposal: propose metadata + the block.
    NewProposal(Propose, Block),
    /// A vote for a proposed block.
    VoteMsg(Certificate, Propose),
    /// Two equivocating proposals from the same leader.
    EquivcationBlameMsg(Block, Block, Certificate),
    NoProgressBlameMsg(Certificate),

    /// Change view: (old view, certificate for the old view).
    ChangeView(View, Certificate),
    /// f+1 waiters quitting the view.
    QuitViewMsg(View, Certificate),
    /// Status: block + its certificate.
    StatusMsg(Certificate),
}

impl WireReady for ProtocolMsg {
    fn from_bytes(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).expect("failed to decode the protocol message")
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to serialize protocol message")
    }

    fn init(self) -> Self {
        self
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ClientMsg {
    /// Leader push of a new block.
    NewBlock(Block, Payload),
    /// Client asks a node to resend the block with the given hash.
    Request(Hash<Block>),
    Response(Hash<Block>, Block),
}

impl WireReady for ClientMsg {
    fn from_bytes(bytes: &[u8]) -> Self {
        bincode::deserialize(bytes).expect("failed to decode the client message")
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to serialize client message")
    }

    fn init(self) -> Self {
        self
    }
}
