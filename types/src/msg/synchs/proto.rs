use libcrypto::hash::Hash;
use libmempool::Batch;
use net_common::Message;
use serde::{Deserialize, Serialize};

use super::{Block, Certificate, Payload, Propose, Transaction, View};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ProtocolMsg {
    /// Leader's new proposal: propose metadata, the block (which
    /// carries only the batch hash), and the batch itself so
    /// followers can persist it without a separate sync round-trip.
    NewProposal(Propose, Block, Batch<Transaction>),
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

impl Message for ProtocolMsg {
    type DeserializationError = bincode::Error;

    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeserializationError> {
        bincode::deserialize(bytes)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ClientMsg {
    /// Leader push of a new committed block, together with the tx
    /// hashes the node hydrated from the referenced batch so the
    /// client can match commits back to tx submissions.
    NewBlock(Block, Vec<Hash<Transaction>>, Payload),
    /// Client asks a node to resend the block with the given hash.
    Request(Hash<Block>),
    Response(Hash<Block>, Block),
}

impl Message for ClientMsg {
    type DeserializationError = bincode::Error;

    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeserializationError> {
        bincode::deserialize(bytes)
    }
}
