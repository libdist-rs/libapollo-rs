use libcrypto::hash::Hash;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::{Block, CertType, Certificate, Payload, Propose, View};
use crate::WireReady;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ProtocolMsg {
    RawNewProposal(Propose, Block),
    NewProposal(Propose),
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
    INVALID,
}

impl ProtocolMsg {}

impl WireReady for ProtocolMsg {
    fn from_bytes(bytes: &[u8]) -> Self {
        let c: Self =
            bincode::deserialize(bytes).expect("failed to decode the protocol message");
        c.init()
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to serialize protocol message")
    }

    fn init(self) -> Self {
        match self {
            ProtocolMsg::RawNewProposal(mut p, b) => {
                let b = b.init();
                p.block = Some(Arc::new(b));
                ProtocolMsg::NewProposal(p)
            }
            ProtocolMsg::VoteMsg(ref c, _) => {
                if matches!(&c.msg, CertType::Vote(_, _)) {
                    self
                } else {
                    log::debug!("Invalid {:?}", self);
                    ProtocolMsg::INVALID
                }
            }
            ProtocolMsg::EquivcationBlameMsg(_, _, ref c) => {
                if matches!(&c.msg, CertType::Blame(_, _)) {
                    self
                } else {
                    log::debug!("Invalid {:?}", self);
                    ProtocolMsg::INVALID
                }
            }
            ProtocolMsg::NoProgressBlameMsg(ref c) => {
                if matches!(&c.msg, CertType::Blame(_, _)) {
                    self
                } else {
                    log::debug!("Invalid {:?}", self);
                    ProtocolMsg::INVALID
                }
            }
            ProtocolMsg::ChangeView(ref v, ref c) => {
                if let CertType::Vote(ref x, _) = c.msg {
                    if *v == *x {
                        self
                    } else {
                        log::debug!("Invalid {:?}", self);
                        ProtocolMsg::INVALID
                    }
                } else {
                    log::debug!("Invalid {:?}", self);
                    ProtocolMsg::INVALID
                }
            }
            other => other,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum ClientMsg {
    /// Leader push of a new block; re-packaged into `NewBlock` during `init`.
    RawNewBlock(Block, Payload),
    NewBlock(Block, Payload),
    /// Client asks a node to resend the block with the given hash.
    Request(Hash<Block>),
    RawResponse(Hash<Block>, Block),
    Response(Hash<Block>, Block),
}

impl WireReady for ClientMsg {
    fn from_bytes(bytes: &[u8]) -> Self {
        let c: Self =
            bincode::deserialize(bytes).expect("failed to decode the client message");
        c.init()
    }

    fn to_bytes(&self) -> Vec<u8> {
        bincode::serialize(self).expect("Failed to serialize client message")
    }

    fn init(self) -> Self {
        match self {
            ClientMsg::RawNewBlock(mut block, payload) => {
                block.hash = block.compute_hash();
                ClientMsg::NewBlock(block, payload)
            }
            ClientMsg::RawResponse(h, mut block) => {
                block.hash = block.compute_hash();
                ClientMsg::Response(h, block)
            }
            other => other,
        }
    }
}
