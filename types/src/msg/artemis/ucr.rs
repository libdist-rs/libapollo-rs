use libcrypto::{hash::Hash, Keypair, PublicKey};
use serde::{Deserialize, Serialize};

use super::{Block, Round, View, Vote};
use crate::KeypairSign;

/// UCRVote is sent by the round leader. It commits to:
/// - a block (by hash),
/// - a view number,
/// - a UCR round number,
/// with a signature over the three.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct UCRVote {
    pub hash: Hash<Block>,
    pub round: Round,
    pub view: View,
    /// Private so callers are forced through `compute_sig`/`check_sig`.
    vote: Vote,
}

#[derive(Serialize, Deserialize)]
struct InternalUCRVote {
    hash: Hash<Block>,
    round: Round,
    view: View,
}

impl UCRVote {
    /// Sign `(hash, round, view)` with the leader's secret key.
    pub fn compute_sig(&mut self, sk: &Keypair) {
        let digest = Hash::<InternalUCRVote>::ser_and_hash(&self.to_internal());
        self.vote.auth = sk
            .sign(digest.as_ref())
            .expect("Failed to sign a ucr message");
    }

    /// Verify the signature over `(hash, round, view)`.
    pub fn check_sig(&self, pk: &PublicKey) -> bool {
        let digest = Hash::<InternalUCRVote>::ser_and_hash(&self.to_internal());
        pk.verify(digest.as_ref(), &self.vote.auth)
    }

    fn to_internal(&self) -> InternalUCRVote {
        InternalUCRVote {
            hash: self.hash.clone(),
            round: self.round,
            view: self.view,
        }
    }

    pub fn new() -> Self {
        Self {
            hash: Hash::<Block>::EMPTY_HASH,
            round: 0,
            view: 0,
            vote: Vote {
                auth: Vec::new(),
                origin: 0,
            },
        }
    }
}
