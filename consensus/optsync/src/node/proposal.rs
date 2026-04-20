use crate::node::context::Context;
use libcrypto::hash::Hash;
use libmempool::{Batch, BatchHash};
use std::collections::HashSet;
use std::sync::Arc;
use types::optsync::{
    Block, CertType, Certificate, ProtocolMsg, Propose, Transaction, Vote,
};
use types::KeypairSign;

pub fn check_proposal(p: &Propose, new_block: &Block, cx: &Context) -> bool {
    if new_block.hash != p.block_hash {
        log::warn!("Block hash mismatch with propose");
        return false;
    }
    if new_block.header.author != cx.leader_of_view() {
        log::warn!("Got a proposal from an incorrect leader for the view");
        return false;
    }

    if new_block.header.height == 1 &&
        new_block.header.prev != libcrypto::hash::Hash::<Block>::EMPTY_HASH
    {
        log::warn!("First block does not extend the genesis block");
        return false;
    }

    if new_block.header.height > 1 && p.cert.votes.len() <= cx.num_faults {
        log::warn!("Insufficient votes in the proposal, rejecting the proposal");
        return false;
    }

    let pk = cx.pub_key_map.get(&new_block.header.author).unwrap();
    if !pk.verify(new_block.hash.as_ref(), &p.proof) {
        log::warn!("Got an incorrectly signed block");
        return false;
    }

    if cx.view != p.view {
        panic!("This view check should be unreachable");
    }

    if new_block.header.height == 1 {
        return true;
    }
    let mut uniq_votes = HashSet::with_capacity(cx.num_faults + 1);
    if let CertType::Vote(_v, h) = &p.cert.msg {
        if *h != new_block.header.prev {
            log::warn!("Certificate is for a different block than the propose's parent");
            return false;
        }
    } else {
        return false;
    }

    log::debug!("Checking certificate: {:?}", p.cert);
    let data = util::io::to_bytes(&p.cert.msg);
    for v in &p.cert.votes {
        let pk = match cx.pub_key_map.get(&v.origin) {
            None => {
                log::warn!("Invalid vote origin");
                return false;
            }
            Some(x) => x,
        };
        if !pk.verify(&data, &v.auth) {
            log::warn!("Invalid vote signature: {:?}", v);
            return false;
        }
        uniq_votes.insert(v.origin);
    }
    if uniq_votes.len() < cx.num_faults {
        return false;
    }
    if new_block.header.prev != cx.last_seen_block.hash {
        log::warn!("Parent undelivered");
        return false;
    }
    true
}

/// Verify the batch carried in a proposal hashes to the block's
/// committed `batch_hash`.
pub fn check_batch_hash(block: &Block, batch: &Batch<Transaction>) -> bool {
    let serialized = match bincode::serialize(batch) {
        Ok(b) => b,
        Err(e) => {
            log::warn!("Failed to serialize incoming batch for hash check: {}", e);
            return false;
        }
    };
    let computed: BatchHash<Transaction> = Hash::do_hash(&serialized);
    computed == block.body.batch_hash
}

pub async fn on_receive_proposal(
    p: Arc<Propose>,
    new_block: Arc<Block>,
    batch: Batch<Transaction>,
    cx: &mut Context,
) -> bool {
    log::debug!("Received a proposal: {}", new_block.header.height);

    if cx.storage.is_delivered_by_hash(&new_block.hash) {
        log::debug!("We have already processed this block last time");
        return false;
    }

    if !check_proposal(p.as_ref(), new_block.as_ref(), cx) {
        log::warn!("Proposal checking failed");
        return false;
    }

    if !check_batch_hash(new_block.as_ref(), &batch) {
        log::warn!("Batch hash mismatch; dropping proposal");
        return false;
    }

    cx.persist_batch(new_block.body.batch_hash.clone(), &batch).await;

    on_new_valid_proposal(p, new_block, cx).await
}

pub async fn on_new_valid_proposal(
    p: Arc<Propose>,
    new_block: Arc<Block>,
    cx: &mut Context,
) -> bool {
    if !cx.storage.is_delivered_by_hash(&new_block.header.prev) {
        log::warn!("We do not have the parent for this block");
        return false;
    }

    let mut my_vote = Certificate::empty_cert();
    my_vote.msg = CertType::Vote(cx.view, new_block.hash.clone());
    let sign_data = util::io::to_bytes(&my_vote.msg);
    match cx.my_secret_key.sign(&sign_data) {
        Err(e) => {
            panic!("Failed to sign a vote: {}", e);
        }
        Ok(vo) => {
            my_vote.votes.push(Vote {
                origin: cx.myid,
                auth: vo,
            });
        }
    };
    if let Some(x) = cx.vote_map.insert(p.block_hash.clone(), my_vote.clone()) {
        panic!("Already have a vote: {:?}", x);
    }

    cx.storage.add_delivered_block(new_block.clone());
    cx.height = new_block.header.height;
    cx.last_seen_block = new_block;
    cx.last_seen_cert = p.cert.clone();

    let vote_msg = ProtocolMsg::VoteMsg(my_vote, p.as_ref().clone());
    cx.multicast(&vote_msg).await;

    log::debug!("Sent a vote to all the nodes");
    true
}

pub async fn do_propose(
    batch_hash: BatchHash<Transaction>,
    cx: &mut Context,
) -> Option<Arc<Propose>> {
    let batch = match cx.read_batch(&batch_hash).await {
        Some(b) => b,
        None => {
            log::warn!(
                "Leader's own batch {:?} missing from store; skipping propose",
                batch_hash
            );
            return None;
        }
    };

    let parent = cx.last_seen_block.clone();
    let mut new_block = Block::with_batch(batch_hash.clone());

    new_block.header.author = cx.myid;
    new_block.header.prev = parent.hash.clone();
    new_block.header.height = parent.header.height + 1;
    let new_block = new_block.init();

    let proof = match cx.my_secret_key.sign(new_block.hash.as_ref()) {
        Err(e) => panic!("Failed to sign the new proposal: {}", e),
        Ok(sig) => sig,
    };

    let mut new_block_cert = Certificate::empty_cert();
    new_block_cert.msg = CertType::Vote(cx.view, new_block.hash.clone());
    let sign_data = util::io::to_bytes(&new_block_cert.msg);
    let sig = match cx.my_secret_key.sign(&sign_data) {
        Err(e) => panic!("Failed to sign the new proposal: {}", e),
        Ok(sig) => sig,
    };
    new_block_cert.votes.push(Vote {
        origin: cx.myid,
        auth: sig,
    });

    let new_block_ref = Arc::new(new_block);
    let mut p = Propose::new();
    p.proof = proof;
    p.block_hash = new_block_ref.hash.clone();
    p.cert = match cx.cert_map.get(&parent.hash) {
        None => panic!("Must call propose only if the parent is certified"),
        Some(x) => x.clone(),
    };
    p.view = cx.view;

    cx.storage.add_delivered_block(new_block_ref.clone());
    cx.vote_map
        .insert(new_block_ref.hash.clone(), new_block_cert);
    cx.height = new_block_ref.header.height;
    cx.last_seen_block = new_block_ref.clone();
    cx.last_committed_block_ht = cx.height;

    let msg = ProtocolMsg::NewProposal(p.clone(), new_block_ref.as_ref().clone(), batch);
    cx.multicast(&msg).await;

    let p = Arc::new(p);
    cx.commit_queue.insert(p.clone(), cx.d2);
    Some(p)
}
