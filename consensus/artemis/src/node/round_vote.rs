use libcrypto::hash::Hash;
use std::convert::TryFrom;
use types::{BlockTrait, artemis::{Block, ClientMsg, Payload, ProtocolMsg, Replica, Transaction, UCRVote}};
use super::*;
use std::{collections::VecDeque, sync::Arc};

/// Called to check if we are ready to do UCR voting
/// Check if
/// 1. Am I the next round leader?
/// 2. Do I have new blocks?
pub async fn try_round_vote(cx: &mut Context) {
    // I am not the next round leader, return
    if cx.myid() != cx.round_leader {
        log::trace!("I {} am not the leader for {}", cx.myid(), cx.round_leader);
        return;
    }
    // Do I have any new blocks that I can vote for?
    if cx.last_seen_block.get_height() <= cx.last_voted_block.get_height() {
        log::trace!("I {} do not have any new blocks", cx.myid());
        return;
    }
    log::debug!("I am the round leader and I have new blocks to vote for");
    do_round_vote(cx).await;
}

/// Triggered when it is this node's turn to UCR vote
pub async fn do_round_vote(cx: &mut Context) {
    cx.metrics.record_vote();
    let mut v = UCRVote::new();
    v.hash = cx.last_seen_block.get_hash();
    v.round = cx.round();
    v.view = cx.view;
    v.compute_sig(cx.myid(), &cx.my_secret_key);
    // Multicast the vote
    let msg = Arc::new(ProtocolMsg::UCRVote(v.clone()));
    cx.multicast(msg).await;
    // Walk the chain of blocks since last_voted_block, pushing
    // `Arc<Block>` (not deep-cloning the Block value). In the happy
    // case the chain depth is 1, but even when deeper the per-block
    // clone cost stays at an Arc bump.
    let mut block_vec: VecDeque<Arc<Block>> = VecDeque::new();
    let mut tail = v.hash.clone();
    while cx.last_voted_block.get_hash() != tail {
        let b = cx
            .storage
            .delivered_block_from_hash(&tail)
            .expect("Failed to get block");
        tail = libcrypto::hash::Hash::<Block>::try_from(b.blk.header.prev.as_ref())
            .expect("hash is exactly 32 bytes");
        block_vec.push_front(b);
    }
    let payload_size = cx.payload;
    // Hydrate each block's batch. `read_batch` hits the in-memory
    // cache first; the `Arc<[Hash<Transaction>]>` returned by
    // `CachedBatch::tx_hashes()` is `OnceLock`-cached, so the Vec
    // that used to wrap it every hydrate call is avoided.
    let mut hydrated: Vec<(Arc<Block>, Arc<[Hash<Transaction>]>, Payload)> =
        Vec::with_capacity(block_vec.len());
    for b in block_vec {
        let tx_hashes: Arc<[Hash<Transaction>]> = match cx.read_batch(&b.blk.body.batch_hash).await {
            Some(batch) => batch.tx_hashes(),
            None => {
                log::warn!(
                    "Missing batch {:?} at round-vote commit time; pushing empty hashes",
                    b.blk.body.batch_hash
                );
                Arc::from(Vec::<Hash<Transaction>>::new().into_boxed_slice())
            }
        };
        hydrated.push((b, tx_hashes, Payload::with_payload(payload_size)));
    }
    let msg = Arc::new(ClientMsg::NewBlock(v.clone(), hydrated));
    cx.multicast_client(msg).await;
    // Process self vote
    on_receive_round_vote(cx, v).await;
}

/// `try_receive_round_vote` is called to check if all the chain is delivered.
/// If it is, then we call `on_receive_round_vote`, otherwise we request it from the sender
/// Also checks if we got votes from the future/past
pub async fn try_receive_round_vote(cx:&mut Context, from: Replica, ucr_vote: UCRVote) {
    // We may get multiple votes from relay and do_round_vote
    if cx.round() > ucr_vote.round {
        log::debug!("Discarding duplicate votes for round {}, already in round {}", ucr_vote.round, cx.round());
        return;
    }
    // Is this the correct round?
    if cx.round() < ucr_vote.round {
        // We got a ucr_vote from the future
        log::debug!("Got a vote for round {} from the future for {}", ucr_vote.round, cx.round());
        if cx.storage.is_delivered_by_hash(&ucr_vote.hash.clone()) {
            cx.vote_ready.insert(ucr_vote.round, ucr_vote);
        } else {
            // I don't have the chain for this. Ask chain from the sender
            let msg = Arc::new(ProtocolMsg::Request(cx.myid(), cx.req_ctr, ucr_vote.hash.clone()));
            cx.send(from, msg).await;
            cx.vote_waiting.insert(ucr_vote.hash.clone(), ucr_vote);
        }
        return;
    }

    // Do I have the chain?
    if !cx.storage.is_delivered_by_hash(&ucr_vote.hash.clone()) {
        // I don't have the chain for this. Ask chain from the sender
        let msg = Arc::new(ProtocolMsg::Request(cx.myid(), cx.req_ctr, ucr_vote.hash.clone()));
        cx.send(from, msg).await;
        cx.vote_waiting.insert(ucr_vote.hash.clone(), ucr_vote);
        return;
    }

    // I have the chain
    on_receive_round_vote(cx, ucr_vote).await;
}

/// `on_receive_vote` is called after ensuring that we have the chain, and we are ready to process this message for this round
pub async fn on_receive_round_vote(cx:&mut Context, ucr_vote: UCRVote) {
    // Check signature
    if cx.view != ucr_vote.view {
        log::warn!("Invalid view in UCR vote message");
        return;
    }
    // The view is correct by now
    if cx.myid() != cx.round_leader {
        if !ucr_vote.check_sig(&cx.pub_key_map[&cx.round_leader]) {
            log::warn!("Invalid signature on the UCR Vote");
            return;
        }
    }

    cx.vote_chain.insert(ucr_vote.round, Arc::new(ucr_vote.clone()));

    if cx.round() > cx.num_faults() as u64 {
        do_commit(cx);
    }

    let last_voted_block = cx.storage.delivered_block_from_hash(&ucr_vote.hash.clone())
        .expect("Obtained a vote for an unknown hash");
    cx.last_voted_block = last_voted_block.clone();

    // Relay the vote to the next round leader.
    let msg = Arc::new(ProtocolMsg::Relay(cx.myid(), ucr_vote));
    cx.send(cx.next_round_leader(), msg).await;

    cx.update_round();
    log::debug!("Going to the next round  {}", cx.round());
}
