use libcrypto::hash::Hash;
use libmempool::Batch;
use std::convert::TryFrom;
use types::artemis::{Block, ProtocolMsg, Replica, Transaction};
use types::BlockTrait;

use super::coordinator::check_batch_hash;
use super::*;

/// Buffer and re-order messages by queueing them. Pairs incoming
/// blocks with their accompanying batches so downstream processing
/// can persist them before voting.
pub fn buffer_message(message: ProtocolMsg, cx: &mut Context) {
    match message {
        ProtocolMsg::Invalid => (),
        ProtocolMsg::NewBlock(b, batch) => cx.block_processing_waiting.push_back((b, batch)),
        ProtocolMsg::Response(from, _req_id, blk, batch) => {
            cx.response_waiting.push_back((from, blk, batch))
        }
        x => cx.other_buf.push_back(x),
    }
}

/// Process message dequeues buffered messages and tries reacting to them.
pub async fn process_message(cx: &mut Context) {
    // Process view leader's blocks (persist batch, then deliver).
    while let Some((b, batch)) = cx.block_processing_waiting.pop_front() {
        if !check_batch_hash(&b, &batch) {
            log::warn!("Batch hash mismatch on NewBlock; dropping");
            continue;
        }
        cx.persist_batch(b.blk.body.batch_hash.clone(), &batch).await;
        on_receive_new_block_direct(cx, b).await;
    }
    // Responses (same: persist then deliver).
    while let Some((sender, block, batch)) = cx.response_waiting.pop_front() {
        if !check_batch_hash(&block, &batch) {
            log::warn!("Batch hash mismatch on Response; dropping");
            continue;
        }
        cx.persist_batch(block.blk.body.batch_hash.clone(), &batch).await;
        update_delivery(cx, block, sender).await;
    }
    while let Some(v) = cx.vote_ready.remove(&cx.round()) {
        on_receive_round_vote(cx, v).await;
    }
    while let Some(msg) = cx.other_buf.pop_front() {
        match msg {
            ProtocolMsg::UCRVote(v) => {
                let from = v.origin();
                try_receive_round_vote(cx, from, v).await
            }
            ProtocolMsg::Relay(from, v) => try_receive_round_vote(cx, from, v).await,
            ProtocolMsg::Request(from, req_id, h) => handle_request(from, req_id, h, cx).await,
            ProtocolMsg::Blame(v) => on_receive_blame(v, cx).await,
            _ => panic!("unreachable"),
        }
    }
    while let Some(v) = cx.vote_ready.remove(&cx.round()) {
        on_receive_round_vote(cx, v).await;
    }
}

/// Take a block (already persisted alongside its batch) and deliver
/// it, or defer to a request path if we're missing ancestors.
pub async fn update_delivery(cx: &mut Context, b: Block, sender: Replica) {
    if cx.storage.is_delivered_by_hash(&b.get_hash()) {
        return;
    }
    let p_hash = Hash::<Block>::try_from(b.blk.header.prev.as_ref())
        .expect("hash is exactly 32 bytes");
    let is_parent_delivered = cx.storage.is_delivered_by_hash(&p_hash);
    if cx.block_parent_waiting.contains_key(&p_hash) {
        log::debug!("Already waiting for this block");
        return;
    }
    let b_hash = b.get_hash();
    if !is_parent_delivered {
        cx.block_parent_waiting.insert(p_hash, b_hash.clone());
        cx.undelivered_blocks.insert(b_hash.clone(), b);
        do_request(cx, sender, b_hash).await;
        return;
    }
    do_delivery(b, cx);
}

// Suppress the unused-type warning for `Batch` / `Transaction` when
// re-exported via `buffer_message`'s signature.
#[allow(dead_code)]
fn _assert_types(_: Batch<Transaction>) {}
