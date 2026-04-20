use libcrypto::hash::Hash;
use std::sync::Arc;
use types::artemis::{Block, ProtocolMsg, Replica};

use super::context::Context;

/// Respond to a block request: send back both the block and its
/// referenced batch so the requester can persist it locally.
pub async fn handle_request(sender: Replica, req_id: u64, h: Hash<Block>, cx: &mut Context) {
    log::debug!("Got a request from {} for {:x?}", sender, h);
    let is_delivered = cx.storage.is_delivered_by_hash(&h);
    if !is_delivered && !cx.undelivered_blocks.contains_key(&h) {
        // I don't have the chain to respond to this request
        return;
    };
    let blk = if is_delivered {
        cx.storage
            .delivered_block_from_hash(&h)
            .unwrap()
            .as_ref()
            .clone()
    } else {
        cx.undelivered_blocks.get(&h).unwrap().clone()
    };
    let batch = match cx.read_batch(&blk.blk.body.batch_hash).await {
        Some(b) => b,
        None => {
            log::warn!(
                "Requested block {:?} has no batch on disk; skipping response",
                h
            );
            return;
        }
    };
    let msg = Arc::new(ProtocolMsg::Response(cx.myid(), req_id, blk, batch));
    cx.send(sender, msg).await;
}

/// Request this block from `sender`.
pub async fn do_request(cx: &mut Context, sender: Replica, h: Hash<Block>) {
    log::debug!("Requesting hash: {:x?}", h);
    let msg = Arc::new(ProtocolMsg::Request(cx.myid(), cx.req_ctr, h));
    cx.send(sender, msg).await;
}
