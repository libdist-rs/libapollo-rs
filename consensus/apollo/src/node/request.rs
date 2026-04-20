use libcrypto::hash::Hash;
use super::context::Context;
use std::sync::Arc;
use types::apollo::{Block, ProtocolMsg, Replica};

/// Serve a `Request` by sending back both the block and its
/// referenced batch -- requester needs the batch to persist and later
/// hydrate for clients.
pub async fn on_recv_request(
    sender: Replica,
    req_id: u64,
    h: Hash<Block>,
    cx: &mut Context,
) {
    log::debug!("Got a request from {} for {:?}", sender, h);
    let p_arc = match cx.prop_chain_by_hash.get(&h) {
        None => return,
        Some(x) => x.clone(),
    };
    let blk = match cx.storage.delivered_block_from_hash(&h) {
        None => return,
        Some(b) => b.as_ref().clone(),
    };
    let batch = match cx.read_batch(&blk.body.batch_hash).await {
        Some(b) => b,
        None => {
            log::warn!(
                "Requested block {:?} has no batch on disk; skipping response",
                h
            );
            return;
        }
    };
    let prop = p_arc.as_ref().clone();
    let msg = ProtocolMsg::Response(cx.myid(), req_id, prop, blk, batch);
    cx.send(sender, Arc::new(msg)).await;
}

pub async fn do_request(b_hash: Hash<Block>, to: Replica, cx: &mut Context) {
    let msg = Arc::new(ProtocolMsg::Request(cx.myid(), cx.req_ctr, b_hash));
    cx.send(to, msg).await;
}
