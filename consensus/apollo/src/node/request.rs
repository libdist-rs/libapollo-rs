use types::apollo::Block;
use libcrypto::hash::Hash;
use types::apollo::{ProtocolMsg, Replica};
use super::context::Context;
use std::sync::Arc;

pub async fn on_recv_request(sender:Replica, req_id: u64, h: Hash<Block>, cx: &mut Context)
{
    log::debug!(
        "Got a request from {} for {:?}", sender, h);
    let p_arc = match cx.prop_chain_by_hash.get(&h) {
        None => return,
        Some(x) => x.clone(),
    };
    let blk = match cx.storage.delivered_block_from_hash(&h) {
        None => return,
        Some(b) => b.as_ref().clone(),
    };
    let prop = p_arc.as_ref().clone();
    let msg = ProtocolMsg::Response(cx.myid(), req_id, prop, blk);
    cx.send(sender, Arc::new(msg)).await;
}

pub async fn do_request(b_hash: Hash<Block>, to: Replica, cx:&mut Context) {
    // I don't have the chain for this. Ask chain from the sender
    let msg = Arc::new(ProtocolMsg::Request(cx.myid(), cx.req_ctr, b_hash));
    cx.send(to, msg).await;
}