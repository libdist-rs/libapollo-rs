use types::optsync::{ClientMsg, Payload, Propose};

use crate::node::context::Context;
use std::sync::Arc;

/// Commit this block and all its ancestors
pub async fn on_commit(p: Arc<Propose>, cx: &mut Context) {
    let b = match cx.storage.delivered_block_from_hash(&p.block_hash) {
        Some(b) => b,
        None => {
            log::warn!("Commit fired for an undelivered block");
            return;
        }
    };
    if cx.storage.is_committed_by_hash(&b.hash) {
        return;
    }
    cx.last_committed_block_ht = b.header.height;
    cx.storage.add_committed_block(b.clone());

    let payload = Payload::with_payload(cx.payload);
    let msg = ClientMsg::NewBlock(b.as_ref().clone(), payload);
    cx.multicast_client(&msg).await;
    cx.gc_handlers();
}
