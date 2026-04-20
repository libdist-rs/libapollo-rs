use types::synchs::{ClientMsg, Payload, Propose};

use super::context::Context;
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

    let ship = cx.cli_send.clone();
    let payload = cx.payload;
    let ship_b = b.clone();
    let ship_block = tokio::spawn(async move {
        let payload = Payload::with_payload(payload);
        let msg = ClientMsg::NewBlock(ship_b.as_ref().clone(), payload);
        log::debug!("sending msg: {:?} to the client", msg);
        if let Err(e) = ship.send(Arc::new(msg)) {
            println!("Error sending the block to the client: {}", e);
        }
        log::debug!("Committed block and sending it to the client now");
    });
    cx.last_committed_block_ht = b.header.height;
    cx.storage.add_committed_block(b.clone());
    ship_block.await.unwrap();
}
