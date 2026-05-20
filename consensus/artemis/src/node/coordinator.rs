use libcrypto::hash::Hash;
use libmempool::{BatchHash, BatcherConsensusMsg, CachedBatch};
use std::convert::TryFrom;
use std::sync::Arc;
use types::{
    artemis::{Block, ProtocolMsg, Transaction},
    BlockTrait,
};

use super::context::Context;

/// View leader dispatches a new block referencing `batch_hash`. The
/// `Arc<CachedBatch>` arrives directly on the mempool->consensus
/// channel — no rocksdb read.
pub async fn do_new_block(
    batch_hash: BatchHash<Transaction>,
    batch: Arc<CachedBatch<Transaction>>,
    cx: &mut Context,
) {
    cx.metrics.record_propose();
    let mut new_block = Block::with_batch(batch_hash.clone());
    let parent_hash = cx.last_seen_block.get_hash();
    new_block.blk.header.prev =
        libcrypto::hash::Hash::try_from(parent_hash.as_ref()).expect("hash is exactly 32 bytes");
    new_block.blk.header.author = cx.myid();
    new_block.blk.header.height = cx.last_seen_block.get_height() + 1;
    new_block.sig.origin = cx.myid();
    let mut new_block = new_block.init();
    new_block.sign(&cx.my_secret_key);

    let msg = Arc::new(ProtocolMsg::NewBlock(new_block.clone(), Arc::clone(&batch)));
    cx.multicast(msg).await;
    log::debug!("Broadcasting new block to all the nodes");

    // Tell the batcher's Txpool that this batch is now InFlight at
    // this height. Idempotent on the proposer's own loopback.
    let _ = cx
        .tx_consensus_to_batcher
        .send(BatcherConsensusMsg::Proposed {
            batch: Arc::clone(&batch),
            round: new_block.blk.header.height,
        });

    on_receive_new_block_direct(cx, new_block).await;
}

/// `on_recv_new_block_direct` is called when we get a new block from
/// the view co-ordinator (directly).
pub async fn on_receive_new_block_direct(cx: &mut Context, blk: Block) {
    log::debug!("Got a new block from the view leader: {:?}", blk);
    if cx.storage.is_delivered_by_hash(&blk.get_hash()) {
        return;
    }

    let prev_hash: Hash<Block> =
        Hash::try_from(blk.blk.header.prev.as_ref()).expect("hash is exactly 32 bytes");
    if !cx.storage.is_delivered_by_hash(&prev_hash) {
        log::warn!("View leader sent out of order blocks");
        return;
    }
    if cx.view_leader != blk.get_author() || cx.view_leader != blk.sig.origin {
        log::warn!(
            "Invalid block: expected from view leader {}, got author {} sig {}",
            cx.view_leader, blk.get_author(), blk.sig.origin
        );
        return;
    }
    if cx.view_leader != cx.myid()
        && !blk.check_sig(
            cx.pub_key_map
                .get(&cx.view_leader)
                .expect("Must have this node's pubkey"),
        )
    {
        log::warn!("Got an invalid signature");
        return;
    }
    log::debug!("Successfully dealt with the view leader's block: {:?}", blk);
    do_delivery(blk, cx);
}

/// Verify a batch carried in `ProtocolMsg::NewBlock` / `Response`
/// hashes to the block's referenced `batch_hash`. Free `OnceLock`
/// compare.
pub fn check_batch_hash(blk: &Block, batch: &CachedBatch<Transaction>) -> bool {
    batch.hash() == blk.blk.body.batch_hash
}

pub fn do_delivery(blk: Block, cx: &mut Context) {
    let b_hash = blk.get_hash();
    let b_rc = Arc::new(blk);
    cx.storage.add_delivered_block(b_rc.clone());
    let mut height_advanced = false;
    if cx.last_seen_block.get_height() < b_rc.get_height() {
        cx.last_seen_block = b_rc;
        height_advanced = true;
    }

    cx.undelivered_blocks.remove(&b_hash);

    if let Some(v) = cx.vote_waiting.remove(&b_hash) {
        cx.vote_ready.insert(v.round, v);
    }

    let mut b_hash = b_hash;
    while let Some(child) = cx.block_parent_waiting.remove(&b_hash) {
        if let Some(b) = cx.undelivered_blocks.remove(&child) {
            let b_rc = Arc::new(b);
            cx.storage.add_delivered_block(b_rc.clone());
            if cx.last_seen_block.get_height() < b_rc.get_height() {
                cx.last_seen_block = b_rc;
                height_advanced = true;
            }
        }
        if let Some(v) = cx.vote_waiting.remove(&child) {
            cx.vote_ready.insert(v.round, v);
        }
        b_hash = child;
    }

    if height_advanced {
        // Keep the batcher's `current_round` in lockstep with the
        // freshly-advanced chain head.
        cx.announce_height_to_batcher();
    }
}
