use libmempool::{BatchHash, CachedBatch};
use std::sync::Arc;
use types::apollo::{Block, Propose, ProtocolMsg, Replica, Transaction};
use types::BlockTrait;
use super::*;

/// Leader proposes a block wrapping the given batch. The `batch` arg
/// arrives from the mempool's Processor via the consensus channel as
/// `Arc<CachedBatch>` -- no rocksdb read needed.
pub async fn do_propose(
    batch_hash: BatchHash<Transaction>,
    batch: Arc<CachedBatch<Transaction>>,
    cx: &mut Context,
) {
    let parent = cx.last_seen_block.as_ref();

    let mut new_block = Block::with_batch(batch_hash.clone());
    new_block.header.prev = parent.hash.clone();
    new_block.header.author = cx.myid();
    new_block.header.height = parent.header.height + 1;
    let new_block = new_block.init();

    let mut p = Propose::new(new_block.hash.clone());
    p.round = cx.round();
    p.sig.origin = cx.myid();
    p.sign(cx.my_secret_key.as_ref());

    // Hydrate tx hashes BEFORE moving `batch` into the ProtocolMsg.
    // Cheap thanks to `CachedBatch::tx_hashes` OnceLock-cache (intake
    // pre-filled on the leader).
    let tx_hashes = if cx.is_client_apollo_enabled() {
        Some(Context::hydrate_tx_hashes(batch.as_ref()))
    } else {
        None
    };

    let msg = Arc::new(ProtocolMsg::NewProposal(
        p.clone(),
        new_block.clone(),
        Arc::clone(&batch),
    ));
    cx.multicast(msg).await;

    let block_arc = Arc::new(new_block);
    if let Some(tx_hashes) = tx_hashes {
        cx.multicast_client(Arc::new(p.clone()), block_arc.clone(), tx_hashes)
            .await;
    }

    cx.storage.add_delivered_block(block_arc.clone());
    on_receive_proposal(Arc::new(p), block_arc, cx).await;
}

/// Incoming proposal: block and parent must already be delivered in storage.
/// `from` is the peer to ask for missing ancestors if we defer this to
/// `future_msgs` (typically the proposal's leader or the Response's
/// responder, depending on how it entered the buffer).
pub async fn try_receive_proposal(
    p: Propose,
    block: Arc<Block>,
    from: Replica,
    cx: &mut Context,
) {
    if cx.round() > p.round {
        log::debug!("Got a proposal from the past");
        return;
    }
    if cx.round() < p.round {
        log::debug!("Got a proposal from the future; queuing");
        cx.future_msgs.insert(p.round, (from, p));
        return;
    }

    if block.hash != p.block_hash {
        log::warn!("Block hash mismatch with propose");
        return;
    }
    if cx.round_leader() != p.sig.origin {
        return;
    }
    if cx.round_leader() != block.get_author() {
        return;
    }
    if p.sig.origin != cx.myid() && !p.check_sig(&cx.pub_key_map[&p.sig.origin]) {
        return;
    }

    on_receive_proposal(Arc::new(p), block, cx).await;
}

/// Verify that the batch carried in a proposal / response hashes to
/// what the block commits to. Free `OnceLock` compare -- the hash was
/// populated during the wire `Deserialize`.
pub fn check_batch_hash(block: &Block, batch: &CachedBatch<Transaction>) -> bool {
    batch.hash() == block.body.batch_hash
}

/// Called when a proposal has been fully delivered (block + ancestors).
pub async fn on_receive_proposal(p: Arc<Propose>, block: Arc<Block>, cx: &mut Context) {
    log::debug!("Handling valid proposal: {:?}", p);

    // Equivocation check: another block at this height from the same author.
    if let Some(x) = cx.storage.delivered_block_from_ht(block.header.height) {
        if x.hash != block.hash && x.header.author == block.header.author {
            log::warn!(
                "Equivocation detected: {:?}, {:?}",
                cx.storage.delivered_block_from_ht(block.header.height),
                block,
            );
            return;
        }
    }

    // Relay to the next leader before doing any heavy local work so the
    // chain keeps moving even if our commit path is slow.
    let msg = Arc::new(ProtocolMsg::Relay(cx.myid(), (*p).clone()));
    cx.send(cx.next_leader(), msg).await;

    cx.prop_chain_by_hash.insert(p.block_hash.clone(), p.clone());
    cx.prop_chain_by_round.insert(p.round, p.clone());

    if cx.round() > cx.num_faults() as u64 {
        do_commit(cx).await;
    }

    cx.last_seen_block = block;
    cx.update_round();
}
