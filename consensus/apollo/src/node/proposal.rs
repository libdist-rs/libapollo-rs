use libmempool::{Batch, BatchHash};
use std::sync::Arc;
use types::apollo::{Block, Propose, ProtocolMsg, Replica, Transaction};
use types::BlockTrait;
use super::*;

/// Leader proposes a block wrapping the given batch. Reads the batch
/// from the local batch store, builds a block that references it by
/// hash, and broadcasts it (batch inline so followers don't need a
/// sync round-trip).
pub async fn do_propose(batch_hash: BatchHash<Transaction>, cx: &mut Context) {
    let batch = match cx.read_batch(&batch_hash).await {
        Some(b) => b,
        None => {
            log::warn!(
                "Leader's own batch {:?} missing from store; skipping propose",
                batch_hash
            );
            return;
        }
    };
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

    let msg = Arc::new(ProtocolMsg::NewProposal(
        p.clone(),
        new_block.clone(),
        batch.clone(),
    ));
    cx.multicast(msg).await;

    let block_arc = Arc::new(new_block);
    if cx.is_client_apollo_enabled() {
        let tx_hashes = Context::hydrate_tx_hashes(&batch);
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
/// what the block commits to.
pub fn check_batch_hash(block: &Block, batch: &Batch<Transaction>) -> bool {
    let serialized = match bincode::serialize(batch) {
        Ok(b) => b,
        Err(e) => {
            log::warn!("Failed to serialize batch for hash check: {}", e);
            return false;
        }
    };
    let computed: BatchHash<Transaction> = libcrypto::hash::Hash::do_hash(&serialized);
    computed == block.body.batch_hash
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
