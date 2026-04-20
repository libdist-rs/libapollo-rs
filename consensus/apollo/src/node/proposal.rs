use types::apollo::{Block, Propose, ProtocolMsg, Transaction, Replica};
use types::BlockTrait;
use types::WireReady;
use super::*;
use std::sync::Arc;

/// Creates a block using the last seen block as the parent, then proposes it.
pub async fn do_propose(txs: Vec<Arc<Transaction>>, cx: &mut Context) {
    let parent = cx.last_seen_block.as_ref();

    let mut new_block = Block::with_tx(txs);
    new_block.header.prev = parent.hash.clone();
    new_block.header.author = cx.myid();
    new_block.header.height = parent.header.height + 1;
    let new_block = new_block.init();

    let mut p = Propose::new(new_block.hash.clone());
    p.round = cx.round();
    p.sig.origin = cx.myid();
    p.sign(cx.my_secret_key.as_ref());

    let msg = Arc::new(ProtocolMsg::NewProposal(p.clone(), new_block.clone()));
    cx.multicast(msg).await;

    let block_arc = Arc::new(new_block);
    if cx.is_client_apollo_enabled() {
        cx.multicast_client(Arc::new(p.clone()), block_arc.clone()).await;
    }

    cx.storage.add_delivered_block(block_arc.clone());
    on_receive_proposal(Arc::new(p), block_arc, cx).await;
}

/// Incoming proposal: block and parent must already be delivered in storage.
pub async fn try_receive_proposal(
    p: Propose,
    block: Arc<Block>,
    _from: Replica,
    cx: &mut Context,
) {
    if cx.round() > p.round {
        log::debug!("Got a proposal from the past");
        return;
    }
    if cx.round() < p.round {
        log::debug!("Got a proposal from the future; queuing");
        cx.future_msgs.insert(p.round, (_from, p));
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

    let msg = Arc::new(ProtocolMsg::Relay((*p).clone()));
    let job = cx.c_send(cx.next_leader(), msg).await;

    cx.storage.clear(&block.body.tx_hashes);
    cx.prop_chain_by_hash.insert(p.block_hash.clone(), p.clone());
    cx.prop_chain_by_round.insert(p.round, p.clone());

    if cx.round() > cx.num_faults() {
        do_commit(cx).await;
    }

    cx.last_seen_block = block;
    cx.update_round();

    job.await.expect("Concurrent relaying failed");
}
