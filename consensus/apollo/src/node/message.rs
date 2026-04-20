use types::apollo::{Block, Propose, ProtocolMsg, Replica};
use super::*;
use std::sync::Arc;

pub async fn process_message(cx:&mut Context)
{
    log::debug!("Handling proposals {:?}", cx.prop_buf);
    while let Some((sender, p, b)) = cx.prop_buf.pop_front() {
        delivery_check(sender, p, Some(b), cx).await;
    }
    log::debug!("Handling relays {:?}", cx.relay_buf);
    while let Some((sender, p)) = cx.relay_buf.pop_front() {
        delivery_check(sender, p, None, cx).await;
    }
    log::debug!("Handling others: {:?}", cx.other_buf);
    while let Some(pmsg) = cx.other_buf.pop_front() {
        match pmsg {
            ProtocolMsg::Request(from, rid, h) => {
                on_recv_request(from, rid, h, cx).await;
            }
            ProtocolMsg::Blame(v) => {
                on_receive_blame(v, cx).await;
            }
            _x => {
                debug_assert!(!matches!(
                    _x,
                    ProtocolMsg::NewProposal(..)
                        | ProtocolMsg::Response(..)
                        | ProtocolMsg::Relay(..)
                ));
            },
        };
    }

    while let Some((sender, p)) = cx.future_msgs.remove(&cx.round()) {
        delivery_check(sender, p, None, cx).await;
    }
}

pub fn handle_message(message: ProtocolMsg, cx: &mut Context) {
    match message {
        ProtocolMsg::NewProposal(p, b) => {
            let sender = p.sig.origin;
            cx.prop_buf.push_back((sender, p, b));
        }
        ProtocolMsg::Response(from, _rid, p, b) => cx.prop_buf.push_back((from, p, b)),
        ProtocolMsg::Relay(from, p) => cx.relay_buf.push_back((from, p)),
        x => cx.other_buf.push_back(x),
    }
}

/// Resolve a proposal to delivered state. If `block` is provided (NewProposal
/// or Response), it's used directly; otherwise (Relay or replayed future_msg)
/// the block is pulled from storage or requested from the sender.
pub async fn delivery_check(
    sender: Replica,
    p: Propose,
    block: Option<Block>,
    cx: &mut Context,
) {
    if cx.prop_chain_by_round.contains_key(&p.round) {
        log::debug!("Already handled {:?} before", p);
        return;
    }

    let block_arc: Arc<Block> = match block {
        Some(b) => {
            if b.hash != p.block_hash {
                log::warn!("Block hash mismatch in proposal");
                return;
            }
            Arc::new(b)
        }
        None => {
            if let Some(b_arc) = cx.storage.delivered_block_from_hash(&p.block_hash) {
                b_arc
            } else {
                log::debug!("Block unknown: {:?}", p.block_hash);
                let msg = Arc::new(ProtocolMsg::Request(cx.myid(), cx.req_ctr, p.block_hash.clone()));
                cx.prop_waiting.insert(p.block_hash.clone(), p);
                cx.send(sender, msg).await;
                return;
            }
        }
    };

    let parent_hash = block_arc.header.prev.clone();
    if !cx.storage.is_delivered_by_hash(&parent_hash) {
        let msg = Arc::new(ProtocolMsg::Request(cx.myid(), cx.req_ctr, parent_hash.clone()));
        cx.storage.add_delivered_block(block_arc);
        cx.prop_waiting_parent.insert(parent_hash, p);
        cx.send(sender, msg).await;
        return;
    }

    debug_assert!(cx.storage.is_delivered_by_hash(&parent_hash));
    cx.storage.add_delivered_block(block_arc.clone());

    let block_hash = p.block_hash.clone();
    if cx.round() < p.round {
        cx.future_msgs.insert(p.round, (sender, p));
    } else {
        try_receive_proposal(p, block_arc, sender, cx).await;
    }
    cx.prop_waiting.remove(&block_hash);

    let mut child_parent_hash = block_hash;
    while let Some(p_new) = cx.prop_waiting_parent.remove(&child_parent_hash) {
        child_parent_hash = p_new.block_hash.clone();
        let child_block = cx
            .storage
            .delivered_block_from_hash(&p_new.block_hash)
            .expect("child block must be in storage");
        if cx.round() < p_new.round {
            cx.future_msgs.insert(p_new.round, (sender, p_new));
        } else {
            try_receive_proposal(p_new, child_block, sender, cx).await;
        }
    }
}
