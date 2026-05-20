use std::sync::Arc;
use types::{
    artemis::{ProtocolMsg, Replica, UCRVote},
    BlockTrait,
};

use super::*;

/// Called to check if we are ready to do UCR voting.
pub async fn try_round_vote(cx: &mut Context) {
    if cx.myid() != cx.round_leader {
        log::trace!("I {} am not the leader for {}", cx.myid(), cx.round_leader);
        return;
    }
    if cx.last_seen_block.get_height() <= cx.last_voted_block.get_height() {
        log::trace!("I {} do not have any new blocks", cx.myid());
        return;
    }
    log::debug!("I am the round leader and I have new blocks to vote for");
    do_round_vote(cx).await;
}

/// Triggered when it is this node's turn to UCR vote.
pub async fn do_round_vote(cx: &mut Context) {
    cx.metrics.record_vote();
    let mut v = UCRVote::new();
    v.hash = cx.last_seen_block.get_hash();
    v.round = cx.round();
    v.view = cx.view;
    v.compute_sig(cx.myid(), &cx.my_secret_key);
    // Multicast the vote
    let msg = Arc::new(ProtocolMsg::UCRVote(v.clone()));
    cx.multicast(msg).await;
    // Per-tx confirmations are emitted from `do_commit` via the
    // mempool's `ConfirmationRouter`, so the legacy
    // `ClientMsg::NewBlock` multicast is gone.
    on_receive_round_vote(cx, v).await;
}

/// `try_receive_round_vote` is called to check if all the chain is delivered.
pub async fn try_receive_round_vote(cx: &mut Context, from: Replica, ucr_vote: UCRVote) {
    if cx.round() > ucr_vote.round {
        log::debug!(
            "Discarding duplicate votes for round {}, already in round {}",
            ucr_vote.round, cx.round()
        );
        return;
    }
    if cx.round() < ucr_vote.round {
        log::debug!(
            "Got a vote for round {} from the future for {}",
            ucr_vote.round, cx.round()
        );
        if cx.storage.is_delivered_by_hash(&ucr_vote.hash.clone()) {
            cx.vote_ready.insert(ucr_vote.round, ucr_vote);
        } else {
            let msg =
                Arc::new(ProtocolMsg::Request(cx.myid(), cx.req_ctr, ucr_vote.hash.clone()));
            cx.send(from, msg).await;
            cx.vote_waiting.insert(ucr_vote.hash.clone(), ucr_vote);
        }
        return;
    }

    if !cx.storage.is_delivered_by_hash(&ucr_vote.hash.clone()) {
        let msg =
            Arc::new(ProtocolMsg::Request(cx.myid(), cx.req_ctr, ucr_vote.hash.clone()));
        cx.send(from, msg).await;
        cx.vote_waiting.insert(ucr_vote.hash.clone(), ucr_vote);
        return;
    }

    on_receive_round_vote(cx, ucr_vote).await;
}

pub async fn on_receive_round_vote(cx: &mut Context, ucr_vote: UCRVote) {
    if cx.view != ucr_vote.view {
        log::warn!("Invalid view in UCR vote message");
        return;
    }
    if cx.myid() != cx.round_leader {
        if !ucr_vote.check_sig(&cx.pub_key_map[&cx.round_leader]) {
            log::warn!("Invalid signature on the UCR Vote");
            return;
        }
    }

    cx.vote_chain.insert(ucr_vote.round, Arc::new(ucr_vote.clone()));

    if cx.round() > cx.num_faults() as u64 {
        do_commit(cx).await;
    }

    let last_voted_block = cx
        .storage
        .delivered_block_from_hash(&ucr_vote.hash.clone())
        .expect("Obtained a vote for an unknown hash");
    cx.last_voted_block = last_voted_block.clone();

    let msg = Arc::new(ProtocolMsg::Relay(cx.myid(), ucr_vote));
    cx.send(cx.next_round_leader(), msg).await;

    cx.update_round();
    log::debug!("Going to the next round {}", cx.round());
}
