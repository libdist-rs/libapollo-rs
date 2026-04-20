use std::sync::Arc;

use types::optsync::ProtocolMsg;
use crate::node::{
    context::Context,
    proposal::on_receive_proposal,
    vote::on_vote,
    commit::on_commit,
};

pub(crate) async fn process_msg(cx: &mut Context, protmsg: ProtocolMsg) {
    log::debug!("Received protocol message: {:?}", protmsg);
    if let ProtocolMsg::NewProposal(p, b) = protmsg {
        log::debug!("Received a proposal: {:?}", p);
        let p = Arc::new(p);
        let b = Arc::new(b);
        let decision = on_receive_proposal(p.clone(), b, cx).await;
        log::debug!("Decision for the incoming proposal is {}", decision);
        if decision {
            cx.commit_queue.insert(p, cx.d2);
        }
    }
    else if let ProtocolMsg::VoteMsg(v, p) = protmsg {
        log::debug!("Received a vote for a proposal: {:?}", v);
        let decision = on_vote(v, &p, cx).await;
        if decision {
            log::debug!("Optimistically committing block");
            on_commit(Arc::new(p), cx).await;
        }
    }
}
