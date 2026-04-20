/// The core consensus module used for Sync HotStuff
///
/// The reactor reacts to all the messages from the network, and talks to the
/// clients accordingly.
use super::{commit::on_commit, context::Context, proposal::*, vote::on_vote};
use config::{ClientId, Node};
use std::sync::Arc;
use tls_receiver::TlsReceiver;
use tls_reliable_sender::TlsReliableSender;
use tokio_stream::StreamExt;
use types::synchs::{ClientMsg, ProtocolMsg, Replica, Transaction};

pub async fn reactor(
    config: &Node,
    consensus_net: TlsReliableSender<Replica, ProtocolMsg>,
    mut consensus_recv: TlsReceiver<ProtocolMsg>,
    client_net: TlsReliableSender<ClientId, ClientMsg>,
    mut tx_recv: TlsReceiver<Transaction>,
) {
    let d2 = std::time::Duration::from_millis(2 * config.delta);
    log::debug!("Started timers");
    let mut cx = Context::new(config, consensus_net, client_net);
    let block_size = config.block_size;
    let myid = config.id;
    // Start event loop
    loop {
        tokio::select! {
            pmsg_opt = consensus_recv.next() => {
                let protmsg = match pmsg_opt {
                    None => break,
                    Some(Err(e)) => {
                        log::warn!("Dropping undecodable protocol message: {}", e);
                        continue;
                    }
                    Some(Ok(x)) => x,
                };
                log::debug!(
                    "Received protocol message: {:?}", protmsg);
                if let ProtocolMsg::NewProposal(p, b) = protmsg {
                    log::debug!("Received a proposal: {:?}", p);
                    let p = Arc::new(p);
                    let b = Arc::new(b);
                    let decision = on_receive_proposal(p.clone(), b, &mut cx).await;
                    log::debug!("Decision for the incoming proposal is {}", decision);
                    if decision {
                        cx.commit_queue.insert(p, d2);
                    }
                }
                else if let ProtocolMsg::VoteMsg(v, p) = protmsg {
                    log::debug!("Received a vote for a proposal: {:?}", v);
                    on_vote(v, p, &mut cx).await;
                }
            },
            tx_opt = tx_recv.next() => {
                // We received a message from the client
                log::trace!("Got tx from the client: {:?}", tx_opt);
                let tx = match tx_opt {
                    None => break,
                    Some(Err(e)) => {
                        log::warn!("Dropping undecodable transaction: {}", e);
                        continue;
                    }
                    Some(Ok(x)) => x,
                };
                cx.storage.add_transaction(tx);
            },
            b_opt = cx.commit_queue.next(), if !cx.commit_queue.is_empty() => {
                // Got something from the timer
                match b_opt {
                    None => {
                        log::info!("Timer finished");
                    },
                    Some(Ok(b)) => {
                        log::debug!("Timer fired");
                        on_commit(b.into_inner(), &mut cx).await;
                    },
                    Some(Err(e)) => {
                        log::warn!("Timer misfired: {}", e);
                        continue;
                    }
                }
            }
        }
        // Do we have sufficient commands, and are we the next leader?
        // Also, do we have sufficient votes?
        if cx.storage.get_tx_pool_size() >= block_size
            && cx.next_leader() == myid
            && cx.cert_map.contains_key(&cx.last_seen_block.hash.clone())
        {
            log::debug!("I {} am the leader and, I am proposing", cx.myid);
            let txs = cx.storage.cleave(block_size);
            let (p, _b) = do_propose(txs, &mut cx).await;
            // Leader setting the timer now
            cx.commit_queue.insert(p, d2);
        }
    }
}
