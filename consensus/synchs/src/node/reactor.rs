/// The core consensus module used for Sync HotStuff
///
/// The reactor reacts to all the messages from the network, and talks to the
/// clients accordingly.
use super::{commit::on_commit, context::Context, proposal::*, vote::on_vote};
use config::{ClientId, Node};
use libmempool::{BatchHash, ConsensusMempoolMsg};
use libstorage::rocksdb::Storage as RocksStore;
use std::sync::Arc;
use tls_receiver::TlsReceiver;
use tls_reliable_sender::TlsReliableSender;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio_stream::StreamExt;
use types::synchs::{ClientMsg, Height, ProtocolMsg, Replica, Transaction};

pub async fn reactor(
    config: &Node,
    consensus_net: TlsReliableSender<Replica, ProtocolMsg>,
    mut consensus_recv: TlsReceiver<ProtocolMsg>,
    client_net: TlsReliableSender<ClientId, ClientMsg>,
    batch_store: RocksStore,
    mut rx_mem_to_consensus: UnboundedReceiver<BatchHash<Transaction>>,
    tx_consensus_to_mem: UnboundedSender<ConsensusMempoolMsg<Replica, Height, Transaction>>,
) {
    let d2 = std::time::Duration::from_millis(2 * config.delta);
    log::debug!("Started timers");
    let mut cx = Context::new(
        config,
        consensus_net,
        client_net,
        batch_store,
        tx_consensus_to_mem,
    );
    let myid = config.id;
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
                log::debug!("Received protocol message: {:?}", protmsg);
                if let ProtocolMsg::NewProposal(p, b, batch) = protmsg {
                    log::debug!("Received a proposal: {:?}", p);
                    let p = Arc::new(p);
                    let b = Arc::new(b);
                    let decision = on_receive_proposal(p.clone(), b, batch, &mut cx).await;
                    log::debug!("Decision for the incoming proposal is {}", decision);
                    if decision {
                        cx.commit_queue.insert(p, d2);
                    }
                } else if let ProtocolMsg::VoteMsg(v, p) = protmsg {
                    log::debug!("Received a vote for a proposal: {:?}", v);
                    on_vote(v, p, &mut cx).await;
                }
            },
            batch_hash_opt = rx_mem_to_consensus.recv() => {
                // Mempool announced a new batch is ready for consensus.
                // Leaders pop it off `pending_batches` when they next
                // have a certified parent; non-leaders just store it
                // (wasted in practice, since they'll never become
                // leader in sync hotstuff's fixed-leader view).
                match batch_hash_opt {
                    None => {
                        log::error!("Mempool channel closed");
                        break;
                    }
                    Some(bh) => {
                        log::debug!("Got new batch from mempool: {:?}", bh);
                        cx.pending_batches.push_back(bh);
                    }
                }
            },
            b_opt = cx.commit_queue.next(), if !cx.commit_queue.is_empty() => {
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
        // If we're the leader, have a certified parent, and have a
        // pending batch queued, propose.
        while cx.next_leader() == myid
            && cx.cert_map.contains_key(&cx.last_seen_block.hash.clone())
            && !cx.pending_batches.is_empty()
        {
            let bh = cx.pending_batches.pop_front().unwrap();
            log::debug!("I {} am the leader and proposing batch {:?}", cx.myid, bh);
            if let Some((p, _b)) = do_propose(bh, &mut cx).await {
                // Start the commit timer after propose, matching the
                // pre-mempool behaviour.
                cx.commit_queue.insert(p, d2);
            }
        }
    }
}
