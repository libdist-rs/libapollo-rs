/// The core consensus module used for Apollo
///
/// The reactor reacts to all the messages from the network, and talks to the
/// clients accordingly.
use super::{context::Context, message::*, proposal::*};
use config::{ClientId, Node};
use futures::future::FutureExt;
use tls_receiver::TlsReceiver;
use tls_reliable_sender::TlsReliableSender;
use tokio_stream::StreamExt;
use types::apollo::{ClientMsg, ProtocolMsg, Replica, Transaction};

pub async fn reactor(
    config: &Node,
    is_client_apollo_enabled: bool,
    consensus_net: TlsReliableSender<Replica, ProtocolMsg>,
    mut consensus_recv: TlsReceiver<ProtocolMsg>,
    client_net: TlsReliableSender<ClientId, ClientMsg>,
    mut tx_recv: TlsReceiver<Transaction>,
) {
    let mut cx = Context::new(config, consensus_net, client_net, is_client_apollo_enabled);

    let block_size = config.block_size;
    let myid = config.id;

    loop {
        tokio::select! {
            pmsg_opt = consensus_recv.next() => {
                let pmsg = match pmsg_opt {
                    None => {
                        log::error!("Protocol message channel closed");
                        std::process::exit(0);
                    }
                    Some(Err(e)) => {
                        log::warn!("Dropping undecodable protocol message: {}", e);
                        continue;
                    }
                    Some(Ok(x)) => x,
                };
                handle_message(pmsg, &mut cx);
                // Drain any other ready messages in one pass so the
                // delivery/relay buffers process together.
                while let Some(Ok(pmsg)) = consensus_recv.next().now_or_never().flatten() {
                    handle_message(pmsg, &mut cx);
                }
                process_message(&mut cx).await;
            },
            tx_opt = tx_recv.next() => {
                match tx_opt {
                    None => break,
                    Some(Err(e)) => {
                        log::warn!("Dropping undecodable transaction: {}", e);
                        continue;
                    }
                    Some(Ok(tx)) => {
                        cx.storage.add_transaction(tx);
                    }
                }
            }
        }
        // Do we have sufficient commands, and are we the next leader?
        if cx.storage.get_tx_pool_size() >= block_size && cx.round_leader() == myid {
            log::debug!("I {} am the leader and, I am proposing", cx.myid());
            let txs = cx.storage.cleave(block_size);
            do_propose(txs, &mut cx).await;
        }
    }
}
