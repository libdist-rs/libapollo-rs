//! Client-facing ingress task — ported from leto-rs
//! (`consensus/src/server/core.rs::run_client_batch_listener`).
//!
//! Binds a `TcpReceiver<ClientMsg<Tx>>` on the configured client port
//! and fans each tx out to:
//!   1. the batcher (so the `Txpool` can mine it), and
//!   2. the confirmation router (so the server can route a per-tx
//!      `Confirmation(Hash<Tx>)` back when the tx commits).

use crate::{messages::ClientMsg, tx::MempoolTx};
use libcrypto::hash::Hash;
use std::net::SocketAddr;
use tcp_receiver::TcpReceiver;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::StreamExt;

pub fn spawn<Tx>(
    addr: SocketAddr,
    tx_batcher: UnboundedSender<(Tx, usize)>,
    tx_router: UnboundedSender<(Hash<Tx>, SocketAddr)>,
) where
    Tx: MempoolTx + serde::de::DeserializeOwned,
{
    tokio::spawn(async move {
        let mut receiver = TcpReceiver::<ClientMsg<Tx>>::spawn(addr);
        while let Some(result) = receiver.next().await {
            match result {
                Ok(ClientMsg::NewBatch { batch, reply_to }) => {
                    for tx in batch {
                        let tx_hash = Hash::<Tx>::ser_and_hash(&tx);
                        let _ = tx_router.send((tx_hash, reply_to));
                        let size = bincode::serialized_size(&tx).unwrap_or(0) as usize;
                        if tx_batcher.send((tx, size)).is_err() {
                            log::info!("ClientListener: batcher channel closed");
                            return;
                        }
                    }
                }
                Ok(ClientMsg::NewTx { tx, reply_to }) => {
                    let tx_hash = Hash::<Tx>::ser_and_hash(&tx);
                    let _ = tx_router.send((tx_hash, reply_to));
                    let size = bincode::serialized_size(&tx).unwrap_or(0) as usize;
                    if tx_batcher.send((tx, size)).is_err() {
                        log::info!("ClientListener: batcher channel closed");
                        return;
                    }
                }
                Ok(ClientMsg::Confirmation(_)) => {
                    // Server-only direction; clients should not send
                    // these on the ingress port.
                }
                Err(_e) => {
                    log::warn!("ClientListener: decode error on client message");
                }
            }
        }
        log::warn!("ClientListener: incoming stream ended");
    });
}
