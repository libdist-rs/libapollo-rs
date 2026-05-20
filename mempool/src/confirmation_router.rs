//! Confirmation router — ported from leto-rs
//! (`consensus/src/server/core.rs::run_confirmation_router`).
//!
//! Per-tx (not per-batch) because the keyed `Txpool` re-batches across
//! `NewBatch` boundaries — the committed batch's hash never matches
//! the client's original `NewBatch` hash. Per-tx confirmations let
//! clients compute latency from `tx_hash → send_ts`.

use crate::{batch::CachedBatch, messages::ClientMsg, tx::MempoolTx};
use bytes::Bytes;
use fnv::FnvHashMap;
use libcrypto::hash::Hash;
use std::{net::SocketAddr, sync::Arc};
use tcp_sender::TcpSimpleSender;
use tokio::sync::mpsc::UnboundedReceiver;

pub fn spawn<Tx>(
    mut rx_tx_sender_map: UnboundedReceiver<(Hash<Tx>, SocketAddr)>,
    mut rx_committed: UnboundedReceiver<Arc<CachedBatch<Tx>>>,
) where
    Tx: MempoolTx + serde::Serialize + serde::de::DeserializeOwned,
{
    tokio::spawn(async move {
        let mut pending: FnvHashMap<Hash<Tx>, SocketAddr> = FnvHashMap::default();
        let mut reply_sender: TcpSimpleSender<SocketAddr, ClientMsg<Tx>> =
            TcpSimpleSender::with_peers(FnvHashMap::default());

        loop {
            tokio::select! {
                reg = rx_tx_sender_map.recv() => {
                    match reg {
                        Some((tx_hash, addr)) => { pending.insert(tx_hash, addr); }
                        None => break,
                    }
                }
                batch = rx_committed.recv() => {
                    let batch = match batch {
                        Some(b) => b,
                        None => break,
                    };
                    for tx in batch.payload.iter() {
                        let tx_hash: Hash<Tx> = Hash::ser_and_hash(tx);
                        if let Some(addr) = pending.remove(&tx_hash) {
                            let msg = ClientMsg::<Tx>::Confirmation(tx_hash);
                            if let Ok(bytes) = bincode::serialize(&msg) {
                                if !reply_sender.get_peers().contains_key(&addr) {
                                    let mut new_peers = reply_sender.get_peers().clone();
                                    new_peers.insert(addr, addr);
                                    reply_sender =
                                        TcpSimpleSender::with_peers(new_peers);
                                }
                                let _ = reply_sender
                                    .send(addr, Bytes::from(bytes))
                                    .await;
                            }
                        }
                    }
                }
            }
        }
        log::warn!("ConfirmationRouter: shutdown");
    });
}
