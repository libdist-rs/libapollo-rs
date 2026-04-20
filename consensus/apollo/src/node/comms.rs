use libcrypto::hash::Hash;
use types::apollo::{Block, ClientMsg, Propose, ProtocolMsg, Replica, Transaction};

use super::context::Context;
use std::sync::Arc;

/// Communication logic
/// - `Send`      - send a `ProtocolMsg` to a specific node
/// - `Multicast` - send a `ProtocolMsg` to every peer but myself
/// - `Multicast client` - send a `ClientMsg` to every registered client

impl Context {
    /// Send a `ProtocolMsg` to a specific peer. Serializes once and
    /// stashes the returned handler under the current round.
    pub(crate) async fn send(&mut self, to: Replica, msg: Arc<ProtocolMsg>) {
        if to == self.myid() {
            return;
        }
        let bytes = Self::serialize_proto(msg.as_ref());
        match self.consensus_net.send(to, bytes).await {
            Ok(h) => self.remember_consensus(h),
            Err(e) => log::warn!("consensus send to {} failed: {:?}", to, e),
        }
    }

    /// Multicast (Sendall) to every peer but myself.
    pub(crate) async fn multicast(&mut self, msg: Arc<ProtocolMsg>) {
        let bytes = Self::serialize_proto(msg.as_ref());
        let results = self
            .consensus_net
            .broadcast(&self.broadcast_peers, bytes)
            .await;
        for r in results {
            match r {
                Ok(h) => self.remember_consensus(h),
                Err(e) => log::warn!("consensus broadcast leg failed: {:?}", e),
            }
        }
    }

    /// Multicast a committed block (wrapped in a `ClientMsg`) to every
    /// registered client. `tx_hashes` is hydrated by the caller from
    /// the block's referenced batch so the client can match commits
    /// back to its outstanding submissions.
    pub(crate) async fn multicast_client(
        &mut self,
        p: Arc<Propose>,
        b: Arc<Block>,
        tx_hashes: Vec<Hash<Transaction>>,
    ) {
        if self.all_clients.is_empty() {
            return;
        }
        let payload = types::apollo::Payload::with_payload(self.payload);
        let msg = ClientMsg::NewBlock(p.as_ref().clone(), b.as_ref().clone(), tx_hashes, payload);
        let bytes = bytes::Bytes::from(bincode::serialize(&msg).expect("ClientMsg serialize"));
        let results = self.client_net.broadcast(&self.all_clients, bytes).await;
        for r in results {
            match r {
                Ok(h) => self.remember_client(h),
                Err(e) => log::warn!("client broadcast leg failed: {:?}", e),
            }
        }
    }
}
