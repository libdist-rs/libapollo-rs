//! Round-robin batcher — ported from leto-rs
//! (`consensus/src/server/rr_batcher.rs`).
//!
//! Drives `Txpool` from three event sources:
//!   - the batch timeout interval (`pool.tick_timer`)
//!   - inbound client txs from the client listener
//!   - `BatcherConsensusMsg::*` from the consensus engine
//!
//! When the local node is the current round leader and either the size
//! threshold is met or the timer fires, the batcher seals a batch and
//! ships it as `Arc<CachedBatch<Tx>>` to the consensus reactor via
//! `tx_outgoing_batch`. The hashes are computed lazily on first access
//! (matches the libapollo-mempool intake convention).

use crate::{
    batch::CachedBatch,
    messages::BatcherConsensusMsg,
    tx::{MempoolTx, Replica},
    tx_pool::Txpool,
};
use anyhow::{anyhow, Result};
use log::*;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

#[derive(Debug, Clone)]
pub struct Parameters {
    pub my_id: Replica,
    pub initial_leader: Replica,
    pub batch_size: usize,
    pub batch_timeout: Duration,
}

impl Parameters {
    pub fn new(
        my_id: Replica,
        initial_leader: Replica,
        batch_size: usize,
        batch_timeout: Duration,
    ) -> Self {
        Self {
            my_id,
            initial_leader,
            batch_size,
            batch_timeout,
        }
    }
}

pub struct RRBatcher<Tx> {
    my_id: Replica,
    current_leader: Replica,
    current_round: u64,
    proposed: bool,
    rx_incoming_tx: UnboundedReceiver<(Tx, usize)>,
    rx_incoming_consensus: UnboundedReceiver<BatcherConsensusMsg<Tx>>,
    tx_outgoing_batch: UnboundedSender<Arc<CachedBatch<Tx>>>,
    pool: Txpool<Tx>,
}

impl<Tx> RRBatcher<Tx>
where
    Tx: MempoolTx,
{
    pub fn spawn(
        params: Parameters,
        rx_incoming_tx: UnboundedReceiver<(Tx, usize)>,
        rx_incoming_consensus: UnboundedReceiver<BatcherConsensusMsg<Tx>>,
        tx_outgoing_batch: UnboundedSender<Arc<CachedBatch<Tx>>>,
    ) {
        tokio::spawn(async move {
            let mut me = Self {
                my_id: params.my_id,
                current_leader: params.initial_leader,
                current_round: 0,
                proposed: false,
                rx_incoming_tx,
                rx_incoming_consensus,
                tx_outgoing_batch,
                pool: Txpool::new(params.batch_size, params.batch_timeout),
            };
            if let Err(e) = me.run().await {
                error!("RR-Batcher terminated: {}", e);
            }
        });
    }

    async fn run(&mut self) -> Result<()> {
        debug!(
            "RRBatcher booted: my_id={} initial_leader={}",
            self.my_id, self.current_leader
        );
        loop {
            let can_propose = self.my_id == self.current_leader && !self.proposed;

            tokio::select! {
                _ = self.pool.tick_timer(), if can_propose => {
                    let payload = self.pool.make_batch(self.current_round);
                    if !payload.is_empty() {
                        debug!("RRBatcher: timer-triggered batch with {} txs", payload.len());
                        let batch = Arc::new(CachedBatch::new(payload));
                        self.propose(batch)?;
                    }
                },
                tx = self.rx_incoming_tx.recv() => {
                    let (tx, tx_size) = tx.ok_or_else(||
                        anyhow!("RRBatcher: incoming-tx channel closed")
                    )?;
                    self.pool.add_tx(tx, tx_size);
                    if can_propose && self.pool.ready() {
                        let payload = self.pool.make_batch(self.current_round);
                        if !payload.is_empty() {
                            debug!("RRBatcher: size-triggered batch with {} txs", payload.len());
                            let batch = Arc::new(CachedBatch::new(payload));
                            self.propose(batch)?;
                        }
                    }
                },
                msg = self.rx_incoming_consensus.recv() => {
                    let msg = msg.ok_or_else(||
                        anyhow!("RRBatcher: consensus channel closed")
                    )?;
                    match msg {
                        BatcherConsensusMsg::NewRound { leader, round } => {
                            self.current_leader = leader;
                            self.current_round = round;
                            self.proposed = false;
                            self.pool.reset_timer();
                            self.try_propose()?;
                        }
                        BatcherConsensusMsg::Proposed { batch, round } => {
                            self.pool.admit_proposal(batch.as_ref(), round);
                        }
                        BatcherConsensusMsg::Committed { batch, round } => {
                            self.pool.commit(batch.as_ref(), round);
                        }
                        BatcherConsensusMsg::Rollback { rounds } => {
                            self.pool.rollback(&rounds);
                        }
                    }
                }
            }
        }
    }

    fn propose(&mut self, batch: Arc<CachedBatch<Tx>>) -> Result<()> {
        self.proposed = true;
        self.tx_outgoing_batch
            .send(batch)
            .map_err(anyhow::Error::new)
    }

    fn try_propose(&mut self) -> Result<()> {
        if self.my_id == self.current_leader && !self.proposed && self.pool.ready() {
            let payload = self.pool.make_batch(self.current_round);
            if payload.is_empty() {
                return Ok(());
            }
            let batch = Arc::new(CachedBatch::new(payload));
            self.propose(batch)
        } else {
            Ok(())
        }
    }
}
