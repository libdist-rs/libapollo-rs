//! A count-based sealer that fires after N transactions have been
//! queued, regardless of their serialized size.
//!
//! libmempool-rs ships `Sized` (byte-threshold) and `Timed`
//! (time-threshold) sealers; ours exists because the historical
//! libchatter-rs / libapollo-rs benchmark parameters are expressed in
//! *transactions per block* (`config.block_size`), not bytes.
//! Preserving that semantic keeps the stress-test matrix directly
//! comparable to the pre-mempool baseline.
use futures::Future;
use libmempool::Sealer;
use std::pin::Pin;
use std::task::{Context, Poll, Waker};

/// Seals a batch after `threshold` transactions have arrived.
pub struct CountSealer<Tx> {
    threshold: usize,
    txs: Vec<Tx>,
    waker: Option<Waker>,
}

impl<Tx> CountSealer<Tx> {
    pub fn new(threshold: usize) -> Self {
        assert!(threshold > 0, "CountSealer threshold must be > 0");
        Self {
            threshold,
            txs: Vec::with_capacity(threshold),
            waker: None,
        }
    }
}

impl<Tx> Sealer<Tx> for CountSealer<Tx>
where
    Tx: Send + Sync + 'static,
{
    fn seal(&mut self) -> Vec<Tx> {
        std::mem::take(&mut self.txs)
    }

    fn update(&mut self, tx: Tx, _tx_size: usize) {
        self.txs.push(tx);
        // `Batcher::run` polls us after every `update`, but we have to
        // wake explicitly in case the batcher is parked on our future
        // and the threshold-crossing update arrives while it's asleep.
        if self.txs.len() >= self.threshold {
            if let Some(w) = self.waker.take() {
                w.wake();
            }
        }
    }
}

impl<Tx> Unpin for CountSealer<Tx> {}

impl<Tx> Future for CountSealer<Tx>
where
    Tx: Send + Sync + 'static,
{
    type Output = Vec<Tx>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.txs.len() >= self.threshold {
            return Poll::Ready(self.seal());
        }
        self.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}
