//! # libapollo-mempool
//!
//! Two parallel mempool pipelines live in this crate:
//!
//! 1. **Legacy** `Receiver` → `Batcher` → `Processor` (used by
//!    synchs and optsync). Stateless count-sealer; takes raw `Tx`
//!    bytes off the wire from a single client.
//!
//! 2. **Keyed** `ClientListener` → `RRBatcher` (driven by the keyed
//!    `Txpool`) → consensus, with a `ConfirmationRouter` reflecting
//!    per-tx `Confirmation(Hash<Tx>)` back to the originating client.
//!    Used by apollo and artemis. Ported 1:1 from leto-rs.
//!
//! Both share `CachedBatch<Tx>` and the in-memory `BatchCache`.
//! Selecting between them is a matter of which entry point a
//! protocol's startup code calls:
//!
//! * `Mempool::spawn(...)` — legacy
//! * `KeyedMempool::spawn(...)` — keyed (apollo/artemis)

pub mod batch;
pub mod batcher;
pub mod cache;
pub mod client_listener;
pub mod confirmation_router;
pub mod messages;
pub mod processor;
pub mod receiver;
pub mod rr_batcher;
pub mod sealer;
pub mod tx;
pub mod tx_pool;

pub use batch::CachedBatch as Batch;
pub use batch::{BatchHash, CachedBatch};
pub use batcher::Batcher;
pub use cache::BatchCache;
pub use messages::{BatcherConsensusMsg, ClientMsg, ConsensusMempoolMsg};
pub use processor::Processor;
pub use receiver::Receiver;
pub use rr_batcher::{Parameters as BatcherParameters, RRBatcher};
pub use sealer::{CountSealer, Sealer};
pub use tx::{ClientId, MempoolTx, Replica};
pub use tx_pool::Txpool;

use libstorage::Store;
use net_common::Message;
use serde::Serialize;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

/// Default in-memory batch cache capacity.
pub const DEFAULT_CACHE_CAP: usize = 1024;

// ---------------------------------------------------------------------------
// Legacy mempool entry point. Used by synchs and optsync.
// ---------------------------------------------------------------------------

pub struct Mempool<Tx> {
    pub cache: Arc<BatchCache<Tx>>,
}

impl<Tx> Mempool<Tx>
where
    Tx: Serialize + Message + Send + Sync + 'static,
{
    pub fn spawn<S, Store_, Id, Round>(
        sealer: S,
        store: Store_,
        client_addr: SocketAddr,
        tx_to_consensus: UnboundedSender<(BatchHash<Tx>, Arc<CachedBatch<Tx>>)>,
        mut rx_consensus_control: UnboundedReceiver<ConsensusMempoolMsg<Id, Round, Tx>>,
        cache_cap: Option<usize>,
    ) -> Self
    where
        S: Sealer<Tx>,
        Store_: Store + Send + 'static,
        Id: Send + 'static,
        Round: Send + 'static,
    {
        let cache = BatchCache::<Tx>::new(cache_cap.unwrap_or(DEFAULT_CACHE_CAP));

        let (tx_batcher, rx_batcher) = unbounded_channel::<Tx>();
        let (tx_proc, rx_proc) = unbounded_channel::<Arc<CachedBatch<Tx>>>();

        Receiver::spawn::<Tx>(client_addr, tx_batcher);
        Batcher::spawn::<Tx, S>(rx_batcher, tx_proc, sealer);
        Processor::spawn::<Tx, Store_>(rx_proc, tx_to_consensus, Arc::clone(&cache), store);

        tokio::spawn(async move {
            while rx_consensus_control.recv().await.is_some() {}
        });

        Self { cache }
    }
}

// ---------------------------------------------------------------------------
// Keyed mempool entry point. Used by apollo and artemis.
// ---------------------------------------------------------------------------

/// Handle returned by `KeyedMempool::spawn`. The consensus reactor
/// threads `cache` into its `Context` for cache-first batch reads,
/// `tx_consensus_to_batcher` to signal `NewRound`/`Proposed`/
/// `Committed`/`Rollback`, and `tx_committed_to_router` to ack
/// committed txs to the originating clients.
pub struct KeyedMempool<Tx> {
    pub cache: Arc<BatchCache<Tx>>,
    pub tx_consensus_to_batcher: UnboundedSender<BatcherConsensusMsg<Tx>>,
    pub tx_committed_to_router: UnboundedSender<Arc<CachedBatch<Tx>>>,
}

impl<Tx> KeyedMempool<Tx>
where
    Tx: MempoolTx + Serialize + serde::de::DeserializeOwned,
{
    /// Wire the keyed pipeline:
    ///
    /// * `ClientListener` on `client_addr` decodes `ClientMsg<Tx>`
    ///   submissions and feeds the Txpool.
    /// * `RRBatcher` proposes sealed batches when this node is the
    ///   round leader, driven by BCM signals from consensus.
    /// * An internal seal task hashes the batch, installs it in
    ///   `cache`, persists it via `store`, and forwards it to
    ///   `tx_to_consensus`.
    /// * `ConfirmationRouter` listens on `tx_committed_to_router` and
    ///   sends `Confirmation(Hash<Tx>)` back to each tx's originating
    ///   client when its batch commits.
    pub fn spawn<Store_>(
        my_id: Replica,
        initial_leader: Replica,
        client_addr: SocketAddr,
        batch_size: usize,
        batch_timeout: Duration,
        mut store: Store_,
        tx_to_consensus: UnboundedSender<(BatchHash<Tx>, Arc<CachedBatch<Tx>>)>,
        cache_cap: Option<usize>,
    ) -> Self
    where
        Store_: Store + Send + 'static,
    {
        let cache = BatchCache::<Tx>::new(cache_cap.unwrap_or(DEFAULT_CACHE_CAP));

        let (tx_to_batcher, rx_to_batcher) = unbounded_channel::<(Tx, usize)>();
        let (tx_to_router, rx_to_router) =
            unbounded_channel::<(libcrypto::hash::Hash<Tx>, SocketAddr)>();
        let (tx_consensus_to_batcher, rx_consensus_to_batcher) =
            unbounded_channel::<BatcherConsensusMsg<Tx>>();
        let (tx_sealed, mut rx_sealed) = unbounded_channel::<Arc<CachedBatch<Tx>>>();
        let (tx_committed_to_router, rx_committed_to_router) =
            unbounded_channel::<Arc<CachedBatch<Tx>>>();

        client_listener::spawn::<Tx>(client_addr, tx_to_batcher, tx_to_router);

        RRBatcher::<Tx>::spawn(
            rr_batcher::Parameters::new(my_id, initial_leader, batch_size, batch_timeout),
            rx_to_batcher,
            rx_consensus_to_batcher,
            tx_sealed,
        );

        confirmation_router::spawn::<Tx>(rx_to_router, rx_committed_to_router);

        // Seal-and-persist task: hash, cache-insert, notify consensus,
        // fire rocksdb write. The rocksdb write is off the critical
        // path (libstorage::Store::write is a fire-and-forget mpsc
        // send into a background writer task).
        let cache_for_seal = Arc::clone(&cache);
        tokio::spawn(async move {
            while let Some(batch) = rx_sealed.recv().await {
                let hash = batch.hash();
                cache_for_seal.insert(hash.clone(), Arc::clone(&batch));
                if tx_to_consensus.send((hash.clone(), Arc::clone(&batch))).is_err() {
                    log::info!("KeyedMempool seal: consensus channel closed");
                    return;
                }
                let bytes = bincode::serialize(batch.as_ref()).expect("CachedBatch serialize");
                let key = hash.as_ref().to_vec();
                store.write(key, bytes).await;
            }
        });

        Self {
            cache,
            tx_consensus_to_batcher,
            tx_committed_to_router,
        }
    }
}
