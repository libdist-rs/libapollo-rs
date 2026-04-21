//! # libapollo-mempool
//!
//! Purpose-built mempool for the libapollo-rs consensus protocols
//! (Apollo, Artemis, Sync HotStuff, Opt Sync). Replaces the
//! libmempool-rs integration. Optimized around four invariants the
//! library couldn't give us:
//!
//! 1. **Tx hashes computed once, at intake.** The TCP receiver hashes
//!    each tx as it lands off the wire and carries the hash alongside
//!    the `Tx` into the batcher. When a batch seals, its `tx_hashes`
//!    field is already populated -- propose-time `hydrate_tx_hashes`
//!    is free on the leader.
//!
//! 2. **Batch hash cached via custom `Deserialize`.** Batches arriving
//!    off the wire inside a `ProtocolMsg::NewProposal` have their
//!    `BatchHash` populated during decode, so follower-side
//!    `check_batch_hash` is a free `OnceLock` compare rather than a
//!    re-serialize + SHA256.
//!
//! 3. **Leader sees its own batch via `Arc<CachedBatch>`, not a
//!    rocksdb round-trip.** The Processor signals consensus with
//!    `(BatchHash, Arc<CachedBatch>)` directly. `do_propose` never
//!    calls `read_batch`.
//!
//! 4. **Rocksdb persist is off the critical path.** `libstorage::Store`
//!    is already a channel-backed writer task; we `await` the enqueue
//!    but never block consensus on fsync. A crash before the write
//!    lands prevents this node from serving the batch on later
//!    requests -- but any honest follower that saw the proposal can,
//!    and `n > 2f` guarantees at least one such follower exists.
//!
//! There is no peer-to-peer mempool gossip. Our consensus protocols
//! already inline the `CachedBatch` in `ProtocolMsg::NewProposal` /
//! `NewBlock` / `Response`, so followers receive batches alongside
//! the blocks that reference them. A follower that missed a block
//! uses `ProtocolMsg::Request`, which (on reply) carries the batch
//! inline.
//!
//! ## Wiring
//!
//! ```text
//! client tx bytes
//!   │
//!   ▼
//! ┌──────────────────────┐   (Tx, Hash<Tx>)
//! │  Receiver (TCP)      ├──────────────────────┐
//! │  binds client_addr   │                      │
//! │  hashes each tx      │                      │
//! └──────────────────────┘                      │
//!                                               │
//! ┌──────────────────────┐                      │
//! │      Batcher         │◄─────────────────────┘
//! │  accumulates txs     │
//! │  seals via Sealer    │   Arc<CachedBatch>
//! └──────────┬───────────┘──────────────────────┐
//!                                               │
//! ┌──────────────────────┐◄─────────────────────┘
//! │     Processor        │
//! │  install in cache    │   (BatchHash, Arc<CachedBatch>)
//! │  notify consensus    ├──────────► consensus
//! │  fire rocksdb write  ├──────────► rocksdb (background)
//! └──────────────────────┘
//! ```

pub mod batch;
pub mod batcher;
pub mod cache;
pub mod messages;
pub mod processor;
pub mod receiver;
pub mod sealer;

pub use batch::{BatchHash, CachedBatch};

/// Source-compat alias for code that previously imported
/// `libmempool::Batch` from libmempool-rs. Our `CachedBatch<Tx>` is a
/// strict superset (wire-compatible) and adds the OnceLock caches.
pub use batch::CachedBatch as Batch;

pub use batcher::Batcher;
pub use cache::BatchCache;
pub use messages::ConsensusMempoolMsg;
pub use processor::Processor;
pub use receiver::Receiver;
pub use sealer::{CountSealer, Sealer};

use libstorage::Store;
use net_common::Message;
use serde::Serialize;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

/// Default in-memory batch cache capacity. Tuned to comfortably cover
/// the in-flight window (`config.window` ~= 10k txs / 400-tx blocks
/// = ~25 batches) across several rounds.
pub const DEFAULT_CACHE_CAP: usize = 1024;

/// Handle to a spawned mempool. Holds the shared `BatchCache` so the
/// consensus reactor can front rocksdb reads with in-memory lookups
/// via `Context::read_batch`.
pub struct Mempool<Tx> {
    pub cache: Arc<BatchCache<Tx>>,
}

impl<Tx> Mempool<Tx>
where
    Tx: Serialize + Message + Send + Sync + 'static,
{
    /// Spawn the mempool pipeline (Receiver, Batcher, Processor) and
    /// return a handle. The returned `cache` should be threaded into
    /// the consensus `Context` so `read_batch`/`persist_batch` hit
    /// memory before rocksdb.
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

        // Control channel drain. Today our mempool has no round-scoped
        // state to GC, but consensus reactors still issue `End` signals
        // on every round advance -- drain them so the channel doesn't
        // back up.
        tokio::spawn(async move {
            while rx_consensus_control.recv().await.is_some() {}
        });

        Self { cache }
    }
}
