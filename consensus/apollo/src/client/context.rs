use std::time::SystemTime;
use fnv::FnvHashMap as HashMap;

use libcrypto::hash::Hash;
use std::sync::Arc;
use types::apollo::{GENESIS_BLOCK, Propose, Round, Storage, Transaction};


pub struct Context {
    pub pending: usize,
    pub num_cmds: u128,
    pub time_map: HashMap<Hash<Transaction>, SystemTime>,
    pub latency_map: HashMap<Hash<Transaction>, (SystemTime, SystemTime)>,
    pub storage: Storage,
    pub round: Round,
    pub future_msgs: HashMap<Round, Propose>,
    /// Tx hashes hydrated server-side in `ClientMsg::NewBlock`, keyed
    /// by the round they were delivered for. The client used to read
    /// these out of `block.body.tx_hashes`, but post-libmempool the
    /// block only carries a `batch_hash`; the server resolves it to
    /// hashes at commit / propose time and sends them alongside.
    pub tx_hash_map: HashMap<Round, Vec<Hash<Transaction>>>,
}

impl Context {
    pub fn new() -> Self {
        let genesis_arc = Arc::new(GENESIS_BLOCK);
        let mut cx = Context {
            pending: 0,
            num_cmds: 0,
            time_map: HashMap::default(),
            latency_map: HashMap::default(),
            storage: Storage::new(),
            round: 1,
            future_msgs: HashMap::default(),
            tx_hash_map: HashMap::default(),
        };
        cx.storage.add_delivered_block(genesis_arc.clone());
        cx.storage.add_committed_block(genesis_arc);
        cx
    }
}
