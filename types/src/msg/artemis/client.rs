//! Artemis no longer ships a protocol-specific `ClientMsg`. The new
//! mempool ingress / confirmation path uses `libmempool::ClientMsg<Tx>`
//! (NewTx / NewBatch / Confirmation), so this file is empty by design.
