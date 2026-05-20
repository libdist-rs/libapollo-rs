use crate::{Height, Replica};
use libcrypto::hash::Hash;
use serde::Serialize;

/// Block trait, anything that claims itself to be a block must satisfy these traits.
///
/// `Self: Serialize` is required so that `get_hash` can return a
/// `Hash<Self>` tied to the block's serialized representation.
pub trait BlockTrait: Sized + Serialize {
    /// A method to get the hash of this block.
    fn get_hash(&self) -> Hash<Self>;

    /// A method that returns the height of this block.
    fn get_height(&self) -> Height;

    /// Return the node id that created this block.
    fn get_author(&self) -> Replica;
}

/// Transaction trait, anything that can compute its own hash.
/// (Nonce/client_id keying lives on libapollo-mempool's `MempoolTx`
/// trait so the keyed pool's dedup contract is co-located with the
/// pool itself rather than spread across the types crate.)
pub trait TxTrait: Sized + Serialize {
    /// A method to get the hash of this transaction.
    fn get_hash(&self) -> Hash<Self>;
}
