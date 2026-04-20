//! Type-erased hash used throughout the consensus code.
//!
//! Thin alias over `libcrypto::hash::Hash<T>`; we monomorphize to
//! `Hash<()>` because the consensus protocols pass hashes through
//! untyped containers (HashMap keys, `Vec<Hash>`, wire messages) where
//! the phantom type parameter buys nothing and only produces friction.

use serde::Serialize;

pub type Hash = libcrypto::hash::Hash<()>;

pub const HASH_SIZE: usize = 32;
pub const EMPTY_HASH: Hash = libcrypto::hash::Hash::<()>::EMPTY_HASH;

pub fn do_hash(bytes: &[u8]) -> Hash {
    libcrypto::hash::Hash::<()>::do_hash(bytes)
}

pub fn ser_and_hash<T: Serialize>(t: &T) -> Hash {
    let bytes = bincode::serialize(t).expect("bincode serialization error");
    libcrypto::hash::Hash::<()>::do_hash(&bytes)
}
