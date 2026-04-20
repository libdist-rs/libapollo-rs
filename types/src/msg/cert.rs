use libcrypto::hash::Hash;
use serde::{Deserialize, Serialize};

use super::Block;
use crate::{Replica, View, Vote};

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub enum CertType {
    Blame(Replica, View),
    Vote(View, Hash<Block>),
    QuitView(View, Hash<Block>),
    DEFAULT,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Certificate {
    pub msg: CertType,
    pub votes: Vec<Vote>,
}

impl Certificate {
    pub fn empty_cert() -> Self {
        Certificate {
            votes: Vec::new(),
            msg: CertType::DEFAULT,
        }
    }
}

impl std::default::Default for Certificate {
    fn default() -> Self {
        Certificate::empty_cert()
    }
}
