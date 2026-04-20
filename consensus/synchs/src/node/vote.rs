use types::synchs::{Block, CertType, Certificate, Propose};
use libcrypto::hash::Hash;
use super::context::Context;

pub fn add_vote(mut c: Certificate, hash: Hash<Block>, cx: &mut Context) {
    if cx.cert_map.contains_key(&hash) {
        log::debug!("Extra vote received. discarding");
        return;
    }
    let mut cert = match cx.vote_map.remove(&hash) {
        None => {
            // First vote
            cx.vote_map.insert(hash, c);
            return;
        },
        Some(cert) => cert,
    };
    // Add the vote to the certificate
    cert.votes.push(c.votes.pop().unwrap());
    // Promote it to a full certificate if it has f+1 signatures
    if cert.votes.len() > cx.num_faults {
        cx.cert_map.insert(hash, cert.clone());
        cx.last_seen_cert = cert;
    } else {
        cx.vote_map.insert(hash, cert);
    }
}

/// A vote acts on an already-delivered block: we verify the vote
/// signature, check for equivocation, then fold it into the
/// certificate. We don't trigger block delivery from here post-mempool
/// -- the block-with-batch path runs when `ProtocolMsg::NewProposal`
/// arrives. Pre-mempool, `on_receive_proposal` was called here as a
/// defensive no-op on already-delivered blocks; it's been dropped since
/// the batch isn't in hand here anyway.
pub async fn on_vote(c: Certificate, p: Propose, cx: &mut Context) -> bool {
    let decision = false;
    log::debug!("Received a vote message: {:?}", c);

    if c.votes.len() != 1 {
        log::warn!("Invalid number of votes in vote message");
        return false;
    }
    let vote = &c.votes[0];
    let pk = match cx.pub_key_map.get(&vote.origin) {
        None => {
            log::warn!("vote from an unknown origin");
            return decision;
        },
        Some(x) => x,
    };
    let (sign_data, blk_hash) = match &c.msg {
        CertType::Vote(_v, d) => (util::io::to_bytes(&c.msg), d.clone()),
        _ => unreachable!("other vote types cant be here"),
    };

    if blk_hash != p.block_hash {
        log::warn!("Invalid vote message received");
        return decision;
    }

    if !pk.verify(&sign_data, &vote.auth) {
        log::warn!("vote not correctly signed");
        return decision;
    }

    if !cx.storage.is_delivered_by_hash(&blk_hash) {
        log::debug!("Received vote for an undelivered block");
        return decision;
    }

    let new_block = cx.storage.delivered_block_from_hash(&blk_hash).unwrap();

    // Is this an equivocation?
    if let Some(x) = cx.storage.delivered_block_from_ht(new_block.header.height) {
        if x.hash != blk_hash {
            log::warn!("Got an equivocation: {:?}, {:?}",
                x.header, new_block.header);
            return decision;
        }
    }

    add_vote(c, blk_hash, cx);
    decision
}
