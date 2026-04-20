pub type Replica = u16;
/// Block-height / round counter. Fixed-width `u64` instead of `usize`
/// because `libmempool::Round` requires `Sub<Output=Self>` and a
/// `const MIN`, which libmempool's blanket impls cover for `u*`/`i*`
/// but not `usize`. Also gives us a stable wire size across 32/64-bit
/// targets.
pub type Height = u64;