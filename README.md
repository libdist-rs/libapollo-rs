# libapollo-rs

Rust implementations of four BFT consensus protocols:

| Crate               | Protocol         | Reference                                             |
| ------------------- | ---------------- | ----------------------------------------------------- |
| `consensus/apollo`  | Apollo           | [FC 2023 paper](https://github.com/libdist-rs/libchatter-rs/releases/tag/apollo-fc2023-artifact) |
| `consensus/artemis` | Artemis          |                                                       |
| `consensus/synchs`  | Sync HotStuff    |                                                       |
| `consensus/optsync` | Opt Sync         |                                                       |

This repository is the active-development home for these protocols. The
frozen FC 2023 artifact (same protocols, older dependencies) lives at
[`libdist-rs/libchatter-rs`](https://github.com/libdist-rs/libchatter-rs)
tag `apollo-fc2023-artifact`.

## Status

Migration in progress from libchatter-rs onto the modern successor crates:

- [x] Import from libchatter-rs@apollo-fc2023-artifact
- [ ] [`libcrypto-rs`](https://github.com/libdist-rs/libcrypto-rs) -- replaces in-tree `crypto/`
- [ ] [`libnet-rs`](https://github.com/libdist-rs/libnet-rs) -- replaces in-tree `net/`
- [ ] [`libmempool-rs`](https://github.com/libdist-rs/libmempool-rs) -- replaces the per-protocol transaction pool
- [ ] [`libstorage-rs`](https://github.com/libdist-rs/libstorage-rs) -- replaces the per-protocol in-memory block store

Each migration step is a focused commit validated end-to-end against the
stress-test harness.

## Building

```sh
cargo build --release
```

## Running the stress test

```sh
cargo build --release
./target/release/stress-test
```

The harness spawns N-node clusters on loopback for each of the four
protocols, drives a client load, and prints throughput + latency in the
same format as
[`libnet-rs`'s stress test](https://github.com/libdist-rs/libnet-rs).
The canonical baseline lives in [`baseline_results.txt`](baseline_results.txt).
