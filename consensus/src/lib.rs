use fnv::FnvHashMap as HashMap;
use libcrypto::hash::Hash;
use std::time::SystemTime;

/// Summarize a run: emit `DP[Throughput]` and `DP[Latency]` lines from the
/// client's per-tx send/commit timestamps. Generic over `Tx` so callers get
/// a type-safe `Hash<Tx>` key; `Tx` is otherwise phantom here.
pub fn statistics<Tx>(
    now: SystemTime,
    start: SystemTime,
    latency_map: HashMap<Hash<Tx>, (SystemTime, SystemTime)>,
) {
    let mut idx = 0u64;
    let mut total_time: u128 = 0;
    log::info!("DP[Start]: {:?}", start);
    log::info!("DP[End]: {:?}", now);
    for (_hash, (begin, end)) in latency_map {
        let time = end
            .duration_since(begin)
            .expect("time differencing errors")
            .as_millis();
        log::trace!("{}: {}", idx, time);
        idx += 1;
        total_time += time;
    }
    let elapsed = now
        .duration_since(start)
        .expect("time differencing errors")
        .as_secs_f64();
    log::info!("DP[Throughput]: {}", (idx as f64) / elapsed);
    log::info!("DP[Latency]: {}", (total_time as f64) / (idx as f64));
}
