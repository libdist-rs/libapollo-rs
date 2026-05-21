use fnv::FnvHashMap as HashMap;
use libcrypto::hash::Hash;
use std::time::SystemTime;

/// Per-window emission cadence (seconds) for `DP[Latency]`. Matches the
/// leto/zeus convention so cross-codebase orchestrators see the same
/// signal shape: per-window median latency on stderr, throughput once
/// at end-of-run. 1s gives finer-grained steady-state visibility.
pub const LAT_WINDOW_SECS: u64 = 1;

/// Emit the median of `samples` (latency in ms) as
/// `DP[Latency]: <median>` on stderr, then clear `samples`. No-op when
/// empty. Mirrors leto/zeus's per-window median-latency convention:
/// the orchestrator's last-seen value is the steady-state window, and
/// medians are robust to startup/cooldown skew that would distort a
/// cumulative mean.
pub fn emit_window_latency(samples: &mut Vec<u128>) {
    if samples.is_empty() {
        return;
    }
    samples.sort_unstable();
    let mid = samples.len() / 2;
    let median = if samples.len() % 2 == 0 {
        (samples[mid - 1] + samples[mid]) / 2
    } else {
        samples[mid]
    };
    eprintln!("DP[Latency]: {}", median);
    samples.clear();
}

/// End-of-run throughput emission: cumulative `confirmed / elapsed`.
/// Uses `eprintln!` so the line lands on stderr unconditionally, no
/// log-filter dependency. Matches the leto/zeus shape (`DP[Throughput]`
/// once at end, `DP[Latency]` per window during the run).
pub fn emit_run_throughput(now: SystemTime, start: SystemTime, confirmed: u64) {
    let elapsed = now
        .duration_since(start)
        .expect("time differencing errors")
        .as_secs_f64();
    let tps = if elapsed > 0.0 {
        confirmed as f64 / elapsed
    } else {
        0.0
    };
    eprintln!("DP[Start]: {:?}", start);
    eprintln!("DP[End]: {:?}", now);
    eprintln!("DP[Throughput]: {}", tps);
}

/// Legacy end-of-run summary: emits `DP[Throughput]` and a cumulative
/// mean `DP[Latency]` via `log::info!`. Retained for the synchs /
/// optsync clients which still drive throughput off client-observed
/// commit counts and do not yet follow the per-window leto/zeus
/// convention. Apollo and Artemis instead use
/// `emit_window_latency` + `emit_run_throughput`.
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

/// Latency-only variant used by clients whose server-side reactor is
/// responsible for emitting `DP[Throughput]` (Apollo, Artemis). Mirrors
/// the convention used by leto-rs: throughput at the server, latency at
/// the client.
pub fn statistics_latency<Tx>(
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
    if idx > 0 {
        log::info!("DP[Latency]: {}", (total_time as f64) / (idx as f64));
    } else {
        log::info!("DP[Latency]: 0");
    }
}
