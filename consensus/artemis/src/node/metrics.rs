//! Low-overhead in-memory metrics for the artemis reactor.
//!
//! Atomic counters + coarse latency histogram, all stored in a
//! shared `Arc<Metrics>`. Event sites call `record_*` which is ~5ns
//! (single atomic increment + one bucket index). No allocation, no
//! syscall, no string formatting. Safe to inline on the hot path
//! even at hundreds-of-thousands of events per second.
//!
//! The reactor registers a `tokio::signal::ctrl_c` arm in its
//! select; on SIGINT we dump a one-shot snapshot to stderr and
//! process::exit. See `reactor.rs`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

/// 9 fixed-width duration buckets in microseconds. Chosen to cover
/// the range of interesting inter-event gaps observed on loopback
/// (tens of microseconds) through slow-peer stalls (hundreds of
/// milliseconds).
pub const BUCKETS_US: &[u64] = &[
    100,       //   0 -   100 us
    500,       // 100 -   500 us
    1_000,     // 500 us - 1 ms
    5_000,     //   1 -     5 ms
    10_000,    //   5 -    10 ms
    50_000,    //  10 -    50 ms
    100_000,   //  50 -   100 ms
    500_000,   // 100 -   500 ms
    u64::MAX,  // 500 ms +
];

pub struct Histogram {
    buckets: [AtomicU64; 9],
    sum_us: AtomicU64,
    count: AtomicU64,
    max_us: AtomicU64,
}

impl Histogram {
    const fn new() -> Self {
        Self {
            buckets: [
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
            ],
            sum_us: AtomicU64::new(0),
            count: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn record_us(&self, us: u64) {
        let idx = match BUCKETS_US.iter().position(|&b| us < b) {
            Some(i) => i,
            None => BUCKETS_US.len() - 1,
        };
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.sum_us.fetch_add(us, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        // Racy-max is fine; we just want "order of magnitude observed".
        let mut cur = self.max_us.load(Ordering::Relaxed);
        while us > cur {
            match self.max_us.compare_exchange_weak(cur, us,
                Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
    }

    fn print(&self, name: &str) {
        let count = self.count.load(Ordering::Relaxed);
        if count == 0 {
            eprintln!("  [{:<18}] (no samples)", name);
            return;
        }
        let sum = self.sum_us.load(Ordering::Relaxed);
        let mean = sum / count;
        let max = self.max_us.load(Ordering::Relaxed);
        eprintln!("  [{:<18}] n={:<6} mean={:>7}us max={:>7}us",
                  name, count, mean, max);
        let edges = [
            "    0 -  100us", "  100 -  500us", "  500us-   1ms",
            "    1 -    5ms", "    5 -   10ms", "   10 -   50ms",
            "   50 -  100ms", "  100 -  500ms", "  500ms+      ",
        ];
        for (edge, b) in edges.iter().zip(self.buckets.iter()) {
            let n = b.load(Ordering::Relaxed);
            if n == 0 { continue; }
            let pct = n as f64 * 100.0 / count as f64;
            let bar = "#".repeat((pct / 2.0) as usize);
            eprintln!("    {}  {:>6}  {:5.1}%  {}", edge, n, pct, bar);
        }
    }
}

/// Reactor-scope counters + histograms. Cheap to clone (`Arc`).
pub struct Metrics {
    pub proposes: AtomicU64,
    pub votes: AtomicU64,
    pub round_advances: AtomicU64,
    pub batch_recvs: AtomicU64,
    pub reactor_iters: AtomicU64,
    pub inter_propose: Histogram,
    pub inter_vote: Histogram,
    pub inter_round_advance: Histogram,
    pub inter_batch_recv: Histogram,
    last_propose: AtomicU64,         // ns since `created_at`
    last_vote: AtomicU64,
    last_round_advance: AtomicU64,
    last_batch_recv: AtomicU64,
    created_at: Instant,
}

impl Metrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            proposes: AtomicU64::new(0),
            votes: AtomicU64::new(0),
            round_advances: AtomicU64::new(0),
            batch_recvs: AtomicU64::new(0),
            reactor_iters: AtomicU64::new(0),
            inter_propose: Histogram::new(),
            inter_vote: Histogram::new(),
            inter_round_advance: Histogram::new(),
            inter_batch_recv: Histogram::new(),
            last_propose: AtomicU64::new(0),
            last_vote: AtomicU64::new(0),
            last_round_advance: AtomicU64::new(0),
            last_batch_recv: AtomicU64::new(0),
            created_at: Instant::now(),
        })
    }

    #[inline]
    fn now_ns(&self) -> u64 {
        self.created_at.elapsed().as_nanos() as u64
    }

    #[inline]
    fn record_interval(&self, last: &AtomicU64, hist: &Histogram) {
        let now = self.now_ns();
        let prev = last.swap(now, Ordering::Relaxed);
        if prev != 0 {
            hist.record_us((now - prev) / 1_000);
        }
    }

    #[inline]
    pub fn record_propose(&self) {
        self.proposes.fetch_add(1, Ordering::Relaxed);
        self.record_interval(&self.last_propose, &self.inter_propose);
    }

    #[inline]
    pub fn record_vote(&self) {
        self.votes.fetch_add(1, Ordering::Relaxed);
        self.record_interval(&self.last_vote, &self.inter_vote);
    }

    #[inline]
    pub fn record_round_advance(&self) {
        self.round_advances.fetch_add(1, Ordering::Relaxed);
        self.record_interval(&self.last_round_advance, &self.inter_round_advance);
    }

    #[inline]
    pub fn record_batch_recv(&self) {
        self.batch_recvs.fetch_add(1, Ordering::Relaxed);
        self.record_interval(&self.last_batch_recv, &self.inter_batch_recv);
    }

    #[inline]
    pub fn record_reactor_iter(&self) {
        self.reactor_iters.fetch_add(1, Ordering::Relaxed);
    }

    /// Dump a human-readable summary to stderr. Safe to call from any
    /// thread; designed for the SIGINT / SIGTERM handler. Explicitly
    /// flushes stderr before returning because when stderr is a file
    /// (not a tty) `eprintln!` is block-buffered and `process::exit`
    /// doesn't flush on the way out.
    pub fn print_summary(&self, myid: u32) {
        use std::io::Write;
        let elapsed = self.created_at.elapsed().as_secs_f64();
        eprintln!();
        eprintln!("=========================================================");
        eprintln!("  artemis metrics  node {}  t={:.2}s", myid, elapsed);
        eprintln!("=========================================================");
        let reactor = self.reactor_iters.load(Ordering::Relaxed);
        let proposes = self.proposes.load(Ordering::Relaxed);
        let votes = self.votes.load(Ordering::Relaxed);
        let rounds = self.round_advances.load(Ordering::Relaxed);
        let batches = self.batch_recvs.load(Ordering::Relaxed);
        eprintln!("  reactor_iters  = {:>6}  ({:.0}/s)", reactor, reactor as f64 / elapsed);
        eprintln!("  proposes       = {:>6}  ({:.0}/s)", proposes, proposes as f64 / elapsed);
        eprintln!("  votes          = {:>6}  ({:.0}/s)", votes, votes as f64 / elapsed);
        eprintln!("  round_advances = {:>6}  ({:.0}/s)", rounds, rounds as f64 / elapsed);
        eprintln!("  batch_recvs    = {:>6}  ({:.0}/s)", batches, batches as f64 / elapsed);
        eprintln!();
        eprintln!("  inter-event histograms:");
        self.inter_propose.print("inter_propose");
        self.inter_vote.print("inter_vote");
        self.inter_round_advance.print("inter_round_adv");
        self.inter_batch_recv.print("inter_batch_recv");
        eprintln!("=========================================================");
        let _ = std::io::stderr().lock().flush();
    }
}
