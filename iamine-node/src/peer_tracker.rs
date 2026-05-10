use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct PeerStats {
    #[allow(dead_code)]
    pub peer_id: String,
    pub avg_latency_ms: f64,
    pub ping_count: u64,
    pub last_seen: Instant,
    pub available_slots: usize,
    pub reputation_score: u32,
}

impl PeerStats {
    pub fn new(peer_id: String) -> Self {
        Self {
            peer_id,
            avg_latency_ms: 0.0,
            ping_count: 0,
            last_seen: Instant::now(),
            available_slots: 0,
            reputation_score: 100,
        }
    }

    pub fn update_latency(&mut self, rtt_ms: f64) {
        self.ping_count += 1;
        let n = self.ping_count as f64;
        // Exponential moving average
        self.avg_latency_ms = self.avg_latency_ms * (n - 1.0) / n + rtt_ms / n;
        self.last_seen = Instant::now();
    }

    /// Score combinado para el scheduler (menor latencia = mejor score)
    #[allow(dead_code)]
    pub fn latency_score(&self) -> u64 {
        if self.avg_latency_ms == 0.0 {
            return 20;
        }
        (1000.0 / self.avg_latency_ms.max(1.0)).min(20.0) as u64
    }

    pub fn is_alive(&self, timeout: Duration) -> bool {
        self.last_seen.elapsed() < timeout
    }
}

pub struct PeerTracker {
    peers: HashMap<String, PeerStats>,
    timeout: Duration,
}

impl PeerTracker {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
            timeout: Duration::from_secs(30),
        }
    }

    pub fn update_latency(&mut self, peer_id: &str, rtt_ms: f64) {
        self.peers
            .entry(peer_id.to_string())
            .or_insert_with(|| PeerStats::new(peer_id.to_string()))
            .update_latency(rtt_ms);
    }

    pub fn update_heartbeat(&mut self, peer_id: &str, slots: usize, rep: u32) {
        let stats = self
            .peers
            .entry(peer_id.to_string())
            .or_insert_with(|| PeerStats::new(peer_id.to_string()));
        stats.available_slots = slots;
        stats.reputation_score = rep;
        stats.last_seen = std::time::Instant::now();
    }

    pub fn get_latency(&self, peer_id: &str) -> f64 {
        self.peers
            .get(peer_id)
            .map(|s| s.avg_latency_ms)
            .unwrap_or(0.0)
    }

    #[allow(dead_code)]
    pub fn alive_peers(&self) -> Vec<&PeerStats> {
        self.peers
            .values()
            .filter(|p| p.is_alive(self.timeout))
            .collect()
    }

    pub fn peer_count(&self) -> usize {
        self.peers
            .values()
            .filter(|p| p.is_alive(self.timeout))
            .count()
    }

    pub fn avg_latency(&self) -> f64 {
        let alive: Vec<_> = self
            .peers
            .values()
            .filter(|p| p.is_alive(self.timeout))
            .collect();
        if alive.is_empty() {
            return 0.0;
        }
        alive.iter().map(|p| p.avg_latency_ms).sum::<f64>() / alive.len() as f64
    }
}
