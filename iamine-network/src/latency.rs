use crate::cluster::ClusterTier;

#[derive(Debug, Clone)]
pub struct PeerLatency {
    pub peer_id: String,
    pub avg_latency_ms: f64,
    pub sample_count: u64,
}

impl PeerLatency {
    pub fn new(peer_id: String) -> Self {
        Self {
            peer_id,
            avg_latency_ms: 0.0,
            sample_count: 0,
        }
    }

    pub fn update(&mut self, rtt_ms: f64) {
        self.sample_count += 1;
        let n = self.sample_count as f64;
        self.avg_latency_ms = self.avg_latency_ms * (n - 1.0) / n + rtt_ms / n;
    }

    pub fn cluster_tier(&self) -> ClusterTier {
        if self.avg_latency_ms < 10.0 {
            ClusterTier::Local
        } else if self.avg_latency_ms < 50.0 {
            ClusterTier::Nearby
        } else {
            ClusterTier::Remote
        }
    }
}
