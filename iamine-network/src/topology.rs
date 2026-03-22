use std::collections::HashMap;

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
            ClusterTier::Same
        } else if self.avg_latency_ms < 30.0 {
            ClusterTier::Nearby
        } else {
            ClusterTier::Remote
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClusterTier {
    Same,
    Nearby,
    Remote,
}

#[derive(Debug, Clone)]
pub struct Cluster {
    pub id: String,
    pub nodes: Vec<String>,
    pub avg_latency_ms: f64,
}

pub struct NetworkTopology {
    /// peer_id -> latency info
    peers: HashMap<String, PeerLatency>,
    /// cluster_id -> cluster
    clusters: HashMap<String, Cluster>,
    /// peer_id -> cluster_id
    peer_cluster: HashMap<String, String>,
}

impl NetworkTopology {
    pub fn new() -> Self {
        Self {
            peers: HashMap::new(),
            clusters: HashMap::new(),
            peer_cluster: HashMap::new(),
        }
    }

    pub fn update_latency(&mut self, peer_id: &str, rtt_ms: f64) {
        self.peers
            .entry(peer_id.to_string())
            .or_insert_with(|| PeerLatency::new(peer_id.to_string()))
            .update(rtt_ms);
    }

    pub fn get_latency(&self, peer_id: &str) -> Option<&PeerLatency> {
        self.peers.get(peer_id)
    }

    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    /// Assign all known peers into clusters based on mutual latency.
    /// Uses the local node as the reference point (hub-and-spoke).
    pub fn assign_clusters(&mut self, local_peer_id: &str) {
        self.clusters.clear();
        self.peer_cluster.clear();

        // Group peers by tier relative to local node
        let mut same: Vec<String> = Vec::new();
        let mut nearby: Vec<String> = Vec::new();
        let mut remote: Vec<String> = Vec::new();

        for (pid, lat) in &self.peers {
            match lat.cluster_tier() {
                ClusterTier::Same => same.push(pid.clone()),
                ClusterTier::Nearby => nearby.push(pid.clone()),
                ClusterTier::Remote => remote.push(pid.clone()),
            }
        }

        // Local node is always in the "same" cluster
        let local_short = &local_peer_id[..12.min(local_peer_id.len())];

        // Always create local cluster (even if only local node)
        let mut nodes = same.clone();
        nodes.insert(0, local_peer_id.to_string());
        let avg = self.avg_latency_for(&same);
        let cid = format!("cluster-{}-local", local_short);
        for pid in &nodes {
            self.peer_cluster.insert(pid.clone(), cid.clone());
        }
        self.clusters.insert(
            cid.clone(),
            Cluster {
                id: cid,
                nodes,
                avg_latency_ms: avg,
            },
        );

        if !nearby.is_empty() {
            let avg = self.avg_latency_for(&nearby);
            let cid = format!("cluster-{}-nearby", local_short);
            for pid in &nearby {
                self.peer_cluster.insert(pid.clone(), cid.clone());
            }
            self.clusters.insert(
                cid.clone(),
                Cluster {
                    id: cid,
                    nodes: nearby,
                    avg_latency_ms: avg,
                },
            );
        }

        if !remote.is_empty() {
            let avg = self.avg_latency_for(&remote);
            let cid = format!("cluster-{}-remote", local_short);
            for pid in &remote {
                self.peer_cluster.insert(pid.clone(), cid.clone());
            }
            self.clusters.insert(
                cid.clone(),
                Cluster {
                    id: cid,
                    nodes: remote,
                    avg_latency_ms: avg,
                },
            );
        }
    }

    pub fn cluster_for_peer(&self, peer_id: &str) -> Option<&str> {
        self.peer_cluster.get(peer_id).map(|s| s.as_str())
    }

    pub fn all_clusters(&self) -> Vec<&Cluster> {
        self.clusters.values().collect()
    }

    pub fn peers_in_same_cluster(&self, peer_id: &str) -> Vec<String> {
        let Some(cid) = self.peer_cluster.get(peer_id) else {
            return Vec::new();
        };
        self.clusters
            .get(cid)
            .map(|c| c.nodes.clone())
            .unwrap_or_default()
    }

    pub fn display(&self) {
        println!(
            "🌐 Network Topology ({} peers, {} clusters) [latency: real ping RTT]",
            self.peers.len(),
            self.clusters.len()
        );
        let mut sorted: Vec<&Cluster> = self.clusters.values().collect();
        sorted.sort_by(|a, b| a.avg_latency_ms.partial_cmp(&b.avg_latency_ms).unwrap());
        for cluster in sorted {
            let tier = if cluster.avg_latency_ms < 10.0 {
                "LOCAL"
            } else if cluster.avg_latency_ms < 30.0 {
                "NEARBY"
            } else {
                "REMOTE"
            };
            println!(
                "   📦 {} [{}] ({:.1}ms avg, {} nodes)",
                cluster.id,
                tier,
                cluster.avg_latency_ms,
                cluster.nodes.len()
            );
            for node in &cluster.nodes {
                let short = &node[..12.min(node.len())];
                let lat = self
                    .peers
                    .get(node)
                    .map(|p| format!("{:.1}ms", p.avg_latency_ms))
                    .unwrap_or_else(|| "local".to_string());
                println!("      └─ {} ({})", short, lat);
            }
        }
    }

    fn avg_latency_for(&self, peer_ids: &[String]) -> f64 {
        if peer_ids.is_empty() {
            return 0.0;
        }
        let sum: f64 = peer_ids
            .iter()
            .filter_map(|pid| self.peers.get(pid))
            .map(|p| p.avg_latency_ms)
            .sum();
        sum / peer_ids.len() as f64
    }
}

pub type SharedNetworkTopology = std::sync::Arc<tokio::sync::RwLock<NetworkTopology>>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_creation() {
        let mut topo = NetworkTopology::new();
        topo.update_latency("peer-A", 5.0);
        topo.update_latency("peer-B", 20.0);
        topo.update_latency("peer-C", 50.0);
        topo.assign_clusters("local-node");

        assert_eq!(topo.all_clusters().len(), 3); // local, nearby, remote
        assert!(topo.cluster_for_peer("peer-A").unwrap().contains("local"));
        assert!(topo.cluster_for_peer("peer-B").unwrap().contains("nearby"));
        assert!(topo.cluster_for_peer("peer-C").unwrap().contains("remote"));
    }

    #[test]
    fn test_latency_grouping() {
        let mut topo = NetworkTopology::new();
        topo.update_latency("peer-1", 3.0);
        topo.update_latency("peer-2", 7.0);
        topo.update_latency("peer-3", 25.0);
        topo.update_latency("peer-4", 80.0);
        topo.assign_clusters("me");

        // peer-1, peer-2 in same (local) cluster
        let c1 = topo.cluster_for_peer("peer-1").unwrap();
        let c2 = topo.cluster_for_peer("peer-2").unwrap();
        assert_eq!(c1, c2);

        // peer-3 in nearby
        let c3 = topo.cluster_for_peer("peer-3").unwrap();
        assert_ne!(c1, c3);
        assert!(c3.contains("nearby"));

        // peer-4 in remote
        let c4 = topo.cluster_for_peer("peer-4").unwrap();
        assert!(c4.contains("remote"));
    }

    #[test]
    fn test_cluster_selection() {
        let mut topo = NetworkTopology::new();
        topo.update_latency("fast-peer", 2.0);
        topo.update_latency("slow-peer", 100.0);
        topo.assign_clusters("me");

        let same = topo.peers_in_same_cluster("me");
        assert!(same.contains(&"me".to_string()));
        assert!(same.contains(&"fast-peer".to_string()));
        assert!(!same.contains(&"slow-peer".to_string()));
    }

    #[test]
    fn test_latency_update_averaging() {
        let mut lat = PeerLatency::new("p1".to_string());
        lat.update(10.0);
        lat.update(20.0);
        assert!((lat.avg_latency_ms - 15.0).abs() < 0.01);
        assert_eq!(lat.sample_count, 2);
    }

    #[test]
    fn test_empty_topology() {
        let mut topo = NetworkTopology::new();
        topo.assign_clusters("me");
        // Only local cluster with just the local node
        assert_eq!(topo.all_clusters().len(), 1);
        assert!(topo.cluster_for_peer("me").is_some());
    }

    #[test]
    fn test_tier_thresholds() {
        let mut p = PeerLatency::new("x".to_string());
        p.avg_latency_ms = 9.9;
        assert_eq!(p.cluster_tier(), ClusterTier::Same);
        p.avg_latency_ms = 10.0;
        assert_eq!(p.cluster_tier(), ClusterTier::Nearby);
        p.avg_latency_ms = 29.9;
        assert_eq!(p.cluster_tier(), ClusterTier::Nearby);
        p.avg_latency_ms = 30.0;
        assert_eq!(p.cluster_tier(), ClusterTier::Remote);
    }
}
