use crate::cluster::{Cluster, ClusterTier};
use crate::latency::PeerLatency;
use libp2p::PeerId;
use std::collections::HashMap;

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
                ClusterTier::Local => same.push(pid.clone()),
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
            println!("[Cluster] Assigned node {} to cluster {}", pid, cid);
        }
        self.clusters.insert(
            cid.clone(),
            Cluster {
                id: cid,
                nodes: parse_peer_ids(&nodes),
                avg_latency: avg as f32,
                tier: ClusterTier::Local,
            },
        );

        if !nearby.is_empty() {
            let avg = self.avg_latency_for(&nearby);
            let cid = format!("cluster-{}-nearby", local_short);
            for pid in &nearby {
                self.peer_cluster.insert(pid.clone(), cid.clone());
                println!("[Cluster] Assigned node {} to cluster {}", pid, cid);
            }
            self.clusters.insert(
                cid.clone(),
                Cluster {
                    id: cid,
                    nodes: parse_peer_ids(&nearby),
                    avg_latency: avg as f32,
                    tier: ClusterTier::Nearby,
                },
            );
        }

        if !remote.is_empty() {
            let avg = self.avg_latency_for(&remote);
            let cid = format!("cluster-{}-remote", local_short);
            for pid in &remote {
                self.peer_cluster.insert(pid.clone(), cid.clone());
                println!("[Cluster] Assigned node {} to cluster {}", pid, cid);
            }
            self.clusters.insert(
                cid.clone(),
                Cluster {
                    id: cid,
                    nodes: parse_peer_ids(&remote),
                    avg_latency: avg as f32,
                    tier: ClusterTier::Remote,
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
            .map(|c| c.nodes.iter().map(ToString::to_string).collect())
            .unwrap_or_default()
    }

    pub fn display(&self) {
        println!(
            "🌐 Network Topology ({} peers, {} clusters) [latency: real ping RTT]",
            self.peers.len(),
            self.clusters.len()
        );
        let mut sorted: Vec<&Cluster> = self.clusters.values().collect();
        sorted.sort_by(|a, b| {
            a.avg_latency
                .partial_cmp(&b.avg_latency)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        for cluster in sorted {
            let tier = match cluster.tier {
                ClusterTier::Local => "LOCAL",
                ClusterTier::Nearby => "NEARBY",
                ClusterTier::Remote => "REMOTE",
            };
            println!(
                "   📦 {} [{}] ({:.1}ms avg, {} nodes)",
                cluster.id,
                tier,
                cluster.avg_latency,
                cluster.nodes.len()
            );
            for node in &cluster.nodes {
                let node_str = node.to_string();
                let short = &node_str[..12.min(node_str.len())];
                let lat = self
                    .peers
                    .get(&node_str)
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

fn parse_peer_ids(peer_ids: &[String]) -> Vec<PeerId> {
    peer_ids
        .iter()
        .filter_map(|peer_id| peer_id.parse::<PeerId>().ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use libp2p::identity;

    fn peer_id() -> String {
        identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id()
            .to_string()
    }

    #[test]
    fn test_cluster_detection() {
        let mut topo = NetworkTopology::new();
        let local = peer_id();
        let peer_a = peer_id();
        let peer_b = peer_id();
        let peer_c = peer_id();
        topo.update_latency(&peer_a, 5.0);
        topo.update_latency(&peer_b, 20.0);
        topo.update_latency(&peer_c, 60.0);
        topo.assign_clusters(&local);

        assert_eq!(topo.all_clusters().len(), 3); // local, nearby, remote
        assert!(topo.cluster_for_peer(&peer_a).unwrap().contains("local"));
        assert!(topo.cluster_for_peer(&peer_b).unwrap().contains("nearby"));
        assert!(topo.cluster_for_peer(&peer_c).unwrap().contains("remote"));
    }

    #[test]
    fn test_cluster_assignment() {
        let mut topo = NetworkTopology::new();
        let local = peer_id();
        let peer_1 = peer_id();
        let peer_2 = peer_id();
        let peer_3 = peer_id();
        let peer_4 = peer_id();
        topo.update_latency(&peer_1, 3.0);
        topo.update_latency(&peer_2, 7.0);
        topo.update_latency(&peer_3, 25.0);
        topo.update_latency(&peer_4, 80.0);
        topo.assign_clusters(&local);

        // peer-1, peer-2 in same (local) cluster
        let c1 = topo.cluster_for_peer(&peer_1).unwrap();
        let c2 = topo.cluster_for_peer(&peer_2).unwrap();
        assert_eq!(c1, c2);

        // peer-3 in nearby
        let c3 = topo.cluster_for_peer(&peer_3).unwrap();
        assert_ne!(c1, c3);
        assert!(c3.contains("nearby"));

        // peer-4 in remote
        let c4 = topo.cluster_for_peer(&peer_4).unwrap();
        assert!(c4.contains("remote"));
    }

    #[test]
    fn test_scheduler_prefers_local_cluster() {
        let mut topo = NetworkTopology::new();
        let local = peer_id();
        let fast_peer = peer_id();
        let slow_peer = peer_id();
        topo.update_latency(&fast_peer, 2.0);
        topo.update_latency(&slow_peer, 100.0);
        topo.assign_clusters(&local);

        let same = topo.peers_in_same_cluster(&local);
        assert!(same.contains(&local));
        assert!(same.contains(&fast_peer));
        assert!(!same.contains(&slow_peer));
    }

    #[test]
    fn test_latency_update() {
        let mut lat = PeerLatency::new("p1".to_string());
        lat.update(10.0);
        lat.update(20.0);
        assert!((lat.avg_latency_ms - 15.0).abs() < 0.01);
        assert_eq!(lat.sample_count, 2);
    }

    #[test]
    fn test_empty_topology() {
        let mut topo = NetworkTopology::new();
        let local = peer_id();
        topo.assign_clusters(&local);
        // Only local cluster with just the local node
        assert_eq!(topo.all_clusters().len(), 1);
        assert!(topo.cluster_for_peer(&local).is_some());
    }

    #[test]
    fn test_tier_thresholds() {
        let mut p = PeerLatency::new("x".to_string());
        p.avg_latency_ms = 9.9;
        assert_eq!(p.cluster_tier(), ClusterTier::Local);
        p.avg_latency_ms = 10.0;
        assert_eq!(p.cluster_tier(), ClusterTier::Nearby);
        p.avg_latency_ms = 49.9;
        assert_eq!(p.cluster_tier(), ClusterTier::Nearby);
        p.avg_latency_ms = 50.0;
        assert_eq!(p.cluster_tier(), ClusterTier::Remote);
    }
}
