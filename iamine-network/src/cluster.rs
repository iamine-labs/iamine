use libp2p::PeerId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterTier {
    Local,
    Nearby,
    Remote,
}

#[derive(Debug, Clone)]
pub struct Cluster {
    pub id: String,
    pub nodes: Vec<PeerId>,
    pub avg_latency: f32,
    pub tier: ClusterTier,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ClusterRelation {
    Same,
    Nearby,
    Remote,
}

pub fn tier_from_cluster_id(cluster_id: &str) -> ClusterTier {
    if cluster_id.ends_with("-local") {
        ClusterTier::Local
    } else if cluster_id.ends_with("-nearby") {
        ClusterTier::Nearby
    } else {
        ClusterTier::Remote
    }
}

pub fn relation_for_cluster(
    cluster_id: Option<&str>,
    local_cluster_id: Option<&str>,
) -> ClusterRelation {
    match (cluster_id, local_cluster_id) {
        (Some(cluster), Some(local)) if cluster == local => ClusterRelation::Same,
        (Some(cluster), _) if matches!(tier_from_cluster_id(cluster), ClusterTier::Nearby) => {
            ClusterRelation::Nearby
        }
        _ => ClusterRelation::Remote,
    }
}
