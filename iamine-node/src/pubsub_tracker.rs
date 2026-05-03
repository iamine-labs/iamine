use super::*;

#[derive(Debug, Clone, Default)]
pub(super) struct PubsubTopicTracker {
    joined_topic_hashes: HashSet<String>,
    topic_peers: HashMap<String, HashSet<String>>,
}

impl PubsubTopicTracker {
    pub(super) fn register_local_subscription(&mut self, topic_name: &str) {
        self.joined_topic_hashes
            .insert(topic_hash_string(topic_name));
    }

    pub(super) fn register_peer_subscription(
        &mut self,
        peer_id: &PeerId,
        topic: &gossipsub::TopicHash,
    ) {
        self.topic_peers
            .entry(topic.to_string())
            .or_default()
            .insert(peer_id.to_string());
    }

    pub(super) fn unregister_peer_subscription(
        &mut self,
        peer_id: &PeerId,
        topic: &gossipsub::TopicHash,
    ) {
        if let Some(peers) = self.topic_peers.get_mut(&topic.to_string()) {
            peers.remove(&peer_id.to_string());
            if peers.is_empty() {
                self.topic_peers.remove(&topic.to_string());
            }
        }
    }

    pub(super) fn unregister_peer(&mut self, peer_id: &PeerId) {
        let peer = peer_id.to_string();
        self.topic_peers.retain(|_, peers| {
            peers.remove(&peer);
            !peers.is_empty()
        });
    }

    pub(super) fn topic_peer_count(&self, topic_name: &str) -> usize {
        self.topic_peers
            .get(&topic_hash_string(topic_name))
            .map(|peers| peers.len())
            .unwrap_or(0)
    }

    pub(super) fn mesh_peer_count(&self) -> usize {
        self.topic_peers
            .values()
            .flat_map(|peers| peers.iter().cloned())
            .collect::<HashSet<_>>()
            .len()
    }

    pub(super) fn joined(&self, topic_name: &str) -> bool {
        self.joined_topic_hashes
            .contains(&topic_hash_string(topic_name))
    }
}
