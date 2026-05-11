use crate::pubsub_topics::{topic_hash_string, tracked_pubsub_topic_name, BROADCAST_PUBSUB_TOPICS};
use libp2p::{gossipsub, PeerId};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Default)]
pub(crate) struct PubsubTopicTracker {
    joined_topic_hashes: HashSet<String>,
    topic_peers: HashMap<String, HashSet<String>>,
}

impl PubsubTopicTracker {
    pub(crate) fn register_local_subscription(&mut self, topic_name: &str) {
        self.joined_topic_hashes
            .insert(topic_hash_string(topic_name));
    }

    pub(crate) fn register_peer_subscription(
        &mut self,
        peer_id: &PeerId,
        topic: &gossipsub::TopicHash,
    ) -> bool {
        self.topic_peers
            .entry(topic.to_string())
            .or_default()
            .insert(peer_id.to_string())
    }

    pub(crate) fn unregister_peer_subscription(
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

    pub(crate) fn unregister_peer(&mut self, peer_id: &PeerId) {
        let peer = peer_id.to_string();
        self.topic_peers.retain(|_, peers| {
            peers.remove(&peer);
            !peers.is_empty()
        });
    }

    pub(crate) fn topic_peer_count(&self, topic_name: &str) -> usize {
        self.topic_peers
            .get(&topic_hash_string(topic_name))
            .map(|peers| peers.len())
            .unwrap_or(0)
    }

    pub(crate) fn mesh_peer_count(&self) -> usize {
        self.topic_peers
            .values()
            .flat_map(|peers| peers.iter().cloned())
            .collect::<HashSet<_>>()
            .len()
    }

    pub(crate) fn joined(&self, topic_name: &str) -> bool {
        self.joined_topic_hashes
            .contains(&topic_hash_string(topic_name))
    }
}

pub(crate) fn gossipsub_topic_peer_count(
    behaviour: &gossipsub::Behaviour,
    topic_name: &str,
) -> usize {
    let target_hash = topic_hash_string(topic_name);
    behaviour
        .all_peers()
        .filter(|(_, topics)| topics.iter().any(|topic| topic.to_string() == target_hash))
        .count()
}

pub(crate) fn gossipsub_all_peers_per_topic_value(behaviour: &gossipsub::Behaviour) -> Value {
    let mut peers_by_topic: HashMap<&'static str, HashSet<String>> = HashMap::new();
    for (peer_id, topics) in behaviour.all_peers() {
        for topic in topics {
            if let Some(topic_name) = tracked_pubsub_topic_name(topic) {
                peers_by_topic
                    .entry(topic_name)
                    .or_default()
                    .insert(peer_id.to_string());
            }
        }
    }

    let mut fields = Map::new();
    for topic_name in BROADCAST_PUBSUB_TOPICS {
        let peers = peers_by_topic
            .remove(topic_name)
            .unwrap_or_default()
            .into_iter()
            .map(Value::String)
            .collect::<Vec<_>>();
        fields.insert(topic_name.to_string(), Value::Array(peers));
    }
    Value::Object(fields)
}

pub(crate) fn gossipsub_mesh_peer_ids(
    behaviour: &gossipsub::Behaviour,
    topic_name: &str,
) -> Vec<String> {
    let topic_hash = gossipsub::IdentTopic::new(topic_name).hash();
    behaviour
        .mesh_peers(&topic_hash)
        .map(|peer_id| peer_id.to_string())
        .collect()
}

pub(crate) fn sync_pubsub_tracker_from_gossipsub(
    tracker: &mut PubsubTopicTracker,
    behaviour: &gossipsub::Behaviour,
) -> Vec<(String, &'static str)> {
    let mut observed = Vec::new();
    for (peer_id, topics) in behaviour.all_peers() {
        for topic in topics {
            let inserted = tracker.register_peer_subscription(peer_id, topic);
            if inserted {
                if let Some(topic_name) = tracked_pubsub_topic_name(topic) {
                    observed.push((peer_id.to_string(), topic_name));
                }
            }
        }
    }
    observed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{RESULTS_TOPIC, TASK_TOPIC};

    #[test]
    fn pubsub_topic_tracker_deduplicates_peer_subscriptions() {
        let mut tracker = PubsubTopicTracker::default();
        let peer = PeerId::random();
        let topic = gossipsub::IdentTopic::new(TASK_TOPIC).hash();

        assert!(tracker.register_peer_subscription(&peer, &topic));
        assert!(!tracker.register_peer_subscription(&peer, &topic));
        assert_eq!(tracker.topic_peer_count(TASK_TOPIC), 1);
    }

    #[test]
    fn pubsub_topic_tracker_counts_subscribed_peers() {
        let mut tracker = PubsubTopicTracker::default();
        let task_topic = gossipsub::IdentTopic::new(TASK_TOPIC).hash();
        let result_topic = gossipsub::IdentTopic::new(RESULTS_TOPIC).hash();

        tracker.register_peer_subscription(&PeerId::random(), &task_topic);
        tracker.register_peer_subscription(&PeerId::random(), &task_topic);
        tracker.register_peer_subscription(&PeerId::random(), &result_topic);

        assert_eq!(tracker.topic_peer_count(TASK_TOPIC), 2);
        assert_eq!(tracker.topic_peer_count(RESULTS_TOPIC), 1);
        assert_eq!(tracker.mesh_peer_count(), 3);
    }

    #[test]
    fn pubsub_topic_tracker_handles_unknown_topic_safely() {
        let mut tracker = PubsubTopicTracker::default();
        let peer = PeerId::random();
        let unknown = gossipsub::IdentTopic::new("iamine-unknown").hash();

        assert!(tracker.register_peer_subscription(&peer, &unknown));
        assert_eq!(tracker.topic_peer_count(TASK_TOPIC), 0);
        assert_eq!(tracker.topic_peer_count("iamine-unknown"), 1);
    }

    #[test]
    fn pubsub_topic_tracker_removes_peer_from_all_topics() {
        let mut tracker = PubsubTopicTracker::default();
        let peer = PeerId::random();
        let task_topic = gossipsub::IdentTopic::new(TASK_TOPIC).hash();
        let result_topic = gossipsub::IdentTopic::new(RESULTS_TOPIC).hash();

        tracker.register_peer_subscription(&peer, &task_topic);
        tracker.register_peer_subscription(&peer, &result_topic);
        tracker.unregister_peer(&peer);

        assert_eq!(tracker.topic_peer_count(TASK_TOPIC), 0);
        assert_eq!(tracker.topic_peer_count(RESULTS_TOPIC), 0);
    }
}
