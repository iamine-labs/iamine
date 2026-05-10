use libp2p::gossipsub;

pub(crate) const TASK_TOPIC: &str = "iamine-tasks";
pub(crate) const CAP_TOPIC: &str = "iamine-capabilities";
pub(crate) const DIRECT_INF_TOPIC: &str = "iamine-direct-inference";
pub(crate) const RESULTS_TOPIC: &str = "iamine-results";
pub(crate) const BIDS_TOPIC: &str = "iamine-bids";
pub(crate) const ASSIGN_TOPIC: &str = "iamine-assign";
pub(crate) const HEARTBEAT_TOPIC: &str = "iamine-heartbeat";
pub(crate) const BROADCAST_PUBSUB_TOPICS: [&str; 7] = [
    TASK_TOPIC,
    BIDS_TOPIC,
    ASSIGN_TOPIC,
    RESULTS_TOPIC,
    HEARTBEAT_TOPIC,
    CAP_TOPIC,
    DIRECT_INF_TOPIC,
];

pub(crate) const BROADCAST_READINESS_TIMEOUT_MS: u64 = 45_000;
pub(crate) const BROADCAST_OFFER_RETRY_DELAY_MS: u64 = 1_000;
pub(crate) const BROADCAST_OFFER_MAX_RETRY_DELAY_MS: u64 = 5_000;
pub(crate) const BROADCAST_OFFER_MAX_ATTEMPTS: u32 = 20;

pub(crate) fn topic_hash_string(topic_name: &str) -> String {
    gossipsub::IdentTopic::new(topic_name).hash().to_string()
}

pub(crate) fn tracked_pubsub_topic_name(topic: &gossipsub::TopicHash) -> Option<&'static str> {
    let topic_hash = topic.to_string();
    BROADCAST_PUBSUB_TOPICS
        .iter()
        .copied()
        .find(|topic_name| topic_hash_string(topic_name) == topic_hash)
}

pub(crate) fn is_remote_delivery_topic(topic_name: &str) -> bool {
    matches!(topic_name, RESULTS_TOPIC | TASK_TOPIC | DIRECT_INF_TOPIC)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pubsub_topic_names_are_stable() {
        assert_eq!(TASK_TOPIC, "iamine-tasks");
        assert_eq!(RESULTS_TOPIC, "iamine-results");
        assert_eq!(BIDS_TOPIC, "iamine-bids");
        assert_eq!(ASSIGN_TOPIC, "iamine-assign");
        assert_eq!(CAP_TOPIC, "iamine-capabilities");
        assert_eq!(HEARTBEAT_TOPIC, "iamine-heartbeat");
        assert_eq!(DIRECT_INF_TOPIC, "iamine-direct-inference");
    }

    #[test]
    fn pubsub_delivery_topics_are_stable() {
        assert!(is_remote_delivery_topic(TASK_TOPIC));
        assert!(is_remote_delivery_topic(RESULTS_TOPIC));
        assert!(is_remote_delivery_topic(DIRECT_INF_TOPIC));
        assert!(!is_remote_delivery_topic(CAP_TOPIC));
        assert!(!is_remote_delivery_topic(HEARTBEAT_TOPIC));
    }

    #[test]
    fn tracked_pubsub_topic_name_handles_unknown_topic_safely() {
        let unknown = gossipsub::IdentTopic::new("iamine-unknown").hash();

        assert_eq!(tracked_pubsub_topic_name(&unknown), None);
    }
}
