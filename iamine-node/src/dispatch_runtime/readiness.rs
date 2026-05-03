use super::*;

pub(crate) struct DispatchReadinessError {
    pub(crate) code: &'static str,
    pub(crate) reason: &'static str,
}

impl DispatchReadinessError {
    pub(crate) fn new(code: &'static str, reason: &'static str) -> Self {
        Self { code, reason }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DispatchReadinessSnapshot {
    pub(crate) connected_peer_count: usize,
    pub(crate) mesh_peer_count: usize,
    pub(crate) topic_peer_count: usize,
    pub(crate) joined_task_topic: bool,
    pub(crate) joined_direct_topic: bool,
    pub(crate) joined_results_topic: bool,
    pub(crate) selected_topic: &'static str,
}

pub(crate) fn topic_hash_string(topic_name: &str) -> String {
    gossipsub::IdentTopic::new(topic_name).hash().to_string()
}

pub(crate) fn evaluate_dispatch_readiness(
    snapshot: &DispatchReadinessSnapshot,
) -> Result<(), DispatchReadinessError> {
    if snapshot.connected_peer_count == 0 {
        return Err(DispatchReadinessError::new(
            NETWORK_NO_PUBSUB_PEERS_001,
            "no_connected_peers",
        ));
    }

    if !snapshot.joined_task_topic
        || !snapshot.joined_direct_topic
        || !snapshot.joined_results_topic
    {
        return Err(DispatchReadinessError::new(
            PUBSUB_TOPIC_NOT_READY_001,
            "required_topics_not_joined",
        ));
    }

    if snapshot.mesh_peer_count == 0 {
        return Err(DispatchReadinessError::new(
            NETWORK_NO_PUBSUB_PEERS_001,
            "no_pubsub_mesh_peers",
        ));
    }

    if snapshot.topic_peer_count == 0 {
        return Err(DispatchReadinessError::new(
            PUBSUB_TOPIC_NOT_READY_001,
            "destination_topic_has_zero_peers",
        ));
    }

    Ok(())
}

pub(crate) fn should_print_result_output(output: &str) -> bool {
    !output.trim().is_empty()
}
