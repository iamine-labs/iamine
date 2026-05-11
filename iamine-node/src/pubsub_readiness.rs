use iamine_network::{NETWORK_NO_PUBSUB_PEERS_001, PUBSUB_TOPIC_NOT_READY_001};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DispatchReadinessError {
    pub(crate) code: &'static str,
    pub(crate) reason: &'static str,
}

impl DispatchReadinessError {
    fn new(code: &'static str, reason: &'static str) -> Self {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DIRECT_INF_TOPIC, RESULTS_TOPIC, TASK_TOPIC};

    #[test]
    fn pubsub_readiness_requires_subscriber_not_only_connected_peer() {
        let snapshot = DispatchReadinessSnapshot {
            connected_peer_count: 3,
            mesh_peer_count: 0,
            topic_peer_count: 0,
            joined_task_topic: true,
            joined_direct_topic: true,
            joined_results_topic: true,
            selected_topic: TASK_TOPIC,
        };

        let error = evaluate_dispatch_readiness(&snapshot).unwrap_err();
        assert_eq!(error.code, NETWORK_NO_PUBSUB_PEERS_001);
        assert_eq!(error.reason, "no_pubsub_mesh_peers");
    }

    #[test]
    fn pubsub_readiness_ready_with_real_topic_subscriber() {
        let snapshot = DispatchReadinessSnapshot {
            connected_peer_count: 3,
            mesh_peer_count: 1,
            topic_peer_count: 1,
            joined_task_topic: true,
            joined_direct_topic: true,
            joined_results_topic: true,
            selected_topic: TASK_TOPIC,
        };

        assert!(evaluate_dispatch_readiness(&snapshot).is_ok());
    }

    #[test]
    fn pubsub_readiness_insufficient_peers_before_subscribers() {
        let snapshot = DispatchReadinessSnapshot {
            connected_peer_count: 2,
            mesh_peer_count: 2,
            topic_peer_count: 0,
            joined_task_topic: true,
            joined_direct_topic: true,
            joined_results_topic: true,
            selected_topic: TASK_TOPIC,
        };

        let error = evaluate_dispatch_readiness(&snapshot).unwrap_err();
        assert_eq!(error.code, PUBSUB_TOPIC_NOT_READY_001);
        assert_eq!(error.reason, "destination_topic_has_zero_peers");
    }

    #[test]
    fn pubsub_readiness_passes_after_subscriber_tracking() {
        for count in 1..=3 {
            let snapshot = DispatchReadinessSnapshot {
                connected_peer_count: 3,
                mesh_peer_count: count,
                topic_peer_count: count,
                joined_task_topic: true,
                joined_direct_topic: true,
                joined_results_topic: true,
                selected_topic: TASK_TOPIC,
            };

            assert!(evaluate_dispatch_readiness(&snapshot).is_ok());
        }
    }

    #[test]
    fn pubsub_readiness_requires_required_local_topic_joins() {
        let snapshot = DispatchReadinessSnapshot {
            connected_peer_count: 3,
            mesh_peer_count: 1,
            topic_peer_count: 1,
            joined_task_topic: true,
            joined_direct_topic: false,
            joined_results_topic: true,
            selected_topic: DIRECT_INF_TOPIC,
        };

        let error = evaluate_dispatch_readiness(&snapshot).unwrap_err();
        assert_eq!(error.code, PUBSUB_TOPIC_NOT_READY_001);
        assert_eq!(error.reason, "required_topics_not_joined");
    }

    #[test]
    fn pubsub_readiness_result_topic_can_be_selected() {
        let snapshot = DispatchReadinessSnapshot {
            connected_peer_count: 1,
            mesh_peer_count: 1,
            topic_peer_count: 1,
            joined_task_topic: true,
            joined_direct_topic: true,
            joined_results_topic: true,
            selected_topic: RESULTS_TOPIC,
        };

        assert!(evaluate_dispatch_readiness(&snapshot).is_ok());
    }
}
