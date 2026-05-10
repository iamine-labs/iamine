use crate::broadcast_protocol::broadcast_payload_preview;
use crate::{
    log_observability_event, ASSIGN_TOPIC, BROADCAST_OFFER_MAX_ATTEMPTS,
    BROADCAST_OFFER_MAX_RETRY_DELAY_MS, BROADCAST_OFFER_RETRY_DELAY_MS,
    BROADCAST_READINESS_TIMEOUT_MS, RESULTS_TOPIC, TASK_TOPIC,
};
use iamine_network::{LogLevel, TASK_DISPATCH_UNCONFIRMED_001};
use serde_json::{Map, Value};

#[derive(Debug, Clone)]
pub(crate) struct BroadcastOfferState {
    pub(crate) task_id: String,
    pub(crate) prepared: bool,
    pub(crate) published: bool,
    pub(crate) failed: bool,
    pub(crate) attempts: u32,
    pub(crate) scheduler_registered: bool,
    pub(crate) assignment_published: bool,
    pub(crate) assigned_worker: Option<String>,
    pub(crate) result_accepted: bool,
    pub(crate) started_at: tokio::time::Instant,
    pub(crate) last_attempt_at: Option<tokio::time::Instant>,
    pub(crate) last_readiness_log_at: Option<tokio::time::Instant>,
    pub(crate) last_subscription_event_at: Option<tokio::time::Instant>,
    pub(crate) last_peer_connected_at: Option<tokio::time::Instant>,
    pub(crate) last_publish_failure_reason: Option<String>,
}

impl BroadcastOfferState {
    pub(crate) fn new(task_id: String) -> Self {
        Self {
            task_id,
            prepared: false,
            published: false,
            failed: false,
            attempts: 0,
            scheduler_registered: false,
            assignment_published: false,
            assigned_worker: None,
            result_accepted: false,
            started_at: tokio::time::Instant::now(),
            last_attempt_at: None,
            last_readiness_log_at: None,
            last_subscription_event_at: None,
            last_peer_connected_at: None,
            last_publish_failure_reason: None,
        }
    }

    pub(crate) fn elapsed_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }

    pub(crate) fn can_attempt_publish(&self) -> bool {
        if self.published || self.failed || self.attempts >= BROADCAST_OFFER_MAX_ATTEMPTS {
            return false;
        }

        self.last_attempt_at
            .map(|last| last.elapsed().as_millis() as u64 >= self.current_retry_delay_ms())
            .unwrap_or(true)
    }

    pub(crate) fn record_publish_result(&mut self, success: bool, failure_reason: Option<&str>) {
        self.attempts = self.attempts.saturating_add(1);
        self.last_attempt_at = Some(tokio::time::Instant::now());
        if success {
            self.published = true;
            self.last_publish_failure_reason = None;
        } else if let Some(reason) = failure_reason {
            self.last_publish_failure_reason = Some(reason.to_string());
        }
    }

    pub(crate) fn current_retry_delay_ms(&self) -> u64 {
        match self.attempts {
            0 => BROADCAST_OFFER_RETRY_DELAY_MS,
            1 => 2_000,
            2 => 3_000,
            _ => BROADCAST_OFFER_MAX_RETRY_DELAY_MS,
        }
    }

    pub(crate) fn mark_subscription_seen(&mut self) {
        self.last_subscription_event_at = Some(tokio::time::Instant::now());
    }

    pub(crate) fn mark_peer_connected(&mut self) {
        self.last_peer_connected_at = Some(tokio::time::Instant::now());
    }

    pub(crate) fn mark_assigned(&mut self, worker_id: &str) {
        self.assignment_published = true;
        self.assigned_worker = Some(worker_id.to_string());
    }

    pub(crate) fn mark_result_accepted(&mut self) {
        self.result_accepted = true;
    }

    pub(crate) fn should_log_readiness_wait(&mut self) -> bool {
        let should_log = self
            .last_readiness_log_at
            .map(|last| last.elapsed().as_millis() as u64 >= BROADCAST_OFFER_RETRY_DELAY_MS)
            .unwrap_or(true);
        if should_log {
            self.last_readiness_log_at = Some(tokio::time::Instant::now());
        }
        should_log
    }
}

#[derive(Debug, Clone)]
pub(crate) struct BroadcastReadinessSnapshot {
    pub(crate) connected_peers: usize,
    pub(crate) subscribed_peers: usize,
    pub(crate) mesh_peers: usize,
    pub(crate) elapsed_ms: u64,
    pub(crate) attempts: u32,
    pub(crate) readiness_reason: &'static str,
    pub(crate) ready: bool,
    pub(crate) all_peers_per_topic: Option<Value>,
    pub(crate) gossipsub_mesh_peers: Vec<String>,
    pub(crate) last_publish_failure_reason: Option<String>,
}

impl BroadcastReadinessSnapshot {
    pub(crate) fn new(
        connected_peers: usize,
        subscribed_peers: usize,
        mesh_peers: usize,
        elapsed_ms: u64,
        attempts: u32,
    ) -> Self {
        let (ready, readiness_reason) = if mesh_peers > 0 {
            (true, "task_topic_mesh_ready")
        } else if subscribed_peers > 0 {
            (true, "task_topic_subscriber_seen")
        } else if connected_peers > 0 {
            (false, "waiting_for_task_topic_subscription")
        } else {
            (false, "waiting_for_connected_peers")
        };

        Self {
            connected_peers,
            subscribed_peers,
            mesh_peers,
            elapsed_ms,
            attempts,
            readiness_reason,
            ready,
            all_peers_per_topic: None,
            gossipsub_mesh_peers: Vec::new(),
            last_publish_failure_reason: None,
        }
    }

    pub(crate) fn with_gossipsub_details(
        mut self,
        all_peers_per_topic: Value,
        gossipsub_mesh_peers: Vec<String>,
        last_publish_failure_reason: Option<&str>,
    ) -> Self {
        self.all_peers_per_topic = Some(all_peers_per_topic);
        self.gossipsub_mesh_peers = gossipsub_mesh_peers;
        self.last_publish_failure_reason = last_publish_failure_reason.map(str::to_string);
        self
    }
}

pub(crate) fn broadcast_offer_ready(
    subscribed_task_peers: usize,
    mesh_peers: usize,
    connected_peers: usize,
    readiness_elapsed_ms: u64,
) -> bool {
    BroadcastReadinessSnapshot::new(
        connected_peers,
        subscribed_task_peers,
        mesh_peers,
        readiness_elapsed_ms,
        0,
    )
    .ready
}

fn add_broadcast_readiness_fields(
    fields: &mut Map<String, Value>,
    snapshot: &BroadcastReadinessSnapshot,
) {
    fields.insert(
        "connected_peers".to_string(),
        (snapshot.connected_peers as u64).into(),
    );
    fields.insert(
        "subscribed_peers".to_string(),
        (snapshot.subscribed_peers as u64).into(),
    );
    fields.insert(
        "mesh_peers".to_string(),
        (snapshot.mesh_peers as u64).into(),
    );
    fields.insert("elapsed_ms".to_string(), snapshot.elapsed_ms.into());
    fields.insert("attempts".to_string(), snapshot.attempts.into());
    fields.insert(
        "readiness_reason".to_string(),
        snapshot.readiness_reason.into(),
    );
    fields.insert("ready".to_string(), snapshot.ready.into());
    if let Some(all_peers_per_topic) = snapshot.all_peers_per_topic.clone() {
        fields.insert("all_peers_per_topic".to_string(), all_peers_per_topic);
    }
    if !snapshot.gossipsub_mesh_peers.is_empty() {
        fields.insert(
            "gossipsub_mesh_peers".to_string(),
            Value::Array(
                snapshot
                    .gossipsub_mesh_peers
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
    }
    if let Some(reason) = &snapshot.last_publish_failure_reason {
        fields.insert(
            "last_publish_failure_reason".to_string(),
            reason.clone().into(),
        );
    }
}

pub(crate) fn evaluate_broadcast_result_acceptance(
    state: Option<&BroadcastOfferState>,
    task_id: &str,
    worker_peer_id: &str,
    success: bool,
) -> Result<(), &'static str> {
    let Some(state) = state else {
        return Err("unknown_task_id");
    };
    if state.task_id != task_id {
        return Err("unknown_task_id");
    }
    if state.result_accepted {
        return Err("duplicate_result");
    }
    if !success {
        return Err("success_false");
    }
    match state.assigned_worker.as_deref() {
        Some(assigned_worker) if assigned_worker == worker_peer_id => Ok(()),
        Some(_) => Err("wrong_worker"),
        None => Err("task_not_assigned"),
    }
}

pub(crate) fn emit_broadcast_task_offer_prepared_event(
    task_id: &str,
    task_type: &str,
    data: &str,
    controller_peer_id: &str,
) {
    log_observability_event(
        LogLevel::Info,
        "broadcast_task_offer_prepared",
        task_id,
        Some(task_id),
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("task_type".to_string(), task_type.into());
            fields.insert("data".to_string(), data.into());
            fields.insert("requester_id".to_string(), controller_peer_id.into());
            fields.insert("origin_peer".to_string(), controller_peer_id.into());
            fields.insert("topic".to_string(), TASK_TOPIC.into());
            fields
        },
    );
}

pub(crate) fn emit_broadcast_readiness_waiting_event(
    task_id: &str,
    snapshot: &BroadcastReadinessSnapshot,
) {
    log_observability_event(
        LogLevel::Info,
        "broadcast_readiness_waiting",
        task_id,
        Some(task_id),
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("topic".to_string(), TASK_TOPIC.into());
            add_broadcast_readiness_fields(&mut fields, snapshot);
            fields
        },
    );
}

pub(crate) fn emit_broadcast_readiness_state_event(
    task_id: &str,
    snapshot: &BroadcastReadinessSnapshot,
) {
    log_observability_event(
        LogLevel::Info,
        "broadcast_readiness_state",
        task_id,
        Some(task_id),
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("topic".to_string(), TASK_TOPIC.into());
            add_broadcast_readiness_fields(&mut fields, snapshot);
            fields
        },
    );
}

pub(crate) fn emit_broadcast_topic_subscriber_seen_event(
    topic: &str,
    peer_id: &str,
    connected_peers: usize,
    subscribed_peers: usize,
    mesh_peers: usize,
    source_event: &str,
) {
    log_observability_event(
        LogLevel::Info,
        "broadcast_topic_subscriber_seen",
        "network",
        None,
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("topic".to_string(), topic.into());
            fields.insert("peer_id".to_string(), peer_id.into());
            fields.insert("source_event".to_string(), source_event.into());
            fields.insert(
                "subscribed_peers_count".to_string(),
                (subscribed_peers as u64).into(),
            );
            fields.insert(
                "connected_peers_count".to_string(),
                (connected_peers as u64).into(),
            );
            fields.insert("mesh_peers_count".to_string(), (mesh_peers as u64).into());
            fields
        },
    );
}

pub(crate) fn emit_observed_peer_subscription_event(
    event_name: &str,
    observer_peer_id: &str,
    observed_peer_id: &str,
    topic: &str,
    mode: &str,
    source_event: &str,
) {
    log_observability_event(LogLevel::Info, event_name, "network", None, None, None, {
        let mut fields = Map::new();
        fields.insert("observer_peer_id".to_string(), observer_peer_id.into());
        fields.insert("observed_peer_id".to_string(), observed_peer_id.into());
        fields.insert("topic".to_string(), topic.into());
        fields.insert(
            "direction".to_string(),
            "local_observed_remote_subscription".into(),
        );
        fields.insert("mode".to_string(), mode.into());
        fields.insert("source_event".to_string(), source_event.into());
        fields
    });
}

pub(crate) fn emit_broadcast_task_offer_publish_attempt_event(
    task_id: &str,
    task_type: &str,
    data: &str,
    payload_size: usize,
    attempt_number: u32,
    snapshot: &BroadcastReadinessSnapshot,
) {
    log_observability_event(
        LogLevel::Info,
        "broadcast_task_offer_publish_attempt",
        task_id,
        Some(task_id),
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("task_type".to_string(), task_type.into());
            fields.insert("topic".to_string(), TASK_TOPIC.into());
            fields.insert(
                "payload_preview".to_string(),
                broadcast_payload_preview(data).into(),
            );
            fields.insert("payload_size".to_string(), (payload_size as u64).into());
            fields.insert("attempt_number".to_string(), attempt_number.into());
            add_broadcast_readiness_fields(&mut fields, snapshot);
            fields
        },
    );
}

pub(crate) fn emit_broadcast_task_offer_published_event(
    task_id: &str,
    task_type: &str,
    data: &str,
    message_id: &str,
    attempt_number: u32,
    snapshot: &BroadcastReadinessSnapshot,
) {
    log_observability_event(
        LogLevel::Info,
        "broadcast_task_offer_published",
        task_id,
        Some(task_id),
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("task_type".to_string(), task_type.into());
            fields.insert("data".to_string(), data.into());
            fields.insert(
                "payload_preview".to_string(),
                broadcast_payload_preview(data).into(),
            );
            fields.insert("topic".to_string(), TASK_TOPIC.into());
            fields.insert("message_id".to_string(), message_id.into());
            fields.insert("attempt_number".to_string(), attempt_number.into());
            add_broadcast_readiness_fields(&mut fields, snapshot);
            fields
        },
    );
}

pub(crate) fn emit_broadcast_task_offer_publish_failed_event(
    task_id: &str,
    task_type: &str,
    reason: &str,
    attempt_number: u32,
    recoverable: bool,
    snapshot: &BroadcastReadinessSnapshot,
) {
    log_observability_event(
        LogLevel::Error,
        "broadcast_task_offer_publish_failed",
        task_id,
        Some(task_id),
        None,
        Some(TASK_DISPATCH_UNCONFIRMED_001),
        {
            let mut fields = Map::new();
            fields.insert("task_type".to_string(), task_type.into());
            fields.insert("topic".to_string(), TASK_TOPIC.into());
            fields.insert("reason".to_string(), reason.into());
            fields.insert("attempt_number".to_string(), attempt_number.into());
            fields.insert("recoverable".to_string(), recoverable.into());
            add_broadcast_readiness_fields(&mut fields, snapshot);
            fields
        },
    );
}

pub(crate) fn emit_broadcast_task_offer_readiness_timeout_event(
    task_id: &str,
    task_type: &str,
    snapshot: &BroadcastReadinessSnapshot,
    last_failure_reason: Option<&str>,
) {
    log_observability_event(
        LogLevel::Error,
        "broadcast_task_offer_readiness_timeout",
        task_id,
        Some(task_id),
        None,
        Some(TASK_DISPATCH_UNCONFIRMED_001),
        {
            let mut fields = Map::new();
            fields.insert("task_type".to_string(), task_type.into());
            fields.insert("topic".to_string(), TASK_TOPIC.into());
            fields.insert(
                "timeout_ms".to_string(),
                BROADCAST_READINESS_TIMEOUT_MS.into(),
            );
            if let Some(reason) = last_failure_reason {
                fields.insert("last_publish_failure_reason".to_string(), reason.into());
            }
            add_broadcast_readiness_fields(&mut fields, snapshot);
            fields
        },
    );
}

pub(crate) fn emit_broadcast_bid_received_event(
    task_id: &str,
    worker_id: &str,
    reputation_score: u32,
    available_slots: usize,
    latency_ms: f64,
) {
    log_observability_event(
        LogLevel::Info,
        "broadcast_bid_received",
        task_id,
        Some(task_id),
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("worker_id".to_string(), worker_id.into());
            fields.insert("reputation_score".to_string(), reputation_score.into());
            fields.insert(
                "available_slots".to_string(),
                (available_slots as u64).into(),
            );
            fields.insert("latency_ms".to_string(), latency_ms.into());
            fields
        },
    );
}

pub(crate) fn emit_broadcast_task_assign_published_event(
    task_id: &str,
    assigned_worker: &str,
    message_id: &str,
    deadline_ms: u64,
) {
    log_observability_event(
        LogLevel::Info,
        "broadcast_task_assign_published",
        task_id,
        Some(task_id),
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("assigned_worker".to_string(), assigned_worker.into());
            fields.insert("topic".to_string(), ASSIGN_TOPIC.into());
            fields.insert("message_id".to_string(), message_id.into());
            fields.insert("deadline_ms".to_string(), deadline_ms.into());
            fields
        },
    );
}

pub(crate) fn emit_controller_topic_subscribed_event(topic: &str, peer_id: &str) {
    log_observability_event(
        LogLevel::Info,
        "controller_topic_subscribed",
        "startup",
        None,
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("topic".to_string(), topic.into());
            fields.insert("peer_id".to_string(), peer_id.into());
            fields.insert("mode".to_string(), "broadcast".into());
            fields
        },
    );
}

pub(crate) fn emit_broadcast_pubsub_ready_event(peer_id: &str, topics: &[&str]) {
    log_observability_event(
        LogLevel::Info,
        "broadcast_pubsub_ready",
        "startup",
        None,
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("peer_id".to_string(), peer_id.into());
            fields.insert("mode".to_string(), "broadcast".into());
            fields.insert(
                "topics".to_string(),
                Value::Array(topics.iter().map(|topic| (*topic).into()).collect()),
            );
            fields
        },
    );
}

pub(crate) struct BroadcastResultReceived<'a> {
    pub(crate) task_id: &'a str,
    pub(crate) task_type: &'a str,
    pub(crate) worker_peer_id: &'a str,
    pub(crate) assigned_worker_peer_id: Option<&'a str>,
    pub(crate) origin_peer: Option<&'a str>,
    pub(crate) success: bool,
    pub(crate) output: &'a str,
    pub(crate) elapsed_ms: u64,
    pub(crate) transport: &'a str,
    pub(crate) accepted: bool,
    pub(crate) rejection_reason: Option<&'a str>,
}

pub(crate) fn emit_broadcast_result_received_events(result: &BroadcastResultReceived<'_>) {
    for event_name in [
        "task_result_received",
        "broadcast_result_received",
        "result_received",
    ] {
        log_observability_event(
            LogLevel::Info,
            event_name,
            result.task_id,
            Some(result.task_id),
            None,
            None,
            {
                let mut fields = Map::new();
                fields.insert("task_type".to_string(), result.task_type.into());
                fields.insert("worker_peer_id".to_string(), result.worker_peer_id.into());
                if let Some(assigned_worker_peer_id) = result.assigned_worker_peer_id {
                    fields.insert(
                        "assigned_worker_peer_id".to_string(),
                        assigned_worker_peer_id.into(),
                    );
                }
                if let Some(origin_peer) = result.origin_peer {
                    fields.insert("origin_peer".to_string(), origin_peer.into());
                    fields.insert("controller_peer_id".to_string(), origin_peer.into());
                }
                fields.insert("success".to_string(), result.success.into());
                fields.insert("output".to_string(), result.output.into());
                fields.insert(
                    "output_preview".to_string(),
                    broadcast_payload_preview(result.output).into(),
                );
                fields.insert("elapsed_ms".to_string(), result.elapsed_ms.into());
                fields.insert("transport".to_string(), result.transport.into());
                fields.insert("topic".to_string(), RESULTS_TOPIC.into());
                fields.insert("accepted".to_string(), result.accepted.into());
                if let Some(rejection_reason) = result.rejection_reason {
                    fields.insert("rejection_reason".to_string(), rejection_reason.into());
                }
                fields
            },
        );
    }
}

pub(crate) fn emit_broadcast_result_rejected_event(
    result: &BroadcastResultReceived<'_>,
    reason: &str,
) {
    log_observability_event(
        LogLevel::Warn,
        "broadcast_result_rejected",
        result.task_id,
        Some(result.task_id),
        None,
        Some(TASK_DISPATCH_UNCONFIRMED_001),
        {
            let mut fields = Map::new();
            fields.insert("task_type".to_string(), result.task_type.into());
            fields.insert("worker_peer_id".to_string(), result.worker_peer_id.into());
            if let Some(assigned_worker_peer_id) = result.assigned_worker_peer_id {
                fields.insert(
                    "assigned_worker_peer_id".to_string(),
                    assigned_worker_peer_id.into(),
                );
            }
            if let Some(origin_peer) = result.origin_peer {
                fields.insert("origin_peer".to_string(), origin_peer.into());
                fields.insert("controller_peer_id".to_string(), origin_peer.into());
            }
            fields.insert("success".to_string(), result.success.into());
            fields.insert("transport".to_string(), result.transport.into());
            fields.insert("topic".to_string(), RESULTS_TOPIC.into());
            fields.insert("accepted".to_string(), false.into());
            fields.insert("rejection_reason".to_string(), reason.into());
            fields
        },
    );
}

pub(crate) fn emit_broadcast_recovery_cancelled_event(task_id: &str, worker_peer_id: &str) {
    log_observability_event(
        LogLevel::Info,
        "broadcast_recovery_cancelled",
        task_id,
        Some(task_id),
        None,
        None,
        {
            let mut fields = Map::new();
            fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
            fields.insert("reason".to_string(), "result_accepted".into());
            fields
        },
    );
}

pub(crate) fn emit_broadcast_final_outcome_success_event(
    task_id: &str,
    worker_peer_id: &str,
    output: &str,
) {
    for event_name in ["final_outcome", "final_outcome_success"] {
        log_observability_event(
            LogLevel::Info,
            event_name,
            task_id,
            Some(task_id),
            None,
            None,
            {
                let mut fields = Map::new();
                fields.insert("outcome".to_string(), "success".into());
                fields.insert("failed".to_string(), false.into());
                fields.insert("worker_peer_id".to_string(), worker_peer_id.into());
                fields.insert("output".to_string(), output.into());
                fields.insert(
                    "output_preview".to_string(),
                    broadcast_payload_preview(output).into(),
                );
                fields
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broadcast_worker::emit_worker_pubsub_ready_event;
    use crate::broadcast_worker::emit_worker_topic_subscribed_event;
    use crate::{PubsubTopicTracker, BIDS_TOPIC, BROADCAST_PUBSUB_TOPICS};
    use libp2p::{gossipsub, PeerId};
    use std::collections::HashSet;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn test_id(prefix: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("{prefix}-{nanos}")
    }

    #[test]
    fn broadcast_waits_for_task_topic_subscriber_before_publish() {
        assert!(!broadcast_offer_ready(0, 0, 0, 0));
        assert!(!broadcast_offer_ready(0, 0, 1, 60_000));
    }

    #[test]
    fn broadcast_publishes_after_task_topic_subscriber_seen() {
        assert!(broadcast_offer_ready(1, 0, 0, 0));
    }

    #[test]
    fn broadcast_marks_ready_when_mesh_peer_present() {
        assert!(broadcast_offer_ready(0, 1, 0, 0));
    }

    #[test]
    fn controller_records_worker_subscription_to_iamine_tasks() {
        let mut tracker = PubsubTopicTracker::default();
        let worker_peer = PeerId::random();
        let inserted = tracker.register_peer_subscription(
            &worker_peer,
            &gossipsub::IdentTopic::new(TASK_TOPIC).hash(),
        );

        assert!(inserted);
        assert_eq!(tracker.topic_peer_count(TASK_TOPIC), 1);
    }

    #[test]
    fn broadcast_does_not_mark_ready_from_connected_peers_only() {
        let snapshot = BroadcastReadinessSnapshot::new(3, 0, 0, 60_000, 0);

        assert!(!snapshot.ready);
        assert_eq!(
            snapshot.readiness_reason,
            "waiting_for_task_topic_subscription"
        );
    }

    #[test]
    fn broadcast_marks_ready_when_task_subscriber_present() {
        let snapshot = BroadcastReadinessSnapshot::new(3, 1, 0, 500, 0);

        assert!(snapshot.ready);
        assert_eq!(snapshot.readiness_reason, "task_topic_subscriber_seen");
    }

    #[test]
    fn broadcast_publishes_initial_task_offer_once() {
        let mut state = BroadcastOfferState::new("broadcast-task-once".to_string());

        assert!(state.can_attempt_publish());
        state.record_publish_result(true, None);

        assert!(state.published);
        assert_eq!(state.attempts, 1);
        assert!(!state.can_attempt_publish());
    }

    #[test]
    fn broadcast_retries_offer_publish_if_publish_fails() {
        let mut state = BroadcastOfferState::new("broadcast-task-retry".to_string());

        state.record_publish_result(false, Some("InsufficientPeers"));
        assert!(!state.published);
        assert!(!state.failed);
        assert_eq!(
            state.last_publish_failure_reason.as_deref(),
            Some("InsufficientPeers")
        );
        state.last_attempt_at = Some(
            tokio::time::Instant::now() - Duration::from_millis(state.current_retry_delay_ms()),
        );
        assert!(state.can_attempt_publish());
    }

    #[test]
    fn broadcast_insufficient_peers_triggers_backoff_not_immediate_terminal_failure() {
        let mut state = BroadcastOfferState::new("broadcast-task-insufficient".to_string());

        state.record_publish_result(false, Some("InsufficientPeers"));
        assert!(!state.failed);
        assert!(!state.can_attempt_publish());
        assert_eq!(state.attempts, 1);
        assert_eq!(
            state.last_publish_failure_reason.as_deref(),
            Some("InsufficientPeers")
        );
    }

    #[test]
    fn insufficient_peers_does_not_retry_publish_without_subscriber_or_mesh() {
        let mut state = BroadcastOfferState::new("broadcast-task-no-subscribers".to_string());
        state.record_publish_result(false, Some("InsufficientPeers"));
        state.last_attempt_at = Some(
            tokio::time::Instant::now() - Duration::from_millis(state.current_retry_delay_ms() + 1),
        );
        let snapshot = BroadcastReadinessSnapshot::new(3, 0, 0, 60_000, state.attempts);

        assert!(state.can_attempt_publish());
        assert!(!snapshot.ready);
        assert!(!broadcast_offer_ready(
            snapshot.subscribed_peers,
            snapshot.mesh_peers,
            snapshot.connected_peers,
            snapshot.elapsed_ms
        ));
    }

    #[test]
    fn broadcast_topic_subscriber_seen_emitted_on_worker_subscription() {
        let peer_id = test_id("worker-subscription");

        emit_broadcast_topic_subscriber_seen_event(
            TASK_TOPIC,
            &peer_id,
            3,
            1,
            0,
            "gossipsub_subscription",
        );

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| entry.event == "broadcast_topic_subscriber_seen")
            .expect("subscriber seen event should be present");

        assert_eq!(
            entry.fields.get("topic").and_then(|value| value.as_str()),
            Some(TASK_TOPIC)
        );
        assert_eq!(
            entry.fields.get("peer_id").and_then(|value| value.as_str()),
            Some(peer_id.as_str())
        );
        assert_eq!(
            entry
                .fields
                .get("source_event")
                .and_then(|value| value.as_str()),
            Some("gossipsub_subscription")
        );
    }

    #[test]
    fn broadcast_readiness_state_includes_peer_counts() {
        let task_id = test_id("broadcast-readiness-state");
        let snapshot = BroadcastReadinessSnapshot::new(3, 2, 1, 2_500, 4).with_gossipsub_details(
            serde_json::json!({
                TASK_TOPIC: ["worker-peer"],
                BIDS_TOPIC: [],
            }),
            vec!["worker-peer".to_string()],
            Some("InsufficientPeers"),
        );

        emit_broadcast_readiness_state_event(&task_id, &snapshot);

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| entry.event == "broadcast_readiness_state" && entry.trace_id == task_id)
            .expect("readiness event should be present");

        assert_eq!(
            entry.fields.get("topic").and_then(|value| value.as_str()),
            Some(TASK_TOPIC)
        );
        assert_eq!(
            entry
                .fields
                .get("connected_peers")
                .and_then(|value| value.as_u64()),
            Some(3)
        );
        assert_eq!(
            entry
                .fields
                .get("subscribed_peers")
                .and_then(|value| value.as_u64()),
            Some(2)
        );
        assert_eq!(
            entry
                .fields
                .get("mesh_peers")
                .and_then(|value| value.as_u64()),
            Some(1)
        );
        assert_eq!(
            entry
                .fields
                .get("attempts")
                .and_then(|value| value.as_u64()),
            Some(4)
        );
        assert_eq!(
            entry
                .fields
                .get("last_publish_failure_reason")
                .and_then(|value| value.as_str()),
            Some("InsufficientPeers")
        );
        assert!(entry.fields.get("all_peers_per_topic").is_some());
        assert!(entry.fields.get("gossipsub_mesh_peers").is_some());
    }

    #[test]
    fn broadcast_readiness_state_reports_zero_subscribers_explicitly() {
        let task_id = test_id("broadcast-zero-subscribers");
        let snapshot = BroadcastReadinessSnapshot::new(3, 0, 0, 30_000, 2);

        emit_broadcast_readiness_state_event(&task_id, &snapshot);

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| entry.event == "broadcast_readiness_state" && entry.trace_id == task_id)
            .expect("readiness event should be present");

        assert_eq!(
            entry
                .fields
                .get("connected_peers")
                .and_then(|value| value.as_u64()),
            Some(3)
        );
        assert_eq!(
            entry
                .fields
                .get("subscribed_peers")
                .and_then(|value| value.as_u64()),
            Some(0)
        );
        assert_eq!(
            entry
                .fields
                .get("mesh_peers")
                .and_then(|value| value.as_u64()),
            Some(0)
        );
        assert_eq!(
            entry.fields.get("ready").and_then(|value| value.as_bool()),
            Some(false)
        );
    }

    #[test]
    fn controller_emits_topic_subscribed_for_required_topics() {
        let peer_id = test_id("controller-topic-test");
        for topic in BROADCAST_PUBSUB_TOPICS {
            emit_controller_topic_subscribed_event(topic, &peer_id);
        }
        emit_broadcast_pubsub_ready_event(&peer_id, &BROADCAST_PUBSUB_TOPICS);

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let topics: HashSet<String> = entries
            .iter()
            .filter(|entry| entry.event == "controller_topic_subscribed")
            .filter(|entry| {
                entry.fields.get("peer_id").and_then(|value| value.as_str())
                    == Some(peer_id.as_str())
            })
            .filter_map(|entry| {
                entry
                    .fields
                    .get("topic")
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
            })
            .collect();

        for topic in BROADCAST_PUBSUB_TOPICS {
            assert!(topics.contains(topic), "missing controller topic {topic}");
        }
        assert!(entries.iter().rev().any(|entry| {
            entry.event == "broadcast_pubsub_ready"
                && entry.fields.get("peer_id").and_then(|value| value.as_str())
                    == Some(peer_id.as_str())
        }));
    }

    #[test]
    fn worker_and_controller_observed_peer_subscription_events_are_emitted() {
        let observer_peer_id = test_id("observer");
        let observed_peer_id = test_id("observed");

        emit_observed_peer_subscription_event(
            "controller_observed_peer_subscription",
            &observer_peer_id,
            &observed_peer_id,
            TASK_TOPIC,
            "broadcast",
            "gossipsub_subscription",
        );
        emit_observed_peer_subscription_event(
            "worker_observed_peer_subscription",
            &observed_peer_id,
            &observer_peer_id,
            TASK_TOPIC,
            "worker",
            "gossipsub_subscription",
        );

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        assert!(entries.iter().rev().any(|entry| {
            entry.event == "controller_observed_peer_subscription"
                && entry
                    .fields
                    .get("observer_peer_id")
                    .and_then(|value| value.as_str())
                    == Some(observer_peer_id.as_str())
                && entry.fields.get("topic").and_then(|value| value.as_str()) == Some(TASK_TOPIC)
        }));
        assert!(entries.iter().rev().any(|entry| {
            entry.event == "worker_observed_peer_subscription"
                && entry
                    .fields
                    .get("observer_peer_id")
                    .and_then(|value| value.as_str())
                    == Some(observed_peer_id.as_str())
                && entry.fields.get("topic").and_then(|value| value.as_str()) == Some(TASK_TOPIC)
        }));
    }

    #[test]
    fn broadcast_does_not_spin_all_retries_before_subscription_propagates() {
        let mut state = BroadcastOfferState::new("broadcast-task-backoff".to_string());
        state.record_publish_result(false, Some("InsufficientPeers"));

        assert_eq!(state.attempts, 1);
        assert!(!state.can_attempt_publish());

        state.last_attempt_at = Some(
            tokio::time::Instant::now() - Duration::from_millis(state.current_retry_delay_ms() - 1),
        );
        assert!(!state.can_attempt_publish());

        state.mark_subscription_seen();
        assert!(broadcast_offer_ready(1, 0, 1, state.elapsed_ms()));
    }

    #[test]
    fn broadcast_task_offer_published_after_readiness() {
        let task_id = test_id("broadcast-published");
        let snapshot = BroadcastReadinessSnapshot::new(3, 1, 1, 1_200, 1);

        emit_broadcast_task_offer_published_event(
            &task_id,
            "reverse_string",
            "qa-payload",
            "message-id-test",
            1,
            &snapshot,
        );

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| entry.event == "broadcast_task_offer_published")
            .expect("published event should be present");

        assert_eq!(
            entry
                .fields
                .get("task_type")
                .and_then(|value| value.as_str()),
            Some("reverse_string")
        );
        assert_eq!(
            entry.fields.get("topic").and_then(|value| value.as_str()),
            Some(TASK_TOPIC)
        );
        assert_eq!(
            entry
                .fields
                .get("attempt_number")
                .and_then(|value| value.as_u64()),
            Some(1)
        );
    }

    #[test]
    fn controller_accepts_task_result_from_assigned_worker() {
        let mut state = BroadcastOfferState::new("broadcast-result-accept".to_string());
        state.mark_assigned("assigned-worker");

        assert!(evaluate_broadcast_result_acceptance(
            Some(&state),
            "broadcast-result-accept",
            "assigned-worker",
            true,
        )
        .is_ok());
    }

    #[test]
    fn controller_rejects_task_result_from_non_assigned_worker() {
        let mut state = BroadcastOfferState::new("broadcast-result-wrong".to_string());
        state.mark_assigned("assigned-worker");

        assert_eq!(
            evaluate_broadcast_result_acceptance(
                Some(&state),
                "broadcast-result-wrong",
                "other-worker",
                true,
            ),
            Err("wrong_worker")
        );
    }

    #[test]
    fn controller_rejects_duplicate_broadcast_result() {
        let mut state = BroadcastOfferState::new("broadcast-result-duplicate".to_string());
        state.mark_assigned("assigned-worker");
        assert!(evaluate_broadcast_result_acceptance(
            Some(&state),
            "broadcast-result-duplicate",
            "assigned-worker",
            true,
        )
        .is_ok());

        state.mark_result_accepted();
        assert_eq!(
            evaluate_broadcast_result_acceptance(
                Some(&state),
                "broadcast-result-duplicate",
                "assigned-worker",
                true,
            ),
            Err("duplicate_result")
        );
    }

    #[test]
    fn controller_cancels_recovery_after_result_accepted() {
        let task_id = test_id("broadcast-recovery-cancel");

        emit_broadcast_recovery_cancelled_event(&task_id, "assigned-worker");

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| {
                entry.trace_id == task_id && entry.event == "broadcast_recovery_cancelled"
            })
            .expect("broadcast_recovery_cancelled should be present");

        assert_eq!(
            entry.fields.get("reason").and_then(|value| value.as_str()),
            Some("result_accepted")
        );
    }

    #[test]
    fn broadcast_final_outcome_success_after_result_received() {
        let task_id = test_id("broadcast-final-success");

        emit_broadcast_final_outcome_success_event(
            &task_id,
            "assigned-worker",
            "tset-tluser-tsacdaorb-aq",
        );

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let entry = entries
            .iter()
            .rev()
            .find(|entry| entry.trace_id == task_id && entry.event == "final_outcome")
            .expect("final_outcome should be present");

        assert_eq!(
            entry.fields.get("outcome").and_then(|value| value.as_str()),
            Some("success")
        );
        assert_eq!(
            entry.fields.get("failed").and_then(|value| value.as_bool()),
            Some(false)
        );
    }

    #[test]
    fn broadcast_no_timeout_after_worker_success_result_accepted() {
        let mut state = BroadcastOfferState::new("broadcast-no-timeout".to_string());
        state.mark_assigned("assigned-worker");

        assert!(evaluate_broadcast_result_acceptance(
            Some(&state),
            "broadcast-no-timeout",
            "assigned-worker",
            true,
        )
        .is_ok());
        state.mark_result_accepted();

        assert!(state.result_accepted);
        assert_eq!(
            evaluate_broadcast_result_acceptance(
                Some(&state),
                "broadcast-no-timeout",
                "assigned-worker",
                true,
            ),
            Err("duplicate_result")
        );
    }

    #[test]
    fn worker_emits_topic_subscribed_for_required_topics() {
        let peer_id = test_id("worker-topic-test");
        for topic in BROADCAST_PUBSUB_TOPICS {
            emit_worker_topic_subscribed_event(topic, &peer_id, "mock");
        }
        emit_worker_pubsub_ready_event(&peer_id, "mock", &BROADCAST_PUBSUB_TOPICS);

        iamine_network::flush_structured_logs().unwrap();
        let entries =
            iamine_network::read_log_entries(&iamine_network::default_node_log_path()).unwrap();
        let topics: HashSet<String> = entries
            .iter()
            .filter(|entry| entry.event == "worker_topic_subscribed")
            .filter(|entry| {
                entry.fields.get("peer_id").and_then(|value| value.as_str())
                    == Some(peer_id.as_str())
            })
            .filter_map(|entry| {
                entry
                    .fields
                    .get("topic")
                    .and_then(|value| value.as_str())
                    .map(str::to_string)
            })
            .collect();

        for topic in BROADCAST_PUBSUB_TOPICS {
            assert!(topics.contains(topic), "missing worker topic {topic}");
        }
        assert!(entries.iter().rev().any(|entry| {
            entry.event == "worker_pubsub_ready"
                && entry.fields.get("peer_id").and_then(|value| value.as_str())
                    == Some(peer_id.as_str())
        }));
    }
}
