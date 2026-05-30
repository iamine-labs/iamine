use super::state::BroadcastReadinessSnapshot;
use crate::broadcast_protocol::broadcast_payload_preview;
use crate::{log_observability_event, ASSIGN_TOPIC, BROADCAST_READINESS_TIMEOUT_MS, TASK_TOPIC};
use iamine_network::{LogLevel, TASK_DISPATCH_UNCONFIRMED_001};
use serde_json::{Map, Value};

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
