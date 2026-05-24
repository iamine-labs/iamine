use crate::cluster_events::{emit_cluster_capabilities_updated, emit_cluster_node_updated};
use crate::cluster_registry::{unix_now_ms, ClusterRegistry};
use crate::infer_observability::{
    emit_attempt_progress_received_event, emit_attempt_progress_recovered_event,
    emit_attempt_state_changed_event, emit_attempt_timeout_extended_event,
    emit_fallback_attempt_claimed_event, emit_remote_progress_client_received_event,
    emit_watchdog_reset_on_progress_event, format_remote_progress_line,
};
use crate::infer_retry::DistributedInferState;
use crate::infer_watchdog::{
    claim_source_from_progress_stage, lifecycle_state_from_progress_stage, ActiveAttempt,
    AttemptKey, AttemptLifecycleState, AttemptWatchdog,
};
use crate::log_observability_event;
use crate::network_event_observability::HumanLogThrottle;
use crate::node_modes::NodeMode;
use crate::peer_tracker::PeerTracker;
use crate::pubsub_topics::{
    tracked_pubsub_topic_name, CAP_TOPIC, DIRECT_INF_TOPIC, RESULTS_TOPIC, TASK_TOPIC,
};
use crate::worker_runtime::WorkerInferenceRuntimeRequest;
use crate::NodeMetrics;
use iamine_network::{
    claim_task_attempt_peer, LogLevel, NodeCapabilityHeartbeat, SharedNodeRegistry,
};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub(crate) fn message_topic_name(topic: &libp2p::gossipsub::TopicHash) -> &'static str {
    tracked_pubsub_topic_name(topic).unwrap_or("unknown")
}

pub(crate) fn broadcast_mode_task_payload(mode: &NodeMode) -> (&str, &str) {
    if let NodeMode::Broadcast { task_type, data } = mode {
        (task_type.as_str(), data.as_str())
    } else {
        ("", "")
    }
}

pub(crate) fn direct_inference_request_for_local_worker(
    msg: &Value,
    local_peer_id: &str,
    from_peer: &str,
) -> Option<WorkerInferenceRuntimeRequest> {
    WorkerInferenceRuntimeRequest::from_direct_inference_request_value(
        msg,
        local_peer_id,
        from_peer,
    )
}

pub(crate) async fn handle_capability_topic_message(
    data: &[u8],
    registry: &SharedNodeRegistry,
    cluster_registry: &mut ClusterRegistry,
    cluster_id: &str,
) {
    if let Ok(hb) = serde_json::from_slice::<NodeCapabilityHeartbeat>(data) {
        let _ = registry.write().await.update_from_heartbeat(hb.clone());
        cluster_registry.update_from_capability_heartbeat(&hb, unix_now_ms());
        emit_cluster_capabilities_updated(
            cluster_registry,
            cluster_id,
            &hb.peer_id,
            CAP_TOPIC,
            hb.models.len(),
            Some(hb.latency_ms),
        );
    }
}

pub(crate) async fn handle_heartbeat_message(
    msg: &Value,
    peer_tracker: &mut PeerTracker,
    metrics: Arc<RwLock<NodeMetrics>>,
    cluster_registry: &mut ClusterRegistry,
    cluster_id: &str,
    human_log_throttle: &mut HumanLogThrottle,
) {
    cluster_registry.update_from_worker_heartbeat_value(msg, unix_now_ms());
    let worker_id = msg["peer_id"].as_str().unwrap_or("");
    if !worker_id.is_empty() {
        emit_cluster_node_updated(cluster_id, worker_id, "heartbeat");
    }
    let slots = msg["available_slots"].as_u64().unwrap_or(0) as usize;
    let rep = msg["reputation_score"].as_u64().unwrap_or(0) as u32;
    let uptime = msg["uptime_secs"].as_u64().unwrap_or(0);
    peer_tracker.update_heartbeat(worker_id, slots, rep);
    let peer_count = peer_tracker.peer_count();
    let heartbeat_value = format!("slots={} rep={} peers={}", slots, rep, peer_count);
    if human_log_throttle.should_log(
        &format!("heartbeat:{}", worker_id),
        15_000,
        Some(&heartbeat_value),
    ) {
        println!(
            "💓 Heartbeat {}... slots={} rep={} uptime={}s peers={}",
            &worker_id[..8.min(worker_id.len())],
            slots,
            rep,
            uptime,
            peer_count
        );
    }
    let mut m = metrics.write().await;
    m.network_peers = peer_count;
    m.mesh_peers = peer_count;
}

pub(crate) async fn handle_node_capabilities_message(
    msg: &Value,
    registry: &SharedNodeRegistry,
    cluster_registry: &mut ClusterRegistry,
    cluster_id: &str,
) {
    if let Ok(hb) = serde_json::from_value::<NodeCapabilityHeartbeat>(msg.clone()) {
        let _ = registry.write().await.update_from_heartbeat(hb.clone());
        cluster_registry.update_from_capability_heartbeat(&hb, unix_now_ms());
        emit_cluster_capabilities_updated(
            cluster_registry,
            cluster_id,
            &hb.peer_id,
            "node_capabilities",
            hb.models.len(),
            None,
        );
    }
}

pub(crate) fn handle_task_cancel_message(msg: &Value) {
    let task_id = msg["task_id"].as_str().unwrap_or("");
    let reason = msg["reason"].as_str().unwrap_or("sin razón");
    println!("🚫 [Worker] Tarea {} cancelada: {}", task_id, reason);
}

pub(crate) fn log_invalid_worker_task_message(
    message_topic: &str,
    from_peer: &str,
    payload_size: usize,
) {
    log_observability_event(
        LogLevel::Warn,
        "task_message_received",
        "dispatch",
        None,
        None,
        Some(iamine_network::TASK_DISPATCH_UNCONFIRMED_001),
        {
            let mut fields = Map::new();
            fields.insert("topic".to_string(), message_topic.into());
            fields.insert("from_peer".to_string(), from_peer.to_string().into());
            fields.insert("payload_parse_result".to_string(), "invalid_json".into());
            fields.insert("payload_size".to_string(), (payload_size as u64).into());
            fields
        },
    );
}

pub(crate) fn handle_inference_token_message(
    msg: &Value,
    infer_request_id: Option<&String>,
    token_buffer: &mut HashMap<u32, String>,
    next_token_idx: &mut u32,
    rendered_output: &mut String,
) {
    let rid = msg["request_id"].as_str().unwrap_or("");
    if infer_request_id.map(|value| value.as_str()) != Some(rid) {
        return;
    }
    let idx = msg["index"].as_u64().unwrap_or(0) as u32;
    let token = msg["token"].as_str().unwrap_or("").to_string();

    token_buffer.insert(idx, token);

    while let Some(next) = token_buffer.remove(next_token_idx) {
        print!("{}", next);
        rendered_output.push_str(&next);
        let _ = std::io::Write::flush(&mut std::io::stdout());
        *next_token_idx += 1;
    }
}

pub(crate) fn handle_inference_progress_message(
    msg: &Value,
    message_topic: &str,
    distributed_infer_state: &mut Option<DistributedInferState>,
    attempt_watchdogs: &mut HashMap<String, AttemptWatchdog>,
    active_attempts: &mut HashMap<AttemptKey, ActiveAttempt>,
) {
    if !matches!(message_topic, RESULTS_TOPIC | TASK_TOPIC | DIRECT_INF_TOPIC) {
        return;
    }
    let task_id = msg["task_id"].as_str().unwrap_or("").to_string();
    let attempt_id = msg["attempt_id"].as_str().unwrap_or("").to_string();
    let attempt_key = AttemptKey::new(task_id.clone(), attempt_id.clone());
    let stage = msg["stage"].as_str().unwrap_or("unknown");
    let worker_peer = msg["worker_peer_id"]
        .as_str()
        .or_else(|| msg["worker_peer"].as_str())
        .unwrap_or("unknown");
    let model_id = msg["model_id"].as_str().unwrap_or("").to_string();
    let elapsed_ms = msg["elapsed_ms"].as_u64().unwrap_or_else(|| {
        attempt_watchdogs
            .get(&attempt_id)
            .map(|watchdog| watchdog.elapsed_ms())
            .unwrap_or_default()
    });
    let tokens_generated_count = msg["tokens_generated_count"].as_u64();
    let expected_task_id = distributed_infer_state
        .as_ref()
        .map(|state| state.trace_task_id.as_str());
    if expected_task_id != Some(task_id.as_str()) {
        return;
    }

    let model_for_received_event = if model_id.is_empty() {
        active_attempts
            .get(&attempt_key)
            .map(|attempt| attempt.model_id.as_str())
    } else {
        Some(model_id.as_str())
    };
    emit_remote_progress_client_received_event(
        &task_id,
        &attempt_id,
        model_for_received_event,
        worker_peer,
        stage,
        message_topic,
        elapsed_ms,
        tokens_generated_count,
    );

    if let Some(active_attempt) = active_attempts.get(&attempt_key) {
        if !active_attempt.accepts_worker(worker_peer) {
            return;
        }
    }

    let Some(watchdog) = attempt_watchdogs.get_mut(&attempt_id) else {
        return;
    };
    if !watchdog.accepts_worker(worker_peer) {
        return;
    }

    let model_for_event = if model_id.is_empty() {
        watchdog.model_id.clone()
    } else {
        model_id.clone()
    };
    let claim_source = claim_source_from_progress_stage(stage);
    if let Some(previous_worker_peer_id) = watchdog.claim_worker(worker_peer, claim_source) {
        if let Some(active_attempt) = active_attempts.get_mut(&attempt_key) {
            let _ = active_attempt.claim_worker(worker_peer);
        }
        emit_fallback_attempt_claimed_event(
            &task_id,
            &attempt_id,
            Some(&model_for_event),
            worker_peer,
            &previous_worker_peer_id,
            claim_source,
        );
        if let Some(infer_state) = distributed_infer_state.as_mut() {
            let _ = infer_state.claim_attempt_record(&attempt_id, worker_peer);
        }
        let _ = claim_task_attempt_peer(&task_id, worker_peer);
    }

    let previous_state = watchdog.state;
    let should_emit_recovered = watchdog.no_progress_warning_emitted
        || matches!(
            previous_state,
            AttemptLifecycleState::Stalled | AttemptLifecycleState::TimedOut
        );
    let deadline_before = watchdog
        .deadline_at
        .duration_since(watchdog.started_at)
        .as_millis() as u64;
    let meaningful = watchdog.record_progress(stage, tokens_generated_count);
    if !meaningful {
        return;
    }

    watchdog.no_progress_warning_emitted = false;
    if let Some(infer_state) = distributed_infer_state.as_mut() {
        let mapped_state = lifecycle_state_from_progress_stage(stage)
            .map(|state| state.as_str())
            .unwrap_or(stage);
        infer_state.update_attempt_state(&attempt_id, mapped_state, Some("in_progress"), None);
    }
    let worker_peer_id = watchdog.worker_peer_id.clone();
    if watchdog.state != previous_state {
        emit_attempt_state_changed_event(
            &task_id,
            &attempt_id,
            Some(&model_for_event),
            Some(&worker_peer_id),
            previous_state.as_str(),
            watchdog.state.as_str(),
        );
    }
    let deadline_after = watchdog
        .deadline_at
        .duration_since(watchdog.started_at)
        .as_millis() as u64;
    if deadline_after > deadline_before {
        emit_attempt_timeout_extended_event(
            &task_id,
            &attempt_id,
            Some(&model_for_event),
            Some(&worker_peer_id),
            deadline_after,
            "real_progress",
        );
    }
    emit_watchdog_reset_on_progress_event(
        &task_id,
        &attempt_id,
        Some(&model_for_event),
        &worker_peer_id,
        stage,
        deadline_after,
    );
    if should_emit_recovered {
        emit_attempt_progress_recovered_event(
            &task_id,
            &attempt_id,
            Some(&model_for_event),
            &worker_peer_id,
        );
    }
    println!(
        "{}",
        format_remote_progress_line(
            &worker_peer_id,
            &attempt_id,
            stage,
            elapsed_ms,
            tokens_generated_count,
        )
    );
    emit_attempt_progress_received_event(
        &task_id,
        &attempt_id,
        Some(&model_for_event),
        &worker_peer_id,
        stage,
        elapsed_ms,
        tokens_generated_count,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pubsub_topics::{BIDS_TOPIC, TASK_TOPIC};

    #[test]
    fn gossipsub_unknown_topic_is_controlled() {
        let topic = libp2p::gossipsub::IdentTopic::new("iamine-unknown").hash();
        assert_eq!(message_topic_name(&topic), "unknown");
    }

    #[test]
    fn gossipsub_routes_task_offer_to_worker_handler() {
        let topic = libp2p::gossipsub::IdentTopic::new(TASK_TOPIC).hash();
        assert_eq!(message_topic_name(&topic), TASK_TOPIC);
    }

    #[test]
    fn gossipsub_routes_task_bid_to_controller_handler() {
        let topic = libp2p::gossipsub::IdentTopic::new(BIDS_TOPIC).hash();
        assert_eq!(message_topic_name(&topic), BIDS_TOPIC);
    }
}
