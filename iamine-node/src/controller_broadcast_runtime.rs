use crate::broadcast_protocol::{
    build_broadcast_task_assign_payload, build_broadcast_task_offer_payload,
};
use crate::broadcast_runtime::{
    broadcast_offer_ready, emit_broadcast_bid_received_event, emit_broadcast_pubsub_ready_event,
    emit_broadcast_readiness_state_event, emit_broadcast_readiness_waiting_event,
    emit_broadcast_task_assign_published_event, emit_broadcast_task_offer_prepared_event,
    emit_broadcast_task_offer_publish_attempt_event,
    emit_broadcast_task_offer_publish_failed_event, emit_broadcast_task_offer_published_event,
    emit_broadcast_task_offer_readiness_timeout_event, emit_broadcast_topic_subscriber_seen_event,
    BroadcastOfferState, BroadcastReadinessSnapshot,
};
use crate::peer_tracker::PeerTracker;
use crate::pubsub_topic_tracker::{
    gossipsub_all_peers_per_topic_value, gossipsub_mesh_peer_ids, gossipsub_topic_peer_count,
    sync_pubsub_tracker_from_gossipsub, PubsubTopicTracker,
};
use crate::router_scheduler::{SchedulerDecision, SelectionReason};
use crate::scheduler_events::emit_scheduler_decision_events;
use crate::task_events::{
    emit_task_lifecycle_assigned, emit_task_lifecycle_created, emit_task_lifecycle_failed,
    emit_task_lifecycle_retrying, emit_task_lifecycle_scheduler_decision,
};
use crate::task_lifecycle::{self, TaskLifecycleErrorCode};
use crate::task_scheduler::TaskScheduler;
use crate::{
    emit_observed_peer_subscription_event, log_observability_event, IamineBehaviour, ASSIGN_TOPIC,
    BROADCAST_OFFER_MAX_ATTEMPTS, BROADCAST_READINESS_TIMEOUT_MS, TASK_TOPIC,
};
use iamine_network::{LogLevel, TASK_DISPATCH_UNCONFIRMED_001};
use libp2p::{gossipsub, swarm::Swarm, PeerId};
use serde_json::{Map, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ControllerBroadcastRuntimeOutcome {
    Continue,
    BreakLoop,
}

impl ControllerBroadcastRuntimeOutcome {
    pub(crate) fn should_break(self) -> bool {
        matches!(self, Self::BreakLoop)
    }
}

pub(crate) struct ControllerBroadcastTickContext<'a> {
    pub(crate) task_type: &'a str,
    pub(crate) data: &'a str,
    pub(crate) controller_peer_id: &'a PeerId,
    pub(crate) scheduler: &'a Arc<TaskScheduler>,
    pub(crate) known_workers: &'a HashSet<PeerId>,
    pub(crate) pubsub_topics: &'a mut PubsubTopicTracker,
    pub(crate) waiting_for_response: &'a mut bool,
    pub(crate) tasks_sent: &'a mut bool,
}

pub(crate) struct ControllerTaskBidContext<'a> {
    pub(crate) task_type: &'a str,
    pub(crate) data: &'a str,
    pub(crate) controller_peer_id: &'a PeerId,
    pub(crate) scheduler: &'a Arc<TaskScheduler>,
    pub(crate) broadcast_offer_state: &'a mut Option<BroadcastOfferState>,
    pub(crate) known_workers: &'a HashSet<PeerId>,
    pub(crate) origin_peer_map: &'a mut HashMap<String, PeerId>,
    pub(crate) peer_tracker: &'a PeerTracker,
}

pub(crate) async fn handle_controller_broadcast_tick(
    swarm: &mut Swarm<IamineBehaviour>,
    state: &mut BroadcastOfferState,
    context: ControllerBroadcastTickContext<'_>,
) -> Result<ControllerBroadcastRuntimeOutcome, String> {
    if !state.prepared {
        state.prepared = true;
        emit_broadcast_task_offer_prepared_event(
            &state.task_id,
            context.task_type,
            context.data,
            &context.controller_peer_id.to_string(),
        );
        emit_task_lifecycle_created(
            &state.task_id,
            context.task_type,
            context.data,
            &context.controller_peer_id.to_string(),
        );
    }

    if state.published && !state.assignment_published {
        if let Some(winner) = context.scheduler.try_assign(&state.task_id).await {
            let task_id = state.task_id.clone();
            publish_controller_task_assign(
                swarm,
                ControllerTaskAssignPublishContext {
                    task_id: &task_id,
                    task_type: context.task_type,
                    data: context.data,
                    winner: &winner,
                    controller_peer_id: context.controller_peer_id,
                    known_workers: context.known_workers,
                    selection_reason: SelectionReason::CurrentBroadcastPolicy,
                    mark_state: Some(state),
                },
            );
            println!("🏆 Asignando worker={} task_id={}", winner, task_id);
        }
        return Ok(ControllerBroadcastRuntimeOutcome::Continue);
    }

    if state.published || state.failed {
        return Ok(ControllerBroadcastRuntimeOutcome::Continue);
    }

    let connected_peers = swarm.connected_peers().count();
    let newly_observed_subscriptions =
        sync_pubsub_tracker_from_gossipsub(context.pubsub_topics, &swarm.behaviour().gossipsub);
    let mesh_peer_ids = gossipsub_mesh_peer_ids(&swarm.behaviour().gossipsub, TASK_TOPIC);
    let mesh_peers = mesh_peer_ids.len();
    let subscribed_peers =
        context
            .pubsub_topics
            .topic_peer_count(TASK_TOPIC)
            .max(gossipsub_topic_peer_count(
                &swarm.behaviour().gossipsub,
                TASK_TOPIC,
            ));
    for (observed_peer_id, topic_name) in newly_observed_subscriptions {
        emit_observed_peer_subscription_event(
            "controller_observed_peer_subscription",
            &context.controller_peer_id.to_string(),
            &observed_peer_id,
            topic_name,
            "broadcast",
            "gossipsub_all_peers",
        );
        if topic_name == TASK_TOPIC {
            state.mark_subscription_seen();
            emit_broadcast_topic_subscriber_seen_event(
                TASK_TOPIC,
                &observed_peer_id,
                connected_peers,
                subscribed_peers,
                mesh_peers,
                "gossipsub_all_peers",
            );
        }
    }
    let all_peers_per_topic = gossipsub_all_peers_per_topic_value(&swarm.behaviour().gossipsub);
    let elapsed_ms = state.elapsed_ms();
    let readiness_snapshot = BroadcastReadinessSnapshot::new(
        connected_peers,
        subscribed_peers,
        mesh_peers,
        elapsed_ms,
        state.attempts,
    )
    .with_gossipsub_details(
        all_peers_per_topic,
        mesh_peer_ids.clone(),
        state.last_publish_failure_reason.as_deref(),
    );
    if state.should_log_readiness_wait() {
        emit_broadcast_readiness_state_event(&state.task_id, &readiness_snapshot);
        if !readiness_snapshot.ready {
            emit_broadcast_readiness_waiting_event(&state.task_id, &readiness_snapshot);
        }
    }

    if elapsed_ms >= BROADCAST_READINESS_TIMEOUT_MS
        || state.attempts >= BROADCAST_OFFER_MAX_ATTEMPTS
    {
        state.failed = true;
        let reason = if elapsed_ms >= BROADCAST_READINESS_TIMEOUT_MS {
            "pubsub_readiness_timeout"
        } else {
            "publish_attempts_exhausted"
        };
        emit_broadcast_task_offer_publish_failed_event(
            &state.task_id,
            context.task_type,
            reason,
            state.attempts.saturating_add(1),
            false,
            &readiness_snapshot,
        );
        emit_broadcast_task_offer_readiness_timeout_event(
            &state.task_id,
            context.task_type,
            &readiness_snapshot,
            state.last_publish_failure_reason.as_deref(),
        );
        emit_task_lifecycle_failed(
            &state.task_id,
            context.task_type,
            TaskLifecycleErrorCode::TaskTimeout,
            reason,
        );
        eprintln!(
            "❌ [Broadcast] PubSub no quedó listo: task_id={} peers={} subscribed_peers={} mesh_peers={} attempts={} reason={}",
            state.task_id, connected_peers, subscribed_peers, mesh_peers, state.attempts, reason
        );
        return Ok(ControllerBroadcastRuntimeOutcome::BreakLoop);
    }

    if !broadcast_offer_ready(subscribed_peers, mesh_peers, connected_peers, elapsed_ms) {
        return Ok(ControllerBroadcastRuntimeOutcome::Continue);
    }

    if !state.can_attempt_publish() {
        return Ok(ControllerBroadcastRuntimeOutcome::Continue);
    }

    if !state.scheduler_registered {
        context
            .scheduler
            .register_task(state.task_id.clone(), context.task_type.to_string())
            .await;
        state.scheduler_registered = true;
    }

    let offer = controller_task_offer_payload(
        &state.task_id,
        context.task_type,
        context.data,
        &context.controller_peer_id.to_string(),
    );
    let payload = serde_json::to_vec(&offer)
        .map_err(|error| format!("Broadcast TaskOffer payload error: {}", error))?;
    let attempt_number = state.attempts.saturating_add(1);
    emit_broadcast_task_offer_publish_attempt_event(
        &state.task_id,
        context.task_type,
        context.data,
        payload.len(),
        attempt_number,
        &readiness_snapshot,
    );

    match swarm
        .behaviour_mut()
        .gossipsub
        .publish(gossipsub::IdentTopic::new(TASK_TOPIC), payload)
    {
        Ok(message_id) => {
            state.record_publish_result(true, None);
            let published_snapshot = BroadcastReadinessSnapshot::new(
                connected_peers,
                subscribed_peers,
                mesh_peers,
                state.elapsed_ms(),
                state.attempts,
            )
            .with_gossipsub_details(
                gossipsub_all_peers_per_topic_value(&swarm.behaviour().gossipsub),
                mesh_peer_ids,
                state.last_publish_failure_reason.as_deref(),
            );
            let message_id = message_id.to_string();
            emit_broadcast_task_offer_published_event(
                &state.task_id,
                context.task_type,
                context.data,
                &message_id,
                attempt_number,
                &published_snapshot,
            );
            *context.waiting_for_response = true;
            *context.tasks_sent = true;
            println!(
                "📣 [Broadcast] TaskOffer publicado: task_id={} type={} data='{}'",
                state.task_id, context.task_type, context.data
            );
        }
        Err(error) => {
            let error_text = error.to_string();
            state.record_publish_result(false, Some(&error_text));
            let failure_snapshot = BroadcastReadinessSnapshot::new(
                connected_peers,
                subscribed_peers,
                mesh_peers,
                state.elapsed_ms(),
                state.attempts,
            )
            .with_gossipsub_details(
                gossipsub_all_peers_per_topic_value(&swarm.behaviour().gossipsub),
                mesh_peer_ids,
                state.last_publish_failure_reason.as_deref(),
            );
            let still_recoverable = state.elapsed_ms() < BROADCAST_READINESS_TIMEOUT_MS
                && state.attempts < BROADCAST_OFFER_MAX_ATTEMPTS;
            emit_broadcast_task_offer_publish_failed_event(
                &state.task_id,
                context.task_type,
                &error_text,
                attempt_number,
                still_recoverable,
                &failure_snapshot,
            );
            if !still_recoverable {
                state.failed = true;
                emit_broadcast_task_offer_readiness_timeout_event(
                    &state.task_id,
                    context.task_type,
                    &failure_snapshot,
                    state.last_publish_failure_reason.as_deref(),
                );
                emit_task_lifecycle_failed(
                    &state.task_id,
                    context.task_type,
                    TaskLifecycleErrorCode::TaskTimeout,
                    &error_text,
                );
                eprintln!(
                    "❌ [Broadcast] No se pudo publicar TaskOffer tras {} intentos: {}",
                    state.attempts, error_text
                );
                return Ok(ControllerBroadcastRuntimeOutcome::BreakLoop);
            }
        }
    }

    Ok(ControllerBroadcastRuntimeOutcome::Continue)
}

pub(crate) async fn handle_controller_task_bid(
    swarm: &mut Swarm<IamineBehaviour>,
    msg: &Value,
    context: ControllerTaskBidContext<'_>,
) {
    let task_id = msg["task_id"].as_str().unwrap_or("").to_string();
    let worker_id = msg["worker_id"].as_str().unwrap_or("").to_string();
    let rep = msg["reputation_score"].as_u64().unwrap_or(0) as u32;
    let slots = msg["available_slots"].as_u64().unwrap_or(0) as usize;
    let est_ms = msg["estimated_ms"].as_u64().unwrap_or(1000);
    let latency = context.peer_tracker.get_latency(&worker_id);

    emit_broadcast_bid_received_event(&task_id, &worker_id, rep, slots, latency);
    println!(
        "📨 [Scheduler] Bid: worker={} task_id={} rep={} slots={} latency={:.1}ms",
        worker_id, task_id, rep, slots, latency
    );

    if let Some(winner) = context
        .scheduler
        .receive_bid(&task_id, worker_id.clone(), rep, slots, est_ms)
        .await
    {
        if context
            .broadcast_offer_state
            .as_ref()
            .map(|state| state.task_id == task_id && state.assignment_published)
            .unwrap_or(false)
        {
            return;
        }
        println!("🏆 Asignando worker={} task_id={}", winner, task_id);

        if let Some(winner_peer) = context
            .known_workers
            .iter()
            .find(|p| p.to_string().starts_with(&winner[..8.min(winner.len())]))
            .copied()
        {
            context.origin_peer_map.insert(task_id.clone(), winner_peer);
        }

        publish_controller_task_assign(
            swarm,
            ControllerTaskAssignPublishContext {
                task_id: &task_id,
                task_type: context.task_type,
                data: context.data,
                winner: &winner,
                controller_peer_id: context.controller_peer_id,
                known_workers: context.known_workers,
                selection_reason: SelectionReason::BroadcastBidSelected,
                mark_state: context.broadcast_offer_state.as_mut().and_then(|state| {
                    if state.task_id == task_id {
                        Some(state)
                    } else {
                        None
                    }
                }),
            },
        );

        schedule_controller_rebroadcast_timeout(
            &task_id,
            context.task_type,
            context.data,
            context.controller_peer_id,
            Arc::clone(context.scheduler),
        );
    }
}

fn schedule_controller_rebroadcast_timeout(
    task_id: &str,
    task_type: &str,
    data: &str,
    controller_peer_id: &PeerId,
    scheduler: Arc<TaskScheduler>,
) {
    let rebroadcast_task_id = task_id.to_string();
    let task_type_clone = task_type.to_string();
    let data_clone = data.to_string();
    let origin_str = controller_peer_id.to_string();
    let (rebroadcast_tx, mut rebroadcast_rx) = tokio::sync::mpsc::channel::<Value>(1);

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30_000)).await;
        let stats = scheduler.stats().await;
        if stats.1 > 0 {
            println!(
                "⏱️ [Recovery] Tarea {} expiró — rebroadcasting...",
                rebroadcast_task_id
            );
            emit_task_lifecycle_retrying(
                &rebroadcast_task_id,
                &task_type_clone,
                1,
                Some("broadcast_assignment_timeout".to_string()),
            );
            let offer = serde_json::json!({
                "type": "TaskOffer",
                "task_id": format!("{}r", rebroadcast_task_id),
                "task_type": task_type_clone,
                "data": data_clone,
                "requester_id": origin_str,
                "origin_peer": origin_str,
                "is_retry": true,
            });
            let _ = rebroadcast_tx.send(offer).await;
        }
    });

    if let Ok(_offer) = rebroadcast_rx.try_recv() {
        // Preserve the existing non-blocking recovery wiring. The active swarm publish
        // remains owned by the controller event loop.
    }
}

struct ControllerTaskAssignPublishContext<'a> {
    task_id: &'a str,
    task_type: &'a str,
    data: &'a str,
    winner: &'a str,
    controller_peer_id: &'a PeerId,
    known_workers: &'a HashSet<PeerId>,
    selection_reason: SelectionReason,
    mark_state: Option<&'a mut BroadcastOfferState>,
}

fn publish_controller_task_assign(
    swarm: &mut Swarm<IamineBehaviour>,
    context: ControllerTaskAssignPublishContext<'_>,
) {
    let deadline_ms = 30_000u64;
    let assign = controller_task_assign_payload(
        context.task_id,
        context.winner,
        &context.controller_peer_id.to_string(),
        deadline_ms,
        context.task_type,
        context.data,
    );
    match swarm.behaviour_mut().gossipsub.publish(
        gossipsub::IdentTopic::new(ASSIGN_TOPIC),
        serde_json::to_vec(&assign).unwrap_or_default(),
    ) {
        Ok(message_id) => {
            if let Some(state) = context.mark_state {
                state.mark_assigned(context.winner);
            }
            emit_broadcast_task_assign_published_event(
                context.task_id,
                context.winner,
                &message_id.to_string(),
                deadline_ms,
            );
            let scheduler_decision = SchedulerDecision::from_current_broadcast_policy(
                context.task_id,
                context.task_type,
                context.winner,
                known_worker_ids(context.known_workers),
                context.selection_reason,
                task_lifecycle::now_ms(),
            );
            emit_scheduler_decision_events(&scheduler_decision);
            emit_task_lifecycle_assigned(
                context.task_id,
                context.task_type,
                context.winner,
                scheduler_decision.candidate_worker_ids(),
                scheduler_decision
                    .selection_reason
                    .to_task_selection_reason(),
            );
            emit_task_lifecycle_scheduler_decision(&scheduler_decision);
        }
        Err(error) => log_observability_event(
            LogLevel::Error,
            "broadcast_task_assign_publish_failed",
            context.task_id,
            Some(context.task_id),
            None,
            Some(TASK_DISPATCH_UNCONFIRMED_001),
            {
                let mut fields = Map::new();
                fields.insert("assigned_worker".to_string(), context.winner.into());
                fields.insert("topic".to_string(), ASSIGN_TOPIC.into());
                fields.insert("error".to_string(), error.to_string().into());
                fields
            },
        ),
    }
}

pub(crate) fn emit_controller_pubsub_ready(peer_id: &PeerId, topics: &[&str]) {
    emit_broadcast_pubsub_ready_event(&peer_id.to_string(), topics);
}

fn known_worker_ids(known_workers: &HashSet<PeerId>) -> Vec<String> {
    known_workers
        .iter()
        .map(|worker| worker.to_string())
        .collect()
}

pub(crate) fn controller_task_offer_payload(
    task_id: &str,
    task_type: &str,
    data: &str,
    controller_peer_id: &str,
) -> Value {
    build_broadcast_task_offer_payload(task_id, task_type, data, controller_peer_id)
}

pub(crate) fn controller_task_assign_payload(
    task_id: &str,
    winner: &str,
    controller_peer_id: &str,
    deadline_ms: u64,
    task_type: &str,
    data: &str,
) -> Value {
    build_broadcast_task_assign_payload(
        task_id,
        winner,
        controller_peer_id,
        deadline_ms,
        task_type,
        data,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn controller_broadcast_prepares_task_offer_preserving_fields() {
        let offer =
            controller_task_offer_payload("task-1", "reverse_string", "abc", "controller-a");

        assert_eq!(offer["type"], "TaskOffer");
        assert_eq!(offer["task_id"], "task-1");
        assert_eq!(offer["task_type"], "reverse_string");
        assert_eq!(offer["data"], "abc");
        assert_eq!(offer["origin_peer"], "controller-a");
    }

    #[test]
    fn controller_broadcast_assign_preserves_selected_worker() {
        let assign = controller_task_assign_payload(
            "task-1",
            "worker-a",
            "controller-a",
            30_000,
            "reverse_string",
            "abc",
        );

        assert_eq!(assign["type"], "TaskAssign");
        assert_eq!(assign["task_id"], "task-1");
        assert_eq!(assign["assigned_worker"], "worker-a");
        assert_eq!(assign["origin_peer"], "controller-a");
        assert_eq!(assign["deadline_ms"], 30_000);
    }
}
