use crate::result_acceptance::{
    decide_broadcast_result_acceptance, BroadcastResultAcceptanceContext,
};
use crate::{
    BROADCAST_OFFER_MAX_ATTEMPTS, BROADCAST_OFFER_MAX_RETRY_DELAY_MS,
    BROADCAST_OFFER_RETRY_DELAY_MS,
};
use serde_json::Value;

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

pub(crate) fn evaluate_broadcast_result_acceptance(
    state: Option<&BroadcastOfferState>,
    task_id: &str,
    worker_peer_id: &str,
    success: bool,
) -> Result<(), &'static str> {
    decide_broadcast_result_acceptance(BroadcastResultAcceptanceContext {
        expected_task_id: state.map(|state| state.task_id.as_str()),
        assigned_worker_peer_id: state.and_then(|state| state.assigned_worker.as_deref()),
        result_already_accepted: state.map(|state| state.result_accepted).unwrap_or(false),
        result_task_id: task_id,
        result_worker_peer_id: worker_peer_id,
        success,
    })
    .into_result()
}
