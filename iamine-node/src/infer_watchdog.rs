use crate::runtime_config::{
    INFER_FALLBACK_AFTER_MS, LATE_RESULT_ACCEPTANCE_WINDOW_MS, MAX_ADAPTIVE_TIMEOUT_MS,
    MAX_REMOTE_EXECUTION_TIMEOUT_MS, MIN_ADAPTIVE_TIMEOUT_MS, MIN_REMOTE_EXECUTION_TIMEOUT_MS,
    UNCLAIMED_WORKER_PEER_ID, WATCHDOG_EXTENSION_STEP_MS, WATCHDOG_MIN_STALL_MS,
    WATCHDOG_STALL_FACTOR_DEN, WATCHDOG_STALL_FACTOR_NUM,
};
use iamine_network::NodeCapability;
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AttemptLifecycleState {
    Queued,
    Starting,
    LoadingModel,
    InferenceWarmup,
    Running,
    ProducingTokens,
    Stalled,
    TimedOut,
    Completed,
    LateCompleted,
    Failed,
}

impl AttemptLifecycleState {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Starting => "starting",
            Self::LoadingModel => "loading_model",
            Self::InferenceWarmup => "inference_warmup",
            Self::Running => "running",
            Self::ProducingTokens => "producing_tokens",
            Self::Stalled => "stalled",
            Self::TimedOut => "timed_out",
            Self::Completed => "completed",
            Self::LateCompleted => "late_completed",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AttemptDispatchType {
    Direct,
    FallbackBroadcast,
}

impl AttemptDispatchType {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::FallbackBroadcast => "fallback_broadcast",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AttemptClaimSource {
    TaskMessageReceived,
    AttemptProgress,
    ResultReceived,
}

impl AttemptClaimSource {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::TaskMessageReceived => "task_message_received",
            Self::AttemptProgress => "attempt_progress",
            Self::ResultReceived => "result_received",
        }
    }
}

pub(crate) fn is_unclaimed_worker_peer_id(peer_id: &str) -> bool {
    peer_id.trim().is_empty() || matches!(peer_id, UNCLAIMED_WORKER_PEER_ID | "unclaimed")
}

pub(crate) fn is_valid_claiming_worker(peer_id: &str) -> bool {
    !is_unclaimed_worker_peer_id(peer_id) && peer_id != "unknown"
}

pub(crate) fn is_meaningfully_in_flight(state: AttemptLifecycleState) -> bool {
    matches!(
        state,
        AttemptLifecycleState::Queued
            | AttemptLifecycleState::Starting
            | AttemptLifecycleState::LoadingModel
            | AttemptLifecycleState::InferenceWarmup
            | AttemptLifecycleState::Running
            | AttemptLifecycleState::ProducingTokens
    )
}

pub(crate) fn lifecycle_state_from_progress_stage(stage: &str) -> Option<AttemptLifecycleState> {
    match stage {
        "task_received" | "task_message_received" | "worker_claimed" => {
            Some(AttemptLifecycleState::Starting)
        }
        "attempt_started" => Some(AttemptLifecycleState::Starting),
        "model_loading" | "model_load_started" => Some(AttemptLifecycleState::LoadingModel),
        "model_loaded" | "model_load_completed" => Some(AttemptLifecycleState::InferenceWarmup),
        "inference_warmup" => Some(AttemptLifecycleState::InferenceWarmup),
        "inference_started" => Some(AttemptLifecycleState::Running),
        "first_token_generated" | "tokens_generated_count" => {
            Some(AttemptLifecycleState::ProducingTokens)
        }
        "inference_finished" | "result_serialized" | "result_published" => {
            Some(AttemptLifecycleState::Running)
        }
        _ => None,
    }
}

pub(crate) fn claim_source_from_progress_stage(stage: &str) -> AttemptClaimSource {
    match stage {
        "task_received" | "task_message_received" | "worker_claimed" => {
            AttemptClaimSource::TaskMessageReceived
        }
        _ => AttemptClaimSource::AttemptProgress,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AttemptTimeoutPolicy {
    pub(crate) timeout_ms: u64,
    pub(crate) claim_timeout_ms: u64,
    pub(crate) first_progress_timeout_ms: u64,
    pub(crate) model_load_timeout_ms: u64,
    pub(crate) first_token_timeout_ms: u64,
    pub(crate) stall_timeout_ms: u64,
    pub(crate) extension_step_ms: u64,
    pub(crate) max_wait_ms: u64,
    pub(crate) max_execution_timeout_ms: u64,
    pub(crate) latency_class: &'static str,
}

impl AttemptTimeoutPolicy {
    pub(crate) fn from_model_and_node(model_id: &str, worker: Option<&NodeCapability>) -> Self {
        let lower_model = model_id.to_ascii_lowercase();
        let mut timeout_ms: u64 = if lower_model.contains("mistral-7b") {
            16_000
        } else if lower_model.contains("llama3-3b") {
            11_000
        } else if lower_model.contains("tinyllama") {
            7_500
        } else {
            9_000
        };

        let mut latency_class = if timeout_ms >= 14_000 {
            "high"
        } else if timeout_ms >= 9_500 {
            "medium"
        } else {
            "low"
        };

        if let Some(worker) = worker {
            if worker.gpu_available || worker.accelerator.to_ascii_lowercase().contains("gpu") {
                timeout_ms = timeout_ms.saturating_sub(2_500);
                latency_class = "low";
            } else if worker.cpu_score <= 45_000 {
                timeout_ms = timeout_ms.saturating_add(6_000);
                latency_class = "high";
            } else if worker.cpu_score <= 90_000 {
                timeout_ms = timeout_ms.saturating_add(3_500);
                if latency_class == "low" {
                    latency_class = "medium";
                }
            }
            timeout_ms = timeout_ms.saturating_add((worker.latency_ms as u64).min(2_000));
        }

        timeout_ms = timeout_ms.clamp(MIN_ADAPTIVE_TIMEOUT_MS, MAX_ADAPTIVE_TIMEOUT_MS);
        let stall_timeout_ms = ((timeout_ms.saturating_mul(WATCHDOG_STALL_FACTOR_NUM))
            / WATCHDOG_STALL_FACTOR_DEN)
            .max(WATCHDOG_MIN_STALL_MS);
        let mut claim_timeout_ms = (timeout_ms / 2).max(INFER_FALLBACK_AFTER_MS);
        let mut first_progress_timeout_ms = timeout_ms;
        if lower_model.contains("mistral-7b") {
            claim_timeout_ms = claim_timeout_ms.max(120_000);
            first_progress_timeout_ms = first_progress_timeout_ms.max(120_000);
        }
        let model_load_timeout_ms = timeout_ms
            .saturating_mul(10)
            .clamp(60_000, MAX_REMOTE_EXECUTION_TIMEOUT_MS / 2);
        let first_token_timeout_ms = timeout_ms
            .saturating_mul(12)
            .clamp(90_000, MAX_REMOTE_EXECUTION_TIMEOUT_MS / 2);
        let max_wait_ms = timeout_ms
            .saturating_add(timeout_ms / 2)
            .min(MAX_ADAPTIVE_TIMEOUT_MS.saturating_add(10_000));
        let max_execution_timeout_ms = timeout_ms.saturating_mul(24).clamp(
            MIN_REMOTE_EXECUTION_TIMEOUT_MS,
            MAX_REMOTE_EXECUTION_TIMEOUT_MS,
        );

        Self {
            timeout_ms,
            claim_timeout_ms,
            first_progress_timeout_ms,
            model_load_timeout_ms,
            first_token_timeout_ms,
            stall_timeout_ms,
            extension_step_ms: WATCHDOG_EXTENSION_STEP_MS,
            max_wait_ms,
            max_execution_timeout_ms,
            latency_class,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AttemptWatchdog {
    pub(crate) task_id: String,
    pub(crate) attempt_id: String,
    pub(crate) worker_peer_id: String,
    pub(crate) model_id: String,
    pub(crate) attempt_type: AttemptDispatchType,
    pub(crate) claimable: bool,
    pub(crate) state: AttemptLifecycleState,
    pub(crate) policy: AttemptTimeoutPolicy,
    pub(crate) started_at: tokio::time::Instant,
    pub(crate) last_progress_at: tokio::time::Instant,
    pub(crate) deadline_at: tokio::time::Instant,
    pub(crate) max_deadline_at: tokio::time::Instant,
    pub(crate) last_progress_stage: String,
    pub(crate) last_tokens_generated: u64,
    pub(crate) progress_observed: bool,
    pub(crate) no_progress_warning_emitted: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WatchdogCheck {
    Healthy,
    Extended,
    Stalled,
    TimedOut,
}

impl AttemptWatchdog {
    pub(crate) fn new(
        task_id: String,
        attempt_id: String,
        worker_peer_id: String,
        model_id: String,
        policy: AttemptTimeoutPolicy,
    ) -> Self {
        let now = tokio::time::Instant::now();
        Self {
            task_id,
            attempt_id,
            worker_peer_id,
            model_id,
            attempt_type: AttemptDispatchType::Direct,
            claimable: false,
            state: AttemptLifecycleState::Queued,
            started_at: now,
            last_progress_at: now,
            deadline_at: now + Duration::from_millis(policy.timeout_ms),
            max_deadline_at: now + Duration::from_millis(policy.max_execution_timeout_ms),
            last_progress_stage: "queued".to_string(),
            last_tokens_generated: 0,
            progress_observed: false,
            no_progress_warning_emitted: false,
            policy,
        }
    }

    pub(crate) fn new_fallback_broadcast(
        task_id: String,
        attempt_id: String,
        model_id: String,
        policy: AttemptTimeoutPolicy,
    ) -> Self {
        let mut watchdog = Self::new(
            task_id,
            attempt_id,
            UNCLAIMED_WORKER_PEER_ID.to_string(),
            model_id,
            policy,
        );
        watchdog.attempt_type = AttemptDispatchType::FallbackBroadcast;
        watchdog.claimable = true;
        watchdog
    }

    pub(crate) fn elapsed_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }

    pub(crate) fn elapsed_since_last_progress_ms(&self) -> u64 {
        self.last_progress_at.elapsed().as_millis() as u64
    }

    pub(crate) fn transition_state(&mut self, next: AttemptLifecycleState) -> bool {
        if self.state == next {
            return false;
        }
        self.state = next;
        true
    }

    pub(crate) fn is_fallback_broadcast(&self) -> bool {
        self.attempt_type == AttemptDispatchType::FallbackBroadcast
    }

    pub(crate) fn is_unclaimed(&self) -> bool {
        self.claimable && is_unclaimed_worker_peer_id(&self.worker_peer_id)
    }

    pub(crate) fn accepts_worker(&self, worker_peer_id: &str) -> bool {
        is_valid_claiming_worker(worker_peer_id)
            && (self.is_unclaimed() || self.worker_peer_id == worker_peer_id)
    }

    pub(crate) fn first_token_observed(&self) -> bool {
        self.last_tokens_generated > 0 || self.state == AttemptLifecycleState::ProducingTokens
    }

    pub(crate) fn claim_worker(
        &mut self,
        worker_peer_id: &str,
        _source: AttemptClaimSource,
    ) -> Option<String> {
        if !self.is_fallback_broadcast()
            || !self.is_unclaimed()
            || !is_valid_claiming_worker(worker_peer_id)
        {
            return None;
        }

        let previous = self.worker_peer_id.clone();
        self.worker_peer_id = worker_peer_id.to_string();
        self.claimable = false;
        Some(previous)
    }

    pub(crate) fn late_acceptance_deadline_ms(&self) -> u64 {
        self.policy
            .max_execution_timeout_ms
            .saturating_add(LATE_RESULT_ACCEPTANCE_WINDOW_MS)
    }

    pub(crate) fn record_progress(
        &mut self,
        stage: &str,
        tokens_generated_count: Option<u64>,
    ) -> bool {
        let stage_changed = self.last_progress_stage != stage;
        let token_growth = tokens_generated_count
            .map(|tokens| tokens > self.last_tokens_generated)
            .unwrap_or(false);
        let startup_heartbeat = stage == "still_running"
            && matches!(
                self.state,
                AttemptLifecycleState::LoadingModel
                    | AttemptLifecycleState::InferenceWarmup
                    | AttemptLifecycleState::Running
            )
            && !self.first_token_observed();
        let meaningful = stage_changed || token_growth || startup_heartbeat;
        if !meaningful {
            return false;
        }

        let now = tokio::time::Instant::now();
        self.last_progress_at = now;
        self.last_progress_stage = stage.to_string();
        self.progress_observed = true;
        if let Some(tokens) = tokens_generated_count {
            self.last_tokens_generated = tokens.max(self.last_tokens_generated);
        }
        if let Some(next_state) = lifecycle_state_from_progress_stage(stage) {
            let _ = self.transition_state(next_state);
        }
        if now < self.max_deadline_at {
            let proposed_deadline = now
                + Duration::from_millis(
                    self.policy
                        .stall_timeout_ms
                        .max(self.policy.extension_step_ms),
                );
            self.deadline_at = std::cmp::max(
                self.deadline_at,
                std::cmp::min(proposed_deadline, self.max_deadline_at),
            );
        }
        true
    }

    pub(crate) fn check(&mut self) -> WatchdogCheck {
        let now = tokio::time::Instant::now();
        if now >= self.max_deadline_at {
            return WatchdogCheck::TimedOut;
        }

        if !self.progress_observed {
            let first_evidence_timeout_ms = if self.is_unclaimed() {
                self.policy.claim_timeout_ms
            } else {
                self.policy.first_progress_timeout_ms
            };
            return if self.elapsed_ms() >= first_evidence_timeout_ms {
                WatchdogCheck::Stalled
            } else {
                WatchdogCheck::Healthy
            };
        }

        if !self.first_token_observed() {
            let phase_timeout_ms = match self.state {
                AttemptLifecycleState::LoadingModel => self.policy.model_load_timeout_ms,
                AttemptLifecycleState::InferenceWarmup | AttemptLifecycleState::Running => {
                    self.policy.first_token_timeout_ms
                }
                _ => 0,
            };
            if phase_timeout_ms > 0 && self.elapsed_ms() >= phase_timeout_ms {
                return WatchdogCheck::Stalled;
            }
        }

        if self.elapsed_since_last_progress_ms() >= self.policy.stall_timeout_ms {
            return WatchdogCheck::Stalled;
        }

        if now < self.deadline_at {
            return WatchdogCheck::Healthy;
        }

        let grace_deadline =
            self.last_progress_at + Duration::from_millis(self.policy.stall_timeout_ms);
        if grace_deadline > self.deadline_at {
            self.deadline_at = std::cmp::min(grace_deadline, self.max_deadline_at);
            if now < self.deadline_at {
                return WatchdogCheck::Extended;
            }
        }

        WatchdogCheck::Healthy
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct AttemptKey {
    pub(crate) task_id: String,
    pub(crate) attempt_id: String,
}

impl AttemptKey {
    pub(crate) fn new(task_id: impl Into<String>, attempt_id: impl Into<String>) -> Self {
        Self {
            task_id: task_id.into(),
            attempt_id: attempt_id.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ActiveAttempt {
    pub(crate) model_id: String,
    pub(crate) worker_peer_id: String,
    pub(crate) attempt_type: AttemptDispatchType,
}

impl ActiveAttempt {
    pub(crate) fn new(
        model_id: impl Into<String>,
        worker_peer_id: impl Into<String>,
        attempt_type: AttemptDispatchType,
    ) -> Self {
        Self {
            model_id: model_id.into(),
            worker_peer_id: worker_peer_id.into(),
            attempt_type,
        }
    }

    pub(crate) fn is_fallback_broadcast(&self) -> bool {
        self.attempt_type == AttemptDispatchType::FallbackBroadcast
    }

    pub(crate) fn accepts_worker(&self, worker_peer_id: &str) -> bool {
        is_valid_claiming_worker(worker_peer_id)
            && (is_unclaimed_worker_peer_id(&self.worker_peer_id)
                || self.worker_peer_id == worker_peer_id)
    }

    pub(crate) fn claim_worker(&mut self, worker_peer_id: &str) -> Option<String> {
        if !self.is_fallback_broadcast()
            || !is_unclaimed_worker_peer_id(&self.worker_peer_id)
            || !is_valid_claiming_worker(worker_peer_id)
        {
            return None;
        }

        let previous = self.worker_peer_id.clone();
        self.worker_peer_id = worker_peer_id.to_string();
        Some(previous)
    }
}

pub(crate) fn should_accept_result_for_attempt(
    attempt_watchdogs: &HashMap<String, AttemptWatchdog>,
    active_attempts: &HashMap<AttemptKey, ActiveAttempt>,
    expected_task_id: Option<&str>,
    task_id: &str,
    attempt_id: &str,
    worker_peer_id: &str,
    current_request_matches: bool,
) -> bool {
    if expected_task_id != Some(task_id) {
        return false;
    }

    if current_request_matches {
        return true;
    }

    if let Some(active_attempt) = active_attempts.get(&AttemptKey::new(task_id, attempt_id)) {
        return active_attempt.accepts_worker(worker_peer_id);
    }

    let Some(watchdog) = attempt_watchdogs.get(attempt_id) else {
        return false;
    };

    watchdog.task_id == task_id
        && watchdog.is_fallback_broadcast()
        && watchdog.accepts_worker(worker_peer_id)
        && watchdog.elapsed_ms() <= watchdog.late_acceptance_deadline_ms()
}
