use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AttemptLifecycleState {
    Queued,
    Starting,
    LoadingModel,
    Running,
    ProducingTokens,
    Stalled,
    TimedOut,
    Completed,
    LateCompleted,
    Failed,
}

impl AttemptLifecycleState {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Starting => "starting",
            Self::LoadingModel => "loading_model",
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

pub(super) fn is_meaningfully_in_flight(state: AttemptLifecycleState) -> bool {
    matches!(
        state,
        AttemptLifecycleState::Queued
            | AttemptLifecycleState::Starting
            | AttemptLifecycleState::LoadingModel
            | AttemptLifecycleState::Running
            | AttemptLifecycleState::ProducingTokens
    )
}

fn lifecycle_state_from_progress_stage(stage: &str) -> Option<AttemptLifecycleState> {
    match stage {
        "attempt_started" => Some(AttemptLifecycleState::Starting),
        "model_load_started" => Some(AttemptLifecycleState::LoadingModel),
        "model_load_completed" | "inference_started" => Some(AttemptLifecycleState::Running),
        "first_token_generated" | "tokens_generated_count" => {
            Some(AttemptLifecycleState::ProducingTokens)
        }
        "inference_finished" | "result_serialized" | "result_published" => {
            Some(AttemptLifecycleState::Running)
        }
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AttemptTimeoutPolicy {
    pub(super) timeout_ms: u64,
    pub(super) stall_timeout_ms: u64,
    pub(super) extension_step_ms: u64,
    pub(super) max_wait_ms: u64,
    pub(super) latency_class: &'static str,
}

impl AttemptTimeoutPolicy {
    pub(super) fn from_model_and_node(model_id: &str, worker: Option<&NodeCapability>) -> Self {
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
        let max_wait_ms = timeout_ms
            .saturating_add(timeout_ms / 2)
            .min(MAX_ADAPTIVE_TIMEOUT_MS.saturating_add(10_000));

        Self {
            timeout_ms,
            stall_timeout_ms,
            extension_step_ms: WATCHDOG_EXTENSION_STEP_MS,
            max_wait_ms,
            latency_class,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct AttemptWatchdog {
    pub(super) task_id: String,
    pub(super) attempt_id: String,
    pub(super) worker_peer_id: String,
    pub(super) model_id: String,
    pub(super) state: AttemptLifecycleState,
    pub(super) policy: AttemptTimeoutPolicy,
    pub(super) started_at: tokio::time::Instant,
    pub(super) last_progress_at: tokio::time::Instant,
    pub(super) deadline_at: tokio::time::Instant,
    pub(super) max_deadline_at: tokio::time::Instant,
    pub(super) last_progress_stage: String,
    pub(super) last_tokens_generated: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WatchdogCheck {
    Healthy,
    Extended,
    Stalled,
    TimedOut,
}

impl AttemptWatchdog {
    pub(super) fn new(
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
            state: AttemptLifecycleState::Queued,
            started_at: now,
            last_progress_at: now,
            deadline_at: now + Duration::from_millis(policy.timeout_ms),
            max_deadline_at: now + Duration::from_millis(policy.max_wait_ms),
            last_progress_stage: "queued".to_string(),
            last_tokens_generated: 0,
            policy,
        }
    }

    pub(super) fn elapsed_ms(&self) -> u64 {
        self.started_at.elapsed().as_millis() as u64
    }

    pub(super) fn elapsed_since_last_progress_ms(&self) -> u64 {
        self.last_progress_at.elapsed().as_millis() as u64
    }

    pub(super) fn transition_state(&mut self, next: AttemptLifecycleState) -> bool {
        if self.state == next {
            return false;
        }
        self.state = next;
        true
    }

    pub(super) fn record_progress(
        &mut self,
        stage: &str,
        tokens_generated_count: Option<u64>,
    ) -> bool {
        let stage_changed = self.last_progress_stage != stage;
        let token_growth = tokens_generated_count
            .map(|tokens| tokens > self.last_tokens_generated)
            .unwrap_or(false);
        let meaningful = stage_changed || token_growth;
        if !meaningful {
            return false;
        }

        let now = tokio::time::Instant::now();
        self.last_progress_at = now;
        self.last_progress_stage = stage.to_string();
        if let Some(tokens) = tokens_generated_count {
            self.last_tokens_generated = tokens.max(self.last_tokens_generated);
        }
        if let Some(next_state) = lifecycle_state_from_progress_stage(stage) {
            let _ = self.transition_state(next_state);
        }
        if now < self.max_deadline_at {
            let proposed_deadline = now + Duration::from_millis(self.policy.extension_step_ms);
            self.deadline_at = std::cmp::max(
                self.deadline_at,
                std::cmp::min(proposed_deadline, self.max_deadline_at),
            );
        }
        true
    }

    pub(super) fn check(&mut self) -> WatchdogCheck {
        let now = tokio::time::Instant::now();
        if now < self.deadline_at {
            return WatchdogCheck::Healthy;
        }

        let no_progress_ms = self.elapsed_since_last_progress_ms();
        if no_progress_ms >= self.policy.stall_timeout_ms {
            return if now >= self.max_deadline_at {
                WatchdogCheck::TimedOut
            } else {
                WatchdogCheck::Stalled
            };
        }

        let grace_deadline =
            self.last_progress_at + Duration::from_millis(self.policy.stall_timeout_ms);
        if grace_deadline > self.deadline_at {
            self.deadline_at = std::cmp::min(grace_deadline, self.max_deadline_at);
            if now < self.deadline_at {
                return WatchdogCheck::Extended;
            }
        }

        if now >= self.max_deadline_at {
            WatchdogCheck::TimedOut
        } else {
            WatchdogCheck::Stalled
        }
    }
}
