use crate::infer_watchdog::{is_unclaimed_worker_peer_id, is_valid_claiming_worker};
use crate::{uuid_simple, INFER_TIMEOUT_MS, MAX_DISTRIBUTED_RETRIES};
use iamine_network::{RetryPolicy, RetryState, TaskType as PromptTaskType};
use std::collections::HashMap;

pub(crate) struct DistributedInferState {
    pub(crate) trace_task_id: String,
    pub(crate) prompt: String,
    pub(crate) model_override: Option<String>,
    pub(crate) max_tokens_override: Option<u32>,
    pub(crate) current_request_id: String,
    pub(crate) current_task_type: Option<PromptTaskType>,
    pub(crate) current_semantic_prompt: Option<String>,
    pub(crate) current_peer: Option<String>,
    pub(crate) current_model: Option<String>,
    pub(crate) candidate_models: Vec<String>,
    pub(crate) local_cluster_id: Option<String>,
    pub(crate) retry_policy: RetryPolicy,
    pub(crate) retry_state: RetryState,
    pub(crate) task_retry_count: u32,
    pub(crate) task_fallback_count: u32,
    pub(crate) attempt_order: Vec<String>,
    pub(crate) attempt_records: HashMap<String, HumanAttemptRecord>,
}

#[derive(Debug, Clone)]
pub(crate) struct HumanAttemptRecord {
    pub(crate) attempt_id: String,
    pub(crate) peer_id: String,
    pub(crate) model_id: String,
    pub(crate) state: String,
    pub(crate) outcome: String,
    pub(crate) reason: Option<String>,
}

impl HumanAttemptRecord {
    pub(crate) fn new(attempt_id: String, peer_id: String, model_id: String) -> Self {
        Self {
            attempt_id,
            peer_id,
            model_id,
            state: "starting".to_string(),
            outcome: "in_progress".to_string(),
            reason: None,
        }
    }
}

impl DistributedInferState {
    pub(crate) fn new(
        prompt: String,
        model_override: Option<String>,
        max_tokens_override: Option<u32>,
    ) -> Self {
        let trace_task_id = uuid_simple();
        Self {
            trace_task_id: trace_task_id.clone(),
            prompt,
            model_override,
            max_tokens_override,
            current_request_id: trace_task_id,
            current_task_type: None,
            current_semantic_prompt: None,
            current_peer: None,
            current_model: None,
            candidate_models: Vec::new(),
            local_cluster_id: None,
            retry_policy: RetryPolicy {
                max_retries: MAX_DISTRIBUTED_RETRIES,
                timeout_ms: INFER_TIMEOUT_MS,
            },
            retry_state: RetryState::default(),
            task_retry_count: 0,
            task_fallback_count: 0,
            attempt_order: Vec::new(),
            attempt_records: HashMap::new(),
        }
    }

    pub(crate) fn record_attempt(
        &mut self,
        peer_id: String,
        model_id: String,
        task_type: PromptTaskType,
        semantic_prompt: String,
        local_cluster_id: Option<String>,
        candidate_models: Vec<String>,
    ) {
        self.current_peer = Some(peer_id);
        self.current_model = Some(model_id);
        self.current_task_type = Some(task_type);
        self.current_semantic_prompt = Some(semantic_prompt);
        self.local_cluster_id = local_cluster_id;
        self.candidate_models = candidate_models;
    }

    pub(crate) fn register_attempt_record(
        &mut self,
        attempt_id: &str,
        peer_id: &str,
        model_id: &str,
        is_fallback: bool,
    ) {
        if !self.attempt_order.iter().any(|id| id == attempt_id) {
            self.attempt_order.push(attempt_id.to_string());
        }
        let entry = self
            .attempt_records
            .entry(attempt_id.to_string())
            .or_insert_with(|| {
                HumanAttemptRecord::new(
                    attempt_id.to_string(),
                    peer_id.to_string(),
                    model_id.to_string(),
                )
            });
        entry.peer_id = peer_id.to_string();
        entry.model_id = model_id.to_string();
        entry.state = if is_fallback {
            "queued_fallback".to_string()
        } else {
            "starting".to_string()
        };
        entry.outcome = "in_progress".to_string();
        entry.reason = None;
    }

    pub(crate) fn update_attempt_state(
        &mut self,
        attempt_id: &str,
        state: impl Into<String>,
        outcome: Option<&str>,
        reason: Option<&str>,
    ) {
        if let Some(record) = self.attempt_records.get_mut(attempt_id) {
            record.state = state.into();
            if let Some(outcome) = outcome {
                record.outcome = outcome.to_string();
            }
            if let Some(reason) = reason {
                record.reason = Some(reason.to_string());
            }
        }
    }

    pub(crate) fn claim_attempt_record(&mut self, attempt_id: &str, worker_peer_id: &str) -> bool {
        if !is_valid_claiming_worker(worker_peer_id) {
            return false;
        }

        if let Some(record) = self.attempt_records.get_mut(attempt_id) {
            if is_unclaimed_worker_peer_id(&record.peer_id) {
                record.peer_id = worker_peer_id.to_string();
                self.current_peer = Some(worker_peer_id.to_string());
                return true;
            }
        }

        false
    }

    pub(crate) fn increment_task_retry_count(&mut self) {
        self.task_retry_count = self.task_retry_count.saturating_add(1);
    }

    pub(crate) fn increment_task_fallback_count(&mut self) {
        self.task_fallback_count = self.task_fallback_count.saturating_add(1);
    }

    pub(crate) fn attempt_summary_lines(&self) -> Vec<String> {
        self.attempt_order
            .iter()
            .enumerate()
            .filter_map(|(index, attempt_id)| {
                self.attempt_records.get(attempt_id).map(|record| {
                    let reason = record
                        .reason
                        .as_deref()
                        .map(|text| format!(" reason={}", text))
                        .unwrap_or_default();
                    format!(
                        "  {}. attempt={} peer={} model={} state={} outcome={}{}",
                        index + 1,
                        record.attempt_id,
                        record.peer_id,
                        record.model_id,
                        record.state,
                        record.outcome,
                        reason
                    )
                })
            })
            .collect()
    }

    pub(crate) fn schedule_retry(
        &mut self,
        failed_peer: Option<&str>,
        failed_model: Option<&str>,
    ) -> bool {
        self.retry_state.record_failure(failed_peer, failed_model);
        if !self.retry_state.can_retry(&self.retry_policy) {
            return false;
        }

        self.retry_state.advance_retry();
        self.current_request_id = uuid_simple();
        self.current_task_type = None;
        self.current_semantic_prompt = None;
        self.current_peer = None;
        self.current_model = None;
        true
    }
}
