use super::*;

pub(super) struct ClientRuntimeState {
    pub(super) pending_tasks: Vec<TaskRequest>,
    pub(super) waiting_for_response: bool,
    pub(super) completed: usize,
    pub(super) total_tasks: usize,
    pub(super) connected_peer: Option<PeerId>,
    pub(super) known_workers: HashSet<PeerId>,
    pub(super) origin_peer_map: HashMap<String, PeerId>,
    pub(super) tasks_sent: bool,
}

impl ClientRuntimeState {
    pub(super) fn new() -> Self {
        Self {
            pending_tasks: Vec::new(),
            waiting_for_response: false,
            completed: 0,
            total_tasks: 0,
            connected_peer: None,
            known_workers: HashSet::new(),
            origin_peer_map: HashMap::new(),
            tasks_sent: false,
        }
    }

    pub(super) fn prime_tasks_from_mode(&mut self, mode: &NodeMode) {
        match mode {
            NodeMode::Client {
                task_type, data, ..
            } => {
                self.pending_tasks.push(TaskRequest::legacy(
                    uuid_simple(),
                    task_type.clone(),
                    data.clone(),
                ));
                self.total_tasks = 1;
            }
            NodeMode::Stress { count, .. } => {
                self.total_tasks = *count;
                for i in 0..*count {
                    self.pending_tasks.push(TaskRequest::legacy(
                        format!("stress_{:03}", i + 1),
                        "reverse_string".to_string(),
                        format!("iamine_{}", i + 1),
                    ));
                }
                println!("🔥 Stress test: {} tareas preparadas\n", count);
            }
            _ => {}
        }
    }
}

pub(super) struct DistributedInferState {
    pub(super) trace_task_id: String,
    pub(super) prompt: String,
    pub(super) model_override: Option<String>,
    pub(super) max_tokens_override: Option<u32>,
    pub(super) current_request_id: String,
    pub(super) current_task_type: Option<PromptTaskType>,
    pub(super) current_semantic_prompt: Option<String>,
    pub(super) current_peer: Option<String>,
    pub(super) current_model: Option<String>,
    pub(super) candidate_models: Vec<String>,
    pub(super) local_cluster_id: Option<String>,
    pub(super) retry_policy: RetryPolicy,
    pub(super) retry_state: RetryState,
}

impl DistributedInferState {
    pub(super) fn new(
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
        }
    }

    pub(super) fn record_attempt(
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

    pub(super) fn schedule_retry(
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

pub(super) struct InferRuntimeState {
    pub(super) infer_request_id: Option<String>,
    pub(super) infer_broadcast_sent: bool,
    pub(super) pending_inference: HashMap<String, tokio::time::Instant>,
    pub(super) attempt_watchdogs: HashMap<String, AttemptWatchdog>,
    pub(super) infer_started_at: Option<tokio::time::Instant>,
    pub(super) distributed_infer_state: Option<DistributedInferState>,
    pub(super) token_buffer: HashMap<u32, String>,
    pub(super) next_token_idx: u32,
    pub(super) rendered_output: String,
}

impl InferRuntimeState {
    pub(super) fn new() -> Self {
        Self {
            infer_request_id: None,
            infer_broadcast_sent: false,
            pending_inference: HashMap::new(),
            attempt_watchdogs: HashMap::new(),
            infer_started_at: None,
            distributed_infer_state: None,
            token_buffer: HashMap::new(),
            next_token_idx: 0,
            rendered_output: String::new(),
        }
    }

    pub(super) fn initialize_infer_mode_if_needed(
        &mut self,
        mode: &NodeMode,
        model_storage: &ModelStorage,
    ) {
        if let NodeMode::Infer {
            prompt,
            model_id,
            max_tokens_override,
            ..
        } = mode
        {
            let local_available = model_storage.list_local_models();
            let resolution =
                resolve_policy_for_prompt(prompt, model_id.as_deref(), &local_available);
            let profile = resolution.profile;
            let candidates = resolution.candidate_models;
            let selected = resolution.selected_model;
            let semantic_prompt = resolution.semantic_prompt;
            let output_policy =
                resolve_output_policy(&profile, &semantic_prompt, *max_tokens_override);
            println!(
                "🧠 Distributing inference: model={} prompt='{}'",
                selected, semantic_prompt
            );
            println!("   Candidates: {}", candidates.join(", "));
            println!(
                "   [OutputPolicy] max_tokens: {} (reason: {})",
                output_policy.max_tokens, output_policy.reason
            );

            let infer_state =
                DistributedInferState::new(prompt.clone(), model_id.clone(), *max_tokens_override);
            let _ = log_structured(prompt_log_entry(
                &infer_state.trace_task_id,
                &semantic_prompt,
                &format!("{:?}", profile.language),
                &format!("{:?}", profile.task_type),
                profile.confidence,
            ));
            println!("   Task ID: {}", infer_state.trace_task_id);
            println!("   Request ID: {}", infer_state.current_request_id);

            self.infer_request_id = Some(infer_state.current_request_id.clone());
            self.infer_started_at = Some(tokio::time::Instant::now());
            let _ = record_distributed_task_started();
            self.distributed_infer_state = Some(infer_state);
        }
    }
}
