use crate::log_observability_event;
use crate::model_executability::{WorkerModelExecutionGate, WorkerModelExecutionRejection};
use iamine_network::{LogLevel, SecureTransportDecision, TestnetAdmissionPolicy};
use libp2p::PeerId;
use serde_json::Map;

pub(crate) const REMOTE_INFERENCE_API_MAX_TASK_ID_BYTES: usize = 128;
pub(crate) const REMOTE_INFERENCE_API_MAX_MODEL_ID_BYTES: usize = 128;
pub(crate) const REMOTE_INFERENCE_API_MAX_PROMPT_BYTES: usize = 32 * 1024;
pub(crate) const REMOTE_INFERENCE_API_MAX_TOKENS: u32 = 4096;
pub(crate) const REMOTE_INFERENCE_API_DEFAULT_MAX_TOKENS: u32 = 200;

#[derive(Debug, Clone, Copy)]
pub(crate) struct RemoteInferenceApiRequest<'a> {
    pub(crate) task_id: &'a str,
    pub(crate) attempt_id: &'a str,
    pub(crate) model_id: &'a str,
    pub(crate) prompt: &'a str,
    pub(crate) max_tokens: u32,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct RemoteInferenceApiContext<'a> {
    pub(crate) requester_peer: &'a PeerId,
    pub(crate) secure_transport_decision: SecureTransportDecision,
    pub(crate) testnet_admission_policy: &'a TestnetAdmissionPolicy,
    pub(crate) model_execution_gate: &'a WorkerModelExecutionGate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RemoteInferenceApiRejection {
    SecureTransportRejected(SecureTransportDecision),
    PeerNotAdmitted,
    MissingTaskId,
    TaskIdTooLarge,
    MissingAttemptId,
    AttemptIdTooLarge,
    MissingModelId,
    ModelIdTooLarge,
    EmptyPrompt,
    PromptTooLarge,
    MaxTokensOutOfBounds,
    ModelExecutionBlocked(WorkerModelExecutionRejection),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RemoteInferenceApiDecision {
    rejections: Vec<RemoteInferenceApiRejection>,
}

impl RemoteInferenceApiDecision {
    pub(crate) fn is_accepted(&self) -> bool {
        self.rejections.is_empty()
    }

    pub(crate) fn rejection_codes(&self) -> Vec<&'static str> {
        self.rejections
            .iter()
            .map(RemoteInferenceApiRejection::reason_code)
            .collect()
    }

    pub(crate) fn failure_message(&self) -> String {
        let codes = self.rejection_codes();
        if codes.is_empty() {
            return "remote inference API accepted".to_string();
        }
        format!("remote inference API rejected: {}", codes.join(","))
    }
}

impl RemoteInferenceApiRejection {
    pub(crate) fn reason_code(&self) -> &'static str {
        match self {
            Self::SecureTransportRejected(decision) => decision
                .reason_code()
                .unwrap_or("secure_transport_rejected"),
            Self::PeerNotAdmitted => "peer_not_admitted",
            Self::MissingTaskId => "missing_task_id",
            Self::TaskIdTooLarge => "task_id_too_large",
            Self::MissingAttemptId => "missing_attempt_id",
            Self::AttemptIdTooLarge => "attempt_id_too_large",
            Self::MissingModelId => "missing_model_id",
            Self::ModelIdTooLarge => "model_id_too_large",
            Self::EmptyPrompt => "empty_prompt",
            Self::PromptTooLarge => "prompt_too_large",
            Self::MaxTokensOutOfBounds => "max_tokens_out_of_bounds",
            Self::ModelExecutionBlocked(reason) => worker_model_rejection_code(*reason),
        }
    }
}

pub(crate) fn remote_inference_max_tokens_from_json(raw: Option<u64>) -> u32 {
    match raw {
        Some(value) => u32::try_from(value).unwrap_or(u32::MAX),
        None => REMOTE_INFERENCE_API_DEFAULT_MAX_TOKENS,
    }
}

pub(crate) fn evaluate_remote_inference_api_request(
    request: RemoteInferenceApiRequest<'_>,
    context: RemoteInferenceApiContext<'_>,
) -> RemoteInferenceApiDecision {
    let mut rejections = Vec::new();

    if !context.secure_transport_decision.is_allowed() {
        rejections.push(RemoteInferenceApiRejection::SecureTransportRejected(
            context.secure_transport_decision,
        ));
    }
    if !context
        .testnet_admission_policy
        .allows_peer(context.requester_peer)
    {
        rejections.push(RemoteInferenceApiRejection::PeerNotAdmitted);
    }

    push_required_id_rejections(
        request.task_id,
        REMOTE_INFERENCE_API_MAX_TASK_ID_BYTES,
        RemoteInferenceApiRejection::MissingTaskId,
        RemoteInferenceApiRejection::TaskIdTooLarge,
        &mut rejections,
    );
    push_required_id_rejections(
        request.attempt_id,
        REMOTE_INFERENCE_API_MAX_TASK_ID_BYTES,
        RemoteInferenceApiRejection::MissingAttemptId,
        RemoteInferenceApiRejection::AttemptIdTooLarge,
        &mut rejections,
    );
    push_required_id_rejections(
        request.model_id,
        REMOTE_INFERENCE_API_MAX_MODEL_ID_BYTES,
        RemoteInferenceApiRejection::MissingModelId,
        RemoteInferenceApiRejection::ModelIdTooLarge,
        &mut rejections,
    );

    if request.prompt.trim().is_empty() {
        rejections.push(RemoteInferenceApiRejection::EmptyPrompt);
    } else if request.prompt.len() > REMOTE_INFERENCE_API_MAX_PROMPT_BYTES {
        rejections.push(RemoteInferenceApiRejection::PromptTooLarge);
    }

    if request.max_tokens == 0 || request.max_tokens > REMOTE_INFERENCE_API_MAX_TOKENS {
        rejections.push(RemoteInferenceApiRejection::MaxTokensOutOfBounds);
    }

    if let Some(rejection) = context.model_execution_gate.rejection {
        rejections.push(RemoteInferenceApiRejection::ModelExecutionBlocked(
            rejection,
        ));
    }

    RemoteInferenceApiDecision { rejections }
}

pub(crate) fn emit_remote_inference_api_rejected_event(
    request: RemoteInferenceApiRequest<'_>,
    requester_peer: &PeerId,
    decision: &RemoteInferenceApiDecision,
) {
    let mut fields = Map::new();
    fields.insert(
        "requester_peer".to_string(),
        requester_peer.to_string().into(),
    );
    fields.insert("model_id".to_string(), request.model_id.to_string().into());
    fields.insert(
        "reasons".to_string(),
        decision.rejection_codes().join(",").into(),
    );
    fields.insert(
        "max_tokens".to_string(),
        serde_json::Value::from(request.max_tokens as u64),
    );
    log_observability_event(
        LogLevel::Warn,
        "remote_inference_api_rejected",
        "inference",
        Some(request.task_id),
        Some(request.attempt_id),
        None,
        fields,
    );
}

fn push_required_id_rejections(
    value: &str,
    max_bytes: usize,
    missing: RemoteInferenceApiRejection,
    too_large: RemoteInferenceApiRejection,
    rejections: &mut Vec<RemoteInferenceApiRejection>,
) {
    if value.trim().is_empty() {
        rejections.push(missing);
    } else if value.len() > max_bytes {
        rejections.push(too_large);
    }
}

fn worker_model_rejection_code(rejection: WorkerModelExecutionRejection) -> &'static str {
    match rejection {
        WorkerModelExecutionRejection::MissingLocalModel => "model_not_installed",
        WorkerModelExecutionRejection::RegistryAdmissionBlocked => "registry_admission_blocked",
        WorkerModelExecutionRejection::HardwareUnsupported => "hardware_incompatible",
        WorkerModelExecutionRejection::BackendUnavailable => "backend_unavailable",
        WorkerModelExecutionRejection::NetworkPolicyBlocked => "network_policy_blocked",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model_backend_availability::ModelBackendAvailabilityDecision;

    fn request() -> RemoteInferenceApiRequest<'static> {
        RemoteInferenceApiRequest {
            task_id: "task-1",
            attempt_id: "attempt-1",
            model_id: "tinyllama-1b",
            prompt: "2+2",
            max_tokens: 128,
        }
    }

    fn gate(rejection: Option<WorkerModelExecutionRejection>) -> WorkerModelExecutionGate {
        WorkerModelExecutionGate {
            local_model_available: true,
            mock_backend_enabled: false,
            real_inference_available: true,
            backend_availability: ModelBackendAvailabilityDecision::available(),
            inference_eligibility: None,
            network_policy: None,
            rejection,
        }
    }

    fn decision(
        request: RemoteInferenceApiRequest<'_>,
        peer: &PeerId,
        policy: &TestnetAdmissionPolicy,
        secure_transport_decision: SecureTransportDecision,
        gate: &WorkerModelExecutionGate,
    ) -> RemoteInferenceApiDecision {
        evaluate_remote_inference_api_request(
            request,
            RemoteInferenceApiContext {
                requester_peer: peer,
                secure_transport_decision,
                testnet_admission_policy: policy,
                model_execution_gate: gate,
            },
        )
    }

    #[test]
    fn remote_inference_api_accepts_authenticated_admitted_bounded_request() {
        let peer = PeerId::random();
        let policy = TestnetAdmissionPolicy::allowlist(vec![peer]);
        let gate = gate(None);

        let decision = decision(
            request(),
            &peer,
            &policy,
            SecureTransportDecision::Allowed,
            &gate,
        );

        assert!(decision.is_accepted());
    }

    #[test]
    fn remote_inference_api_rejects_transport_downgrade() {
        let peer = PeerId::random();
        let policy = TestnetAdmissionPolicy::open();
        let gate = gate(None);

        let decision = decision(
            request(),
            &peer,
            &policy,
            SecureTransportDecision::UnauthenticatedTransportRejected,
            &gate,
        );

        assert_eq!(
            decision.rejection_codes(),
            vec!["unauthenticated_transport_rejected"]
        );
    }

    #[test]
    fn remote_inference_api_rejects_peer_outside_testnet_allowlist() {
        let allowed = PeerId::random();
        let denied = PeerId::random();
        let policy = TestnetAdmissionPolicy::allowlist(vec![allowed]);
        let gate = gate(None);

        let decision = decision(
            request(),
            &denied,
            &policy,
            SecureTransportDecision::Allowed,
            &gate,
        );

        assert_eq!(decision.rejection_codes(), vec!["peer_not_admitted"]);
    }

    #[test]
    fn remote_inference_api_rejects_unbounded_payloads() {
        let peer = PeerId::random();
        let policy = TestnetAdmissionPolicy::open();
        let gate = gate(None);
        let mut request = request();
        let prompt = "x".repeat(REMOTE_INFERENCE_API_MAX_PROMPT_BYTES + 1);
        request.prompt = &prompt;
        request.max_tokens = REMOTE_INFERENCE_API_MAX_TOKENS + 1;

        let decision = decision(
            request,
            &peer,
            &policy,
            SecureTransportDecision::Allowed,
            &gate,
        );

        assert_eq!(
            decision.rejection_codes(),
            vec!["prompt_too_large", "max_tokens_out_of_bounds"]
        );
    }

    #[test]
    fn remote_inference_api_rejects_non_executable_model_gate() {
        let peer = PeerId::random();
        let policy = TestnetAdmissionPolicy::open();
        let gate = gate(Some(WorkerModelExecutionRejection::NetworkPolicyBlocked));

        let decision = decision(
            request(),
            &peer,
            &policy,
            SecureTransportDecision::Allowed,
            &gate,
        );

        assert_eq!(decision.rejection_codes(), vec!["network_policy_blocked"]);
    }

    #[test]
    fn remote_inference_json_max_tokens_marks_overflow_for_rejection() {
        assert_eq!(
            remote_inference_max_tokens_from_json(None),
            REMOTE_INFERENCE_API_DEFAULT_MAX_TOKENS
        );
        assert_eq!(remote_inference_max_tokens_from_json(Some(42)), 42);
        assert_eq!(
            remote_inference_max_tokens_from_json(Some(u64::MAX)),
            u32::MAX
        );
    }
}
