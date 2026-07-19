mod decision;
mod policy;
mod request;

pub use decision::{ScopeDecision, ScopeEvaluation, ScopeReasonCode};
pub use policy::{ScopePolicy, ScopePolicyError, ScopePolicyErrorCode, ScopePolicySpec};
pub use request::{ScopeRequestClassification, ScopeRequestRef};

use crate::identifiers::{is_package_identifier, is_snake_identifier};

const MAX_REQUEST_INPUT_CLASSES: usize = 16;

pub fn evaluate_scope(policy: &ScopePolicy, request: ScopeRequestRef<'_>) -> ScopeEvaluation {
    match request.classification() {
        ScopeRequestClassification::Ambiguous => {
            return evaluation(ScopeDecision::Clarify, ScopeReasonCode::AmbiguousTask);
        }
        ScopeRequestClassification::Dangerous => {
            return evaluation(ScopeDecision::Refuse, ScopeReasonCode::DangerousTask);
        }
        ScopeRequestClassification::CrossDomain => {
            return evaluation(
                ScopeDecision::HandoffToOrchestrator,
                ScopeReasonCode::CrossDomainTask,
            );
        }
        ScopeRequestClassification::PermissionEscalation => {
            return evaluation(ScopeDecision::Refuse, ScopeReasonCode::PermissionEscalation);
        }
        ScopeRequestClassification::PromptInjection => {
            return evaluation(ScopeDecision::Refuse, ScopeReasonCode::PromptInjection);
        }
        ScopeRequestClassification::RoleConfusion => {
            return evaluation(ScopeDecision::Refuse, ScopeReasonCode::RoleConfusion);
        }
        ScopeRequestClassification::InScopeCandidate => {}
    }

    if !valid_request_shape(&request) {
        return evaluation(
            ScopeDecision::HandoffToOrchestrator,
            ScopeReasonCode::InvalidRequest,
        );
    }
    if !policy.package_matches(request.package_id()) {
        return evaluation(
            ScopeDecision::HandoffToOrchestrator,
            ScopeReasonCode::PackageMismatch,
        );
    }
    if !policy.supports_task_type(request.task_type()) {
        return evaluation(
            ScopeDecision::HandoffToOrchestrator,
            ScopeReasonCode::UnsupportedTaskType,
        );
    }
    if policy.excludes_task(request.task()) || !policy.includes_task(request.task()) {
        return evaluation(
            ScopeDecision::HandoffToOrchestrator,
            ScopeReasonCode::OutsideDeclaredScope,
        );
    }
    if policy.blocks_action(request.operation()) {
        return evaluation(ScopeDecision::Refuse, ScopeReasonCode::BlockedAction);
    }
    if !policy.allows_operation(request.operation()) {
        return evaluation(
            ScopeDecision::HandoffToOrchestrator,
            ScopeReasonCode::UnsupportedOperation,
        );
    }
    if request
        .input_classes()
        .iter()
        .any(|input| policy.forbids_input(input))
    {
        return evaluation(ScopeDecision::Refuse, ScopeReasonCode::ForbiddenInput);
    }
    if request
        .input_classes()
        .iter()
        .any(|input| !policy.allows_input(input))
    {
        return evaluation(
            ScopeDecision::HandoffToOrchestrator,
            ScopeReasonCode::UnsupportedInput,
        );
    }

    evaluation(ScopeDecision::Allow, ScopeReasonCode::InScope)
}

fn valid_request_shape(request: &ScopeRequestRef<'_>) -> bool {
    is_package_identifier(request.package_id())
        && is_snake_identifier(request.task_type())
        && is_snake_identifier(request.task())
        && is_snake_identifier(request.operation())
        && request.input_classes().len() <= MAX_REQUEST_INPUT_CLASSES
        && request
            .input_classes()
            .iter()
            .all(|input| is_snake_identifier(input))
}

const fn evaluation(decision: ScopeDecision, reason: ScopeReasonCode) -> ScopeEvaluation {
    ScopeEvaluation::new(decision, reason)
}
