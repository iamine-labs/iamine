mod decision;
mod policy;
mod request;

pub use decision::{PermissionDecision, PermissionEvaluation, PermissionReasonCode};
pub use policy::{
    PermissionDefaultPolicy, PermissionPolicy, PermissionPolicyError, PermissionPolicyErrorCode,
    PermissionPolicySpec,
};
pub use request::{PermissionConfirmation, PermissionRequestRef};

use crate::identifiers::{is_package_identifier, is_snake_identifier};
use crate::ScopeEvaluation;

const MAX_REQUEST_CATEGORIES: usize = 16;

pub fn evaluate_permissions(
    policy: &PermissionPolicy,
    scope: &ScopeEvaluation,
    request: PermissionRequestRef<'_>,
) -> PermissionEvaluation {
    if !scope.allowed() {
        return evaluation(
            PermissionDecision::HandoffToOrchestrator,
            PermissionReasonCode::ScopeGateNotPassed,
        );
    }
    if !valid_request_shape(&request) {
        return evaluation(
            PermissionDecision::Refuse,
            PermissionReasonCode::InvalidRequest,
        );
    }
    if !policy.package_matches(request.package_id()) {
        return evaluation(
            PermissionDecision::Refuse,
            PermissionReasonCode::PackageMismatch,
        );
    }
    if policy.blocks_action(request.action()) {
        return evaluation(
            PermissionDecision::Refuse,
            PermissionReasonCode::BlockedAction,
        );
    }
    if request
        .required_categories()
        .iter()
        .any(|category| policy.forbids_category(category))
    {
        return evaluation(
            PermissionDecision::Refuse,
            PermissionReasonCode::ForbiddenCategory,
        );
    }
    if !policy.approves_action(request.action()) {
        return evaluation(
            PermissionDecision::Refuse,
            PermissionReasonCode::UndeclaredAction,
        );
    }
    if request
        .required_categories()
        .iter()
        .any(|category| !policy.approves_category(category))
    {
        return evaluation(
            PermissionDecision::Refuse,
            PermissionReasonCode::UndeclaredCategory,
        );
    }
    if policy.requires_confirmation(request.action(), request.required_categories())
        && request.confirmation() != PermissionConfirmation::TrustedOrchestratorConfirmed
    {
        return evaluation(
            PermissionDecision::RequireConfirmation,
            PermissionReasonCode::ConfirmationRequired,
        );
    }

    evaluation(PermissionDecision::Allow, PermissionReasonCode::Permitted)
}

fn valid_request_shape(request: &PermissionRequestRef<'_>) -> bool {
    let categories = request.required_categories();
    is_package_identifier(request.package_id())
        && is_snake_identifier(request.action())
        && !categories.is_empty()
        && categories.len() <= MAX_REQUEST_CATEGORIES
        && categories.iter().enumerate().all(|(index, category)| {
            is_snake_identifier(category) && !categories[..index].contains(category)
        })
}

const fn evaluation(
    decision: PermissionDecision,
    reason: PermissionReasonCode,
) -> PermissionEvaluation {
    PermissionEvaluation::new(decision, reason)
}
