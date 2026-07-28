use std::sync::Arc;

use iamine_agents::{evaluate_permissions, evaluate_scope};

use crate::{
    execution_lifecycle::ExecutionIdentity, sandbox_enforcement::SandboxEvidenceIdentity,
    ExecutionLifecycleState, PackageReviewSubject, RoutingCandidateSelectionOutcome,
};

use super::{
    ExecutionAuthorizationError, ExecutionAuthorizationErrorCode, ExecutionAuthorizationRequest,
    ExecutionAuthorizationRequirement,
};

pub(crate) struct AuthorizationFacts<'subject> {
    pub(crate) subject: PackageReviewSubject<'subject>,
    pub(crate) execution: Arc<ExecutionIdentity>,
    pub(crate) sandbox: Arc<SandboxEvidenceIdentity>,
    pub(crate) selected_candidate_id: String,
    pub(crate) lifecycle_revision: u8,
}

pub(crate) fn evaluate_request<'subject>(
    request: &ExecutionAuthorizationRequest<'_, 'subject>,
) -> Result<AuthorizationFacts<'subject>, ExecutionAuthorizationError> {
    let subject = request.subject();
    let (review_authority, review_evidence) = request.review().ok_or_else(|| {
        error(
            ExecutionAuthorizationErrorCode::PackageReviewNotVerified,
            ExecutionAuthorizationRequirement::PackageReviewEvidence,
        )
    })?;
    if !review_authority.verifies(review_evidence, subject) {
        return Err(error(
            ExecutionAuthorizationErrorCode::PackageReviewNotVerified,
            ExecutionAuthorizationRequirement::PackageReviewEvidence,
        ));
    }

    let (compatibility_authority, compatibility_evidence) =
        request.compatibility().ok_or_else(|| {
            error(
                ExecutionAuthorizationErrorCode::RuntimeCompatibilityNotVerified,
                ExecutionAuthorizationRequirement::RuntimeCompatibilityEvidence,
            )
        })?;
    if !compatibility_authority.verifies(compatibility_evidence, subject) {
        return Err(error(
            ExecutionAuthorizationErrorCode::RuntimeCompatibilityNotVerified,
            ExecutionAuthorizationRequirement::RuntimeCompatibilityEvidence,
        ));
    }

    let (input_output_authority, input_output_evidence) =
        request.input_output().ok_or_else(|| {
            error(
                ExecutionAuthorizationErrorCode::InputOutputEnforcementNotVerified,
                ExecutionAuthorizationRequirement::InputOutputEnforcementEvidence,
            )
        })?;
    if !input_output_authority.verifies(input_output_evidence, subject) {
        return Err(error(
            ExecutionAuthorizationErrorCode::InputOutputEnforcementNotVerified,
            ExecutionAuthorizationRequirement::InputOutputEnforcementEvidence,
        ));
    }

    let (sandbox_authority, sandbox_evidence) = request.sandbox().ok_or_else(|| {
        error(
            ExecutionAuthorizationErrorCode::SandboxEnforcementNotVerified,
            ExecutionAuthorizationRequirement::SandboxEnforcementEvidence,
        )
    })?;
    if !sandbox_authority.verifies(sandbox_evidence, subject) {
        return Err(error(
            ExecutionAuthorizationErrorCode::SandboxEnforcementNotVerified,
            ExecutionAuthorizationRequirement::SandboxEnforcementEvidence,
        ));
    }

    let (lifecycle_authority, lifecycle_record) = request.lifecycle().ok_or_else(|| {
        error(
            ExecutionAuthorizationErrorCode::LifecycleRecordNotVerified,
            ExecutionAuthorizationRequirement::LifecycleRecord,
        )
    })?;
    if !lifecycle_authority.verifies_record(
        lifecycle_record,
        sandbox_authority,
        sandbox_evidence,
        subject,
    ) {
        return Err(error(
            ExecutionAuthorizationErrorCode::LifecycleRecordNotVerified,
            ExecutionAuthorizationRequirement::LifecycleRecord,
        ));
    }
    if lifecycle_record.state() != ExecutionLifecycleState::ScopeCheck {
        return Err(error(
            ExecutionAuthorizationErrorCode::LifecycleNotReady,
            ExecutionAuthorizationRequirement::LifecycleState,
        ));
    }

    let (timeout_authority, timeout_control) = request.timeout_cancel().ok_or_else(|| {
        error(
            ExecutionAuthorizationErrorCode::TimeoutCancelControlNotVerified,
            ExecutionAuthorizationRequirement::TimeoutCancelControl,
        )
    })?;
    if !timeout_authority.verifies_control(timeout_control, lifecycle_authority, lifecycle_record) {
        return Err(error(
            ExecutionAuthorizationErrorCode::TimeoutCancelControlNotVerified,
            ExecutionAuthorizationRequirement::TimeoutCancelControl,
        ));
    }
    if timeout_control
        .cancellation_handle()
        .cancellation_requested()
    {
        return Err(error(
            ExecutionAuthorizationErrorCode::CancellationAlreadyRequested,
            ExecutionAuthorizationRequirement::TimeoutCancelControl,
        ));
    }

    if !request
        .scope_request()
        .targets_package(subject.package_id())
        || !request
            .permission_request()
            .targets_package(subject.package_id())
    {
        return Err(error(
            ExecutionAuthorizationErrorCode::PackageIdentityMismatch,
            ExecutionAuthorizationRequirement::PackageIdentity,
        ));
    }
    let scope = evaluate_scope(request.scope_policy(), request.scope_request());
    if !scope.allowed() {
        return Err(error(
            ExecutionAuthorizationErrorCode::ScopeNotAllowed,
            ExecutionAuthorizationRequirement::ScopeEvaluation,
        ));
    }
    let permission = evaluate_permissions(
        request.permission_policy(),
        &scope,
        request.permission_request(),
    );
    if !permission.allowed() {
        return Err(error(
            ExecutionAuthorizationErrorCode::PermissionNotAllowed,
            ExecutionAuthorizationRequirement::PermissionEvaluation,
        ));
    }

    let (routing_authority, routing_evidence) = request.routing().ok_or_else(|| {
        error(
            ExecutionAuthorizationErrorCode::RoutingSelectionNotVerified,
            ExecutionAuthorizationRequirement::RoutingCandidateSelectionEvidence,
        )
    })?;
    if !routing_authority.verifies(routing_evidence) {
        return Err(error(
            ExecutionAuthorizationErrorCode::RoutingSelectionNotVerified,
            ExecutionAuthorizationRequirement::RoutingCandidateSelectionEvidence,
        ));
    }
    if routing_evidence.outcome() != RoutingCandidateSelectionOutcome::CandidateSelected {
        return Err(error(
            ExecutionAuthorizationErrorCode::CandidateNotSelected,
            ExecutionAuthorizationRequirement::RoutingCandidateSelectionEvidence,
        ));
    }
    let selected_candidate_id = routing_evidence
        .selected_candidate_id()
        .ok_or_else(|| {
            error(
                ExecutionAuthorizationErrorCode::CandidateNotSelected,
                ExecutionAuthorizationRequirement::RoutingCandidateSelectionEvidence,
            )
        })?
        .to_string();
    if !routing_authority.verifies_selected_sandbox(routing_evidence, sandbox_evidence) {
        return Err(error(
            ExecutionAuthorizationErrorCode::RoutingSubjectNotVerified,
            ExecutionAuthorizationRequirement::RoutingCandidateSelectionEvidence,
        ));
    }

    let (audit_authority, scope_audit, permission_audit, lifecycle_audit) =
        request.audit().ok_or_else(|| {
            error(
                ExecutionAuthorizationErrorCode::ScopeAuditNotVerified,
                ExecutionAuthorizationRequirement::AuditScopeEvidence,
            )
        })?;
    if !audit_authority.verifies_scope(scope_audit, &scope) {
        return Err(error(
            ExecutionAuthorizationErrorCode::ScopeAuditNotVerified,
            ExecutionAuthorizationRequirement::AuditScopeEvidence,
        ));
    }
    if !audit_authority.verifies_permission(permission_audit, &permission) {
        return Err(error(
            ExecutionAuthorizationErrorCode::PermissionAuditNotVerified,
            ExecutionAuthorizationRequirement::AuditPermissionEvidence,
        ));
    }
    if !audit_authority.verifies_lifecycle(lifecycle_audit, lifecycle_authority, lifecycle_record) {
        return Err(error(
            ExecutionAuthorizationErrorCode::LifecycleAuditNotVerified,
            ExecutionAuthorizationRequirement::AuditLifecycleEvidence,
        ));
    }

    Ok(AuthorizationFacts {
        subject,
        execution: Arc::clone(lifecycle_record.execution()),
        sandbox: Arc::clone(sandbox_evidence.identity()),
        selected_candidate_id,
        lifecycle_revision: lifecycle_record.revision(),
    })
}

const fn error(
    code: ExecutionAuthorizationErrorCode,
    requirement: ExecutionAuthorizationRequirement,
) -> ExecutionAuthorizationError {
    ExecutionAuthorizationError::new(code, requirement)
}
