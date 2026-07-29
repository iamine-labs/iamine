use std::error::Error;

use iamine_agent_runtime::{
    AuditEventEnforcementAuthority, AuditEventEnforcementEvidence, DependencyPolicyReviewDecision,
    ExecutionAuthorizationRequest, ExecutionLifecycleAuthority, ExecutionLifecycleRecord,
    ExecutionLifecycleState, HumanReviewDecision, InputOutputEnforcementAuthority,
    InputOutputEnforcementEvidence, InputOutputPolicy, LanguagePolicyReviewDecision,
    LocalRegistryReviewDecision, PackageReviewAuthority, PackageReviewDecisions,
    PackageReviewEvidence, PackageReviewSubject, RoutingCandidateAvailability,
    RoutingCandidateCompatibility, RoutingCandidateRef, RoutingCandidateRiskClass,
    RoutingCandidateSandbox, RoutingCandidateSelectionAuthority, RoutingCandidateSelectionEvidence,
    RoutingResourceRequirements, RoutingSelectionRequestRef, RuntimeCompatibilityAuthority,
    RuntimeCompatibilityEvidence, RuntimeLanguageAvailability, RuntimeLanguageDecision,
    RuntimeLanguageMode, RuntimeNetworkAvailability, RuntimeResourceEnvelope,
    SandboxEnforcementAuthority, SandboxEnforcementEvidence, SandboxEnforcementPolicy,
    TimeoutCancelAuthority, TimeoutCancelControl, TimeoutCancelPolicy,
};
use iamine_agents::{
    evaluate_permissions, evaluate_scope, PermissionConfirmation, PermissionEvaluation,
    PermissionPolicy, ResourceOperatingMode, ScopeEvaluation, ScopePolicy,
    ScopeRequestClassification,
};

use super::routing_policy::{
    permission_policy, permission_request, scope_policy, scope_request, PACKAGE_ID, TASK_TYPE,
};

type TestResult<T> = Result<T, Box<dyn Error>>;
const LOCAL_READONLY_CATEGORIES: [&str; 1] = ["local_readonly"];

pub struct PreparedAuthorizationChain<'subject> {
    pub subject: PackageReviewSubject<'subject>,
    pub review_authority: PackageReviewAuthority,
    pub review_evidence: PackageReviewEvidence<'subject>,
    pub compatibility_authority: RuntimeCompatibilityAuthority,
    pub compatibility_evidence: RuntimeCompatibilityEvidence<'subject>,
    pub input_output_authority: InputOutputEnforcementAuthority,
    pub input_output_evidence: InputOutputEnforcementEvidence<'subject>,
    pub sandbox_authority: SandboxEnforcementAuthority,
    pub sandbox_evidence: SandboxEnforcementEvidence<'subject>,
    pub lifecycle_authority: ExecutionLifecycleAuthority,
    pub lifecycle_record: ExecutionLifecycleRecord<'subject>,
    pub timeout_authority: TimeoutCancelAuthority,
    pub timeout_control: TimeoutCancelControl,
    pub scope_policy: ScopePolicy,
    pub scope: ScopeEvaluation,
    pub permission_policy: PermissionPolicy,
    pub permission: PermissionEvaluation,
    pub routing_authority: RoutingCandidateSelectionAuthority,
    pub routing_evidence: RoutingCandidateSelectionEvidence,
    pub audit_authority: AuditEventEnforcementAuthority,
    pub scope_audit: AuditEventEnforcementEvidence,
    pub permission_audit: AuditEventEnforcementEvidence,
    pub lifecycle_audit: AuditEventEnforcementEvidence,
}

impl<'subject> PreparedAuthorizationChain<'subject> {
    pub fn new(subject: PackageReviewSubject<'subject>) -> TestResult<Self> {
        Self::new_with_timeout_policy(
            subject,
            TimeoutCancelPolicy::new(1_000, 1_000, 1_000, 1_000, 1_000, 1_000)?,
        )
    }

    pub fn new_with_timeout_policy(
        subject: PackageReviewSubject<'subject>,
        timeout_policy: TimeoutCancelPolicy,
    ) -> TestResult<Self> {
        let review_authority = PackageReviewAuthority::new_operator_local();
        let review_evidence = review_authority.issue(subject, approved_review_decisions())?;
        let compatibility_authority = RuntimeCompatibilityAuthority::new_operator_local(
            RuntimeLanguageDecision::new(
                RuntimeLanguageMode::RustNativeOfficial,
                RuntimeLanguageAvailability::Available,
            ),
            RuntimeResourceEnvelope::new(2, 512, 84, RuntimeNetworkAvailability::None)?,
        );
        let compatibility_evidence =
            compatibility_authority.evaluate(&review_authority, &review_evidence, subject)?;
        let input_output_authority = InputOutputEnforcementAuthority::new_operator_local(
            InputOutputPolicy::new(128, 128, false)?,
        );
        let input_output_evidence = input_output_authority.establish(
            &compatibility_authority,
            &compatibility_evidence,
            subject,
        )?;
        let sandbox_authority = SandboxEnforcementAuthority::new_operator_local(
            SandboxEnforcementPolicy::new(30_000, 128)?,
        )?;
        let sandbox_evidence = sandbox_authority.establish(
            &compatibility_authority,
            &compatibility_evidence,
            &input_output_authority,
            &input_output_evidence,
            subject,
        )?;
        let lifecycle_authority = ExecutionLifecycleAuthority::new_operator_local();
        let mut lifecycle_record =
            lifecycle_authority.queue(&sandbox_authority, &sandbox_evidence, subject)?;
        let _ = lifecycle_authority.transition(
            &mut lifecycle_record,
            0,
            ExecutionLifecycleState::PermissionPending,
        )?;
        let _ = lifecycle_authority.transition(
            &mut lifecycle_record,
            1,
            ExecutionLifecycleState::ScopeCheck,
        )?;
        let timeout_authority = TimeoutCancelAuthority::new_operator_local();
        let timeout_control = timeout_authority.establish(
            &lifecycle_authority,
            &lifecycle_record,
            &sandbox_authority,
            &sandbox_evidence,
            subject,
            timeout_policy,
        )?;
        let scope_policy = scope_policy()?;
        let scope = evaluate_scope(
            &scope_policy,
            scope_request(PACKAGE_ID, ScopeRequestClassification::InScopeCandidate),
        );
        let permission_policy = permission_policy()?;
        let permission = evaluate_permissions(
            &permission_policy,
            &scope,
            permission_request(
                PACKAGE_ID,
                "inspect_status",
                &LOCAL_READONLY_CATEGORIES,
                PermissionConfirmation::NotProvided,
            ),
        );
        let routing_authority = RoutingCandidateSelectionAuthority::new_operator_local();
        let routing_evidence = routing_authority.select(
            RoutingSelectionRequestRef::new(
                TASK_TYPE,
                ResourceOperatingMode::LocalReadonly,
                RoutingResourceRequirements::new(1, 256, 20, RuntimeNetworkAvailability::None)?,
                RoutingCandidateRiskClass::Moderate,
            ),
            &[RoutingCandidateRef::new(
                "candidate-local",
                TASK_TYPE,
                RoutingCandidateRiskClass::Low,
                RoutingCandidateAvailability::Available,
                subject,
                scope,
                permission,
                RoutingCandidateCompatibility::Compatible(&compatibility_evidence),
                RoutingCandidateSandbox::Prepared(&sandbox_evidence),
            )],
            &compatibility_authority,
            &sandbox_authority,
        )?;
        let audit_authority = AuditEventEnforcementAuthority::new_operator_local();
        let scope_audit = audit_authority.enforce_scope(&scope);
        let permission_audit = audit_authority.enforce_permission(&permission);
        let lifecycle_audit =
            audit_authority.enforce_lifecycle(&lifecycle_authority, &lifecycle_record)?;

        Ok(Self {
            subject,
            review_authority,
            review_evidence,
            compatibility_authority,
            compatibility_evidence,
            input_output_authority,
            input_output_evidence,
            sandbox_authority,
            sandbox_evidence,
            lifecycle_authority,
            lifecycle_record,
            timeout_authority,
            timeout_control,
            scope_policy,
            scope,
            permission_policy,
            permission,
            routing_authority,
            routing_evidence,
            audit_authority,
            scope_audit,
            permission_audit,
            lifecycle_audit,
        })
    }

    pub fn request(&self) -> ExecutionAuthorizationRequest<'_, 'subject> {
        self.request_with(
            scope_request(PACKAGE_ID, ScopeRequestClassification::InScopeCandidate),
            permission_request(
                PACKAGE_ID,
                "inspect_status",
                &LOCAL_READONLY_CATEGORIES,
                PermissionConfirmation::NotProvided,
            ),
        )
    }

    pub fn request_with<'context>(
        &'context self,
        scope: iamine_agents::ScopeRequestRef<'context>,
        permission: iamine_agents::PermissionRequestRef<'context>,
    ) -> ExecutionAuthorizationRequest<'context, 'subject> {
        ExecutionAuthorizationRequest::new(
            self.subject,
            &self.scope_policy,
            scope,
            &self.permission_policy,
            permission,
        )
        .with_package_review(&self.review_authority, &self.review_evidence)
        .with_runtime_compatibility(&self.compatibility_authority, &self.compatibility_evidence)
        .with_input_output(&self.input_output_authority, &self.input_output_evidence)
        .with_sandbox(&self.sandbox_authority, &self.sandbox_evidence)
        .with_lifecycle(&self.lifecycle_authority, &self.lifecycle_record)
        .with_timeout_cancel(&self.timeout_authority, &self.timeout_control)
        .with_routing(&self.routing_authority, &self.routing_evidence)
        .with_audit(
            &self.audit_authority,
            &self.scope_audit,
            &self.permission_audit,
            &self.lifecycle_audit,
        )
    }
}

fn approved_review_decisions() -> PackageReviewDecisions {
    PackageReviewDecisions::new(
        LocalRegistryReviewDecision::RegistryReviewReady,
        LanguagePolicyReviewDecision::RustOfficialAllowed,
        DependencyPolicyReviewDecision::Allowed,
        HumanReviewDecision::IndependentApproved,
    )
}
