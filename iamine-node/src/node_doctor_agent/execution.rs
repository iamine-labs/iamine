use std::path::Path;

use iamine_agent_runtime::{
    AuditEventEnforcementAuthority, AuditEventEnforcementEvidence, DependencyPolicyReviewDecision,
    ExecutionAuthorizationAuthority, ExecutionAuthorizationRequest, ExecutionLifecycleAuthority,
    ExecutionLifecycleRecord, ExecutionLifecycleState, HumanReviewDecision, InputClassification,
    InputOutputEnforcementAuthority, InputOutputEnforcementEvidence, InputOutputPolicy,
    LanguagePolicyReviewDecision, LoadedAgentPackage, LocalRegistryReviewDecision,
    OfficialRustProgram, OfficialRustProgramRegistry, PackageLoadEvidenceAuthority,
    PackageLoaderAuthority, PackageReviewAuthority, PackageReviewDecisions, PackageReviewEvidence,
    PackageReviewSubject, RoutingCandidateAvailability, RoutingCandidateCompatibility,
    RoutingCandidateRef, RoutingCandidateRiskClass, RoutingCandidateSandbox,
    RoutingCandidateSelectionAuthority, RoutingCandidateSelectionEvidence,
    RoutingResourceRequirements, RoutingSelectionRequestRef, RuntimeCompatibilityAuthority,
    RuntimeCompatibilityEvidence, RuntimeExecutionPermit, RuntimeExecutionPreparation,
    RuntimeExecutionRequest, RuntimeExecutionVerification, RuntimeExecutorAuthority,
    RuntimeLanguageAvailability, RuntimeLanguageDecision, RuntimeLanguageMode,
    RuntimeNetworkAvailability, RuntimeResourceEnvelope, SandboxEnforcementAuthority,
    SandboxEnforcementEvidence, SandboxEnforcementPolicy, TimeoutCancelAuthority,
    TimeoutCancelControl, TimeoutCancelPolicy,
};
use iamine_agents::{
    evaluate_permissions, evaluate_scope, PermissionConfirmation, PermissionEvaluation,
    PermissionPolicy, PermissionRequestRef, ResourceOperatingMode, ScopeEvaluation, ScopePolicy,
    ScopeRequestClassification, ScopeRequestRef,
};
use serde::Serialize;

use super::{
    package::VerifiedNodeDoctorPackage, policy::runtime_policies, register_node_doctor_program,
    NodeDoctorAgentError, NodeDoctorAgentErrorCode, NODE_DOCTOR_OUTPUT_SCHEMA_VERSION,
    NODE_DOCTOR_PACKAGE_ID, NODE_DOCTOR_SCOPE_ID, NODE_DOCTOR_TASK_INPUT, NODE_DOCTOR_TASK_TYPE,
};

const INPUT_CLASSES: [&str; 1] = ["iamine_node_status_summary"];
const REQUIRED_CATEGORIES: [&str; 2] = ["local_readonly", "redacted_status_summary"];

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct NodeDoctorAgentExecution {
    pub(crate) schema_version: &'static str,
    pub(crate) package_id: &'static str,
    pub(crate) task_type: &'static str,
    pub(crate) scope_id: &'static str,
    pub(crate) status: &'static str,
    pub(crate) classification: &'static str,
    pub(crate) content: String,
    pub(crate) package_loaded: bool,
    pub(crate) execution_authorized: bool,
    pub(crate) sandbox_adapter_was_active: bool,
    pub(crate) os_isolation_claimed: bool,
    pub(crate) cleanup_completed: bool,
    pub(crate) audit_recorded: bool,
    pub(crate) scheduler_mutated: bool,
    pub(crate) transport_started: bool,
    pub(crate) persisted: bool,
}

pub(crate) fn execute_node_doctor_agent(
    package_root: &Path,
) -> Result<NodeDoctorAgentExecution, NodeDoctorAgentError> {
    let package = VerifiedNodeDoctorPackage::load(package_root)?;
    let subject = package.subject();
    let (scope_policy, permission_policy) = runtime_policies()?;
    execute_verified_package(subject, scope_policy, permission_policy)
}

fn execute_verified_package<'subject>(
    subject: PackageReviewSubject<'subject>,
    scope_policy: ScopePolicy,
    permission_policy: PermissionPolicy,
) -> Result<NodeDoctorAgentExecution, NodeDoctorAgentError> {
    let mut chain = PreparedChain::new(subject, scope_policy, permission_policy)?;
    let input = enforced_input(&chain)?;
    let executor = RuntimeExecutorAuthority::new_operator_local();
    let mut runtime = PreparedRuntime::new(&chain, &executor)?;
    let result = executor
        .execute(runtime.request(&mut chain, &input)?)
        .map_err(|_| runtime_rejected())?;
    let verified = executor.verifies_result(
        RuntimeExecutionVerification::new(
            &result,
            &runtime.loaded,
            &chain.lifecycle_authority,
            &chain.lifecycle_record,
        )
        .with_program(&runtime.registry, &runtime.program)
        .with_audit(&chain.audit_authority),
    );
    if !verified || result.output().scope_id() != NODE_DOCTOR_SCOPE_ID {
        return Err(NodeDoctorAgentError::new(
            NodeDoctorAgentErrorCode::OutputVerificationFailed,
        ));
    }

    Ok(NodeDoctorAgentExecution {
        schema_version: NODE_DOCTOR_OUTPUT_SCHEMA_VERSION,
        package_id: NODE_DOCTOR_PACKAGE_ID,
        task_type: NODE_DOCTOR_TASK_TYPE,
        scope_id: NODE_DOCTOR_SCOPE_ID,
        status: "completed",
        classification: result.output().classification().as_str(),
        content: result.output().redacted_content().to_string(),
        package_loaded: result.package_loaded(),
        execution_authorized: result.execution_authorized(),
        sandbox_adapter_was_active: result.sandbox_adapter_was_active(),
        os_isolation_claimed: result.os_isolation_claimed(),
        cleanup_completed: result.cleanup_completed(),
        audit_recorded: result.audit_recorded(),
        scheduler_mutated: result.scheduler_mutated(),
        transport_started: result.transport_started(),
        persisted: result.persisted(),
    })
}

struct PreparedChain<'subject> {
    subject: PackageReviewSubject<'subject>,
    review_authority: PackageReviewAuthority,
    review_evidence: PackageReviewEvidence<'subject>,
    compatibility_authority: RuntimeCompatibilityAuthority,
    compatibility_evidence: RuntimeCompatibilityEvidence<'subject>,
    input_output_authority: InputOutputEnforcementAuthority,
    input_output_evidence: InputOutputEnforcementEvidence<'subject>,
    sandbox_authority: SandboxEnforcementAuthority,
    sandbox_evidence: SandboxEnforcementEvidence<'subject>,
    lifecycle_authority: ExecutionLifecycleAuthority,
    lifecycle_record: ExecutionLifecycleRecord<'subject>,
    timeout_authority: TimeoutCancelAuthority,
    timeout_control: TimeoutCancelControl,
    scope_policy: ScopePolicy,
    permission_policy: PermissionPolicy,
    routing_authority: RoutingCandidateSelectionAuthority,
    routing_evidence: RoutingCandidateSelectionEvidence,
    audit_authority: AuditEventEnforcementAuthority,
    scope_audit: AuditEventEnforcementEvidence,
    permission_audit: AuditEventEnforcementEvidence,
    lifecycle_audit: AuditEventEnforcementEvidence,
}

impl<'subject> PreparedChain<'subject> {
    fn new(
        subject: PackageReviewSubject<'subject>,
        scope_policy: ScopePolicy,
        permission_policy: PermissionPolicy,
    ) -> Result<Self, NodeDoctorAgentError> {
        let review_authority = PackageReviewAuthority::new_operator_local();
        let review_evidence = review_authority
            .issue(subject, approved_review_decisions())
            .map_err(|_| runtime_rejected())?;
        let compatibility_authority = RuntimeCompatibilityAuthority::new_operator_local(
            RuntimeLanguageDecision::new(
                RuntimeLanguageMode::RustNativeOfficial,
                RuntimeLanguageAvailability::Available,
            ),
            RuntimeResourceEnvelope::new(2, 512, 84, RuntimeNetworkAvailability::None)
                .map_err(|_| runtime_rejected())?,
        );
        let compatibility_evidence = compatibility_authority
            .evaluate(&review_authority, &review_evidence, subject)
            .map_err(|_| runtime_rejected())?;
        let input_output_authority = InputOutputEnforcementAuthority::new_operator_local(
            InputOutputPolicy::new(256, 4_096, true).map_err(|_| runtime_rejected())?,
        );
        let input_output_evidence = input_output_authority
            .establish(&compatibility_authority, &compatibility_evidence, subject)
            .map_err(|_| runtime_rejected())?;
        let sandbox_authority = SandboxEnforcementAuthority::new_operator_local(
            SandboxEnforcementPolicy::new(30_000, 128).map_err(|_| runtime_rejected())?,
        )
        .map_err(|_| runtime_rejected())?;
        let sandbox_evidence = sandbox_authority
            .establish(
                &compatibility_authority,
                &compatibility_evidence,
                &input_output_authority,
                &input_output_evidence,
                subject,
            )
            .map_err(|_| runtime_rejected())?;
        let lifecycle_authority = ExecutionLifecycleAuthority::new_operator_local();
        let mut lifecycle_record = lifecycle_authority
            .queue(&sandbox_authority, &sandbox_evidence, subject)
            .map_err(|_| runtime_rejected())?;
        let _ = lifecycle_authority
            .transition(
                &mut lifecycle_record,
                0,
                ExecutionLifecycleState::PermissionPending,
            )
            .map_err(|_| runtime_rejected())?;
        let _ = lifecycle_authority
            .transition(
                &mut lifecycle_record,
                1,
                ExecutionLifecycleState::ScopeCheck,
            )
            .map_err(|_| runtime_rejected())?;
        let timeout_authority = TimeoutCancelAuthority::new_operator_local();
        let timeout_control = timeout_authority
            .establish(
                &lifecycle_authority,
                &lifecycle_record,
                &sandbox_authority,
                &sandbox_evidence,
                subject,
                TimeoutCancelPolicy::new(1_000, 1_000, 1_000, 1_000, 1_000, 1_000)
                    .map_err(|_| runtime_rejected())?,
            )
            .map_err(|_| runtime_rejected())?;
        let scope = evaluate_scope(
            &scope_policy,
            scope_request(ScopeRequestClassification::InScopeCandidate),
        );
        let permission = evaluate_permissions(&permission_policy, &scope, permission_request());
        let routing_authority = RoutingCandidateSelectionAuthority::new_operator_local();
        let routing_evidence = routing_authority
            .select(
                routing_request()?,
                &[routing_candidate(
                    subject,
                    scope,
                    permission,
                    &compatibility_evidence,
                    &sandbox_evidence,
                )],
                &compatibility_authority,
                &sandbox_authority,
            )
            .map_err(|_| runtime_rejected())?;
        let audit_authority = AuditEventEnforcementAuthority::new_operator_local();
        let scope_audit = audit_authority.enforce_scope(&scope);
        let permission_audit = audit_authority.enforce_permission(&permission);
        let lifecycle_audit = audit_authority
            .enforce_lifecycle(&lifecycle_authority, &lifecycle_record)
            .map_err(|_| runtime_rejected())?;

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
            permission_policy,
            routing_authority,
            routing_evidence,
            audit_authority,
            scope_audit,
            permission_audit,
            lifecycle_audit,
        })
    }

    fn request(&self) -> ExecutionAuthorizationRequest<'_, 'subject> {
        ExecutionAuthorizationRequest::new(
            self.subject,
            &self.scope_policy,
            scope_request(ScopeRequestClassification::InScopeCandidate),
            &self.permission_policy,
            permission_request(),
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

struct PreparedRuntime<'subject> {
    loaded: LoadedAgentPackage<'subject>,
    registry: OfficialRustProgramRegistry,
    program: OfficialRustProgram<'subject>,
    permit: Option<RuntimeExecutionPermit<'subject>>,
}

impl<'subject> PreparedRuntime<'subject> {
    fn new(
        chain: &PreparedChain<'subject>,
        executor: &RuntimeExecutorAuthority,
    ) -> Result<Self, NodeDoctorAgentError> {
        let authorization_authority = ExecutionAuthorizationAuthority::new_operator_local();
        let authorization = authorization_authority
            .authorize(&chain.request())
            .map_err(|_| runtime_rejected())?;
        let evidence_authority = PackageLoadEvidenceAuthority::new_operator_local();
        let evidence = evidence_authority
            .integrate(&authorization_authority, &authorization, &chain.request())
            .map_err(|_| runtime_rejected())?;
        let loader = PackageLoaderAuthority::new_operator_local();
        let loaded = loader
            .load(
                &evidence_authority,
                &evidence,
                &authorization_authority,
                &authorization,
                &chain.request(),
            )
            .map_err(|_| runtime_rejected())?;
        let registry = OfficialRustProgramRegistry::new_operator_local();
        let program = register_node_doctor_program(&registry, chain.subject)
            .map_err(|_| runtime_rejected())?;
        let permit = executor
            .prepare(
                RuntimeExecutionPreparation::new(&loaded, &chain.request())
                    .with_loader(&loader, &evidence_authority, &evidence)
                    .with_authorization(&authorization_authority, &authorization)
                    .with_program(&registry, &program),
            )
            .map_err(|_| runtime_rejected())?;
        Ok(Self {
            loaded,
            registry,
            program,
            permit: Some(permit),
        })
    }

    fn request<'context>(
        &'context mut self,
        chain: &'context mut PreparedChain<'subject>,
        input: &'context iamine_agent_runtime::EnforcedInputRecord,
    ) -> Result<RuntimeExecutionRequest<'context, 'subject>, NodeDoctorAgentError> {
        Ok(RuntimeExecutionRequest::new(
            self.permit.take().ok_or_else(runtime_rejected)?,
            &chain.lifecycle_authority,
            &mut chain.lifecycle_record,
            input,
        )
        .with_program(&self.registry, &self.program)
        .with_sandbox(&chain.sandbox_authority, &chain.sandbox_evidence)
        .with_timeout_cancel(&chain.timeout_authority, &chain.timeout_control)
        .with_input_output(&chain.input_output_authority, &chain.input_output_evidence)
        .with_audit(&chain.audit_authority))
    }
}

fn enforced_input(
    chain: &PreparedChain<'_>,
) -> Result<iamine_agent_runtime::EnforcedInputRecord, NodeDoctorAgentError> {
    let input = chain
        .input_output_authority
        .attest_redacted_input(
            &chain.input_output_evidence,
            chain.subject,
            NODE_DOCTOR_TASK_INPUT,
        )
        .map_err(|_| runtime_rejected())?;
    chain
        .input_output_authority
        .enforce_input(
            &chain.input_output_evidence,
            chain.subject,
            InputClassification::TaskDescriptor,
            input,
        )
        .map_err(|_| runtime_rejected())
}

fn scope_request(classification: ScopeRequestClassification) -> ScopeRequestRef<'static> {
    ScopeRequestRef::new(
        NODE_DOCTOR_PACKAGE_ID,
        NODE_DOCTOR_TASK_TYPE,
        "explain_node_readiness",
        "read_declared_summary",
        &INPUT_CLASSES,
        classification,
    )
}

fn permission_request() -> PermissionRequestRef<'static> {
    PermissionRequestRef::new(
        NODE_DOCTOR_PACKAGE_ID,
        "read_declared_summary",
        &REQUIRED_CATEGORIES,
        PermissionConfirmation::NotProvided,
    )
}

fn routing_request() -> Result<RoutingSelectionRequestRef<'static>, NodeDoctorAgentError> {
    Ok(RoutingSelectionRequestRef::new(
        NODE_DOCTOR_TASK_TYPE,
        ResourceOperatingMode::LocalReadonly,
        RoutingResourceRequirements::new(1, 256, 20, RuntimeNetworkAvailability::None)
            .map_err(|_| runtime_rejected())?,
        RoutingCandidateRiskClass::Moderate,
    ))
}

fn routing_candidate<'subject>(
    subject: PackageReviewSubject<'subject>,
    scope: ScopeEvaluation,
    permission: PermissionEvaluation,
    compatibility: &'subject RuntimeCompatibilityEvidence<'subject>,
    sandbox: &'subject SandboxEnforcementEvidence<'subject>,
) -> RoutingCandidateRef<'subject> {
    RoutingCandidateRef::new(
        "node-doctor-local",
        NODE_DOCTOR_TASK_TYPE,
        RoutingCandidateRiskClass::Low,
        RoutingCandidateAvailability::Available,
        subject,
        scope,
        permission,
        RoutingCandidateCompatibility::Compatible(compatibility),
        RoutingCandidateSandbox::Prepared(sandbox),
    )
}

fn approved_review_decisions() -> PackageReviewDecisions {
    PackageReviewDecisions::new(
        LocalRegistryReviewDecision::RegistryReviewReady,
        LanguagePolicyReviewDecision::RustOfficialAllowed,
        DependencyPolicyReviewDecision::Allowed,
        HumanReviewDecision::IndependentApproved,
    )
}

const fn runtime_rejected() -> NodeDoctorAgentError {
    NodeDoctorAgentError::new(NodeDoctorAgentErrorCode::RuntimeRejected)
}
