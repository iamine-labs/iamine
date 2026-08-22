use iamine_agent_runtime::{
    AuditEventEnforcementAuthority, AuditEventEnforcementEvidence, DependencyPolicyReviewDecision,
    ExecutionAuthorizationAuthority, ExecutionAuthorizationRequest, ExecutionLifecycleAuthority,
    ExecutionLifecycleRecord, ExecutionLifecycleState, HumanReviewDecision, InputClassification,
    InputOutputEnforcementAuthority, InputOutputEnforcementEvidence, InputOutputPolicy,
    LanguagePolicyReviewDecision, LoadedAgentPackage, LocalRegistryReviewDecision,
    OfficialRustProgram, OfficialRustProgramFailure, OfficialRustProgramRegistry,
    OutputClassification, PackageLoadEvidenceAuthority, PackageLoaderAuthority,
    PackageReviewAuthority, PackageReviewDecisions, PackageReviewEvidence, PackageReviewSubject,
    RoutingCandidateAvailability, RoutingCandidateCompatibility, RoutingCandidateRef,
    RoutingCandidateRiskClass, RoutingCandidateSandbox, RoutingCandidateSelectionAuthority,
    RoutingCandidateSelectionEvidence, RoutingResourceRequirements, RoutingSelectionRequestRef,
    RuntimeCompatibilityAuthority, RuntimeCompatibilityEvidence, RuntimeExecutionPermit,
    RuntimeExecutionPreparation, RuntimeExecutionRequest, RuntimeExecutionVerification,
    RuntimeExecutorAuthority, RuntimeLanguageAvailability, RuntimeLanguageDecision,
    RuntimeLanguageMode, RuntimeNetworkAvailability, RuntimeResourceEnvelope,
    SandboxEnforcementAuthority, SandboxEnforcementEvidence, SandboxEnforcementPolicy,
    TimeoutCancelAuthority, TimeoutCancelControl, TimeoutCancelPolicy,
};
use iamine_agents::{
    evaluate_permissions, evaluate_scope, PermissionConfirmation, PermissionEvaluation,
    PermissionPolicy, PermissionRequestRef, ResourceOperatingMode, ScopeEvaluation, ScopePolicy,
    ScopeRequestClassification, ScopeRequestRef,
};

pub(crate) type OfficialAgentProgramRegistrar =
    for<'subject> fn(
        &OfficialRustProgramRegistry,
        PackageReviewSubject<'subject>,
    ) -> Result<OfficialRustProgram<'subject>, OfficialRustProgramFailure>;

pub(crate) struct OfficialAgentExecutionSpec {
    pub(crate) package_id: &'static str,
    pub(crate) task_type: &'static str,
    pub(crate) scope_id: &'static str,
    pub(crate) task_name: &'static str,
    pub(crate) operation: &'static str,
    pub(crate) input_classes: &'static [&'static str],
    pub(crate) required_categories: &'static [&'static str],
    pub(crate) routing_candidate_id: &'static str,
    pub(crate) input_classification: InputClassification,
    pub(crate) max_input_bytes: usize,
    pub(crate) register_program: OfficialAgentProgramRegistrar,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OfficialAgentExecutionResult {
    pub(crate) classification: OutputClassification,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OfficialAgentExecutionError {
    RuntimeRejected,
    OutputVerificationFailed,
}

pub(crate) fn execute_official_local_readonly_agent(
    subject: PackageReviewSubject<'_>,
    scope_policy: ScopePolicy,
    permission_policy: PermissionPolicy,
    input: &str,
    spec: &'static OfficialAgentExecutionSpec,
) -> Result<OfficialAgentExecutionResult, OfficialAgentExecutionError> {
    let mut chain = PreparedChain::new(subject, scope_policy, permission_policy, spec)?;
    let input = enforced_input(&chain, input, spec)?;
    let executor = RuntimeExecutorAuthority::new_operator_local();
    let mut runtime = PreparedRuntime::new(&chain, &executor, spec)?;
    let result = executor
        .execute(runtime.request(&mut chain, &input)?)
        .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?;
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
    if !verified || result.output().scope_id() != spec.scope_id {
        return Err(OfficialAgentExecutionError::OutputVerificationFailed);
    }

    Ok(OfficialAgentExecutionResult {
        classification: result.output().classification(),
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
        spec: &'static OfficialAgentExecutionSpec,
    ) -> Result<Self, OfficialAgentExecutionError> {
        let review_authority = PackageReviewAuthority::new_operator_local();
        let review_evidence = review_authority
            .issue(subject, approved_review_decisions())
            .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?;
        let compatibility_authority = RuntimeCompatibilityAuthority::new_operator_local(
            RuntimeLanguageDecision::new(
                RuntimeLanguageMode::RustNativeOfficial,
                RuntimeLanguageAvailability::Available,
            ),
            RuntimeResourceEnvelope::new(2, 512, 84, RuntimeNetworkAvailability::None)
                .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?,
        );
        let compatibility_evidence = compatibility_authority
            .evaluate(&review_authority, &review_evidence, subject)
            .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?;
        let input_output_authority = InputOutputEnforcementAuthority::new_operator_local(
            InputOutputPolicy::new(spec.max_input_bytes, 4_096, true)
                .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?,
        );
        let input_output_evidence = input_output_authority
            .establish(&compatibility_authority, &compatibility_evidence, subject)
            .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?;
        let sandbox_authority = SandboxEnforcementAuthority::new_operator_local(
            SandboxEnforcementPolicy::new(30_000, 128)
                .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?,
        )
        .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?;
        let sandbox_evidence = sandbox_authority
            .establish(
                &compatibility_authority,
                &compatibility_evidence,
                &input_output_authority,
                &input_output_evidence,
                subject,
            )
            .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?;
        let lifecycle_authority = ExecutionLifecycleAuthority::new_operator_local();
        let mut lifecycle_record = lifecycle_authority
            .queue(&sandbox_authority, &sandbox_evidence, subject)
            .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?;
        let _ = lifecycle_authority
            .transition(
                &mut lifecycle_record,
                0,
                ExecutionLifecycleState::PermissionPending,
            )
            .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?;
        let _ = lifecycle_authority
            .transition(
                &mut lifecycle_record,
                1,
                ExecutionLifecycleState::ScopeCheck,
            )
            .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?;
        let timeout_authority = TimeoutCancelAuthority::new_operator_local();
        let timeout_control = timeout_authority
            .establish(
                &lifecycle_authority,
                &lifecycle_record,
                &sandbox_authority,
                &sandbox_evidence,
                subject,
                TimeoutCancelPolicy::new(1_000, 1_000, 1_000, 1_000, 1_000, 1_000)
                    .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?,
            )
            .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?;
        let scope = evaluate_scope(
            &scope_policy,
            scope_request(spec, ScopeRequestClassification::InScopeCandidate),
        );
        let permission = evaluate_permissions(&permission_policy, &scope, permission_request(spec));
        let routing_authority = RoutingCandidateSelectionAuthority::new_operator_local();
        let routing_evidence = routing_authority
            .select(
                routing_request(spec)?,
                &[routing_candidate(
                    subject,
                    scope,
                    permission,
                    &compatibility_evidence,
                    &sandbox_evidence,
                    spec,
                )],
                &compatibility_authority,
                &sandbox_authority,
            )
            .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?;
        let audit_authority = AuditEventEnforcementAuthority::new_operator_local();
        let scope_audit = audit_authority.enforce_scope(&scope);
        let permission_audit = audit_authority.enforce_permission(&permission);
        let lifecycle_audit = audit_authority
            .enforce_lifecycle(&lifecycle_authority, &lifecycle_record)
            .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?;

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

    fn request(
        &self,
        spec: &'static OfficialAgentExecutionSpec,
    ) -> ExecutionAuthorizationRequest<'_, 'subject> {
        ExecutionAuthorizationRequest::new(
            self.subject,
            &self.scope_policy,
            scope_request(spec, ScopeRequestClassification::InScopeCandidate),
            &self.permission_policy,
            permission_request(spec),
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
        spec: &'static OfficialAgentExecutionSpec,
    ) -> Result<Self, OfficialAgentExecutionError> {
        let authorization_authority = ExecutionAuthorizationAuthority::new_operator_local();
        let authorization = authorization_authority
            .authorize(&chain.request(spec))
            .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?;
        let evidence_authority = PackageLoadEvidenceAuthority::new_operator_local();
        let evidence = evidence_authority
            .integrate(
                &authorization_authority,
                &authorization,
                &chain.request(spec),
            )
            .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?;
        let loader = PackageLoaderAuthority::new_operator_local();
        let loaded = loader
            .load(
                &evidence_authority,
                &evidence,
                &authorization_authority,
                &authorization,
                &chain.request(spec),
            )
            .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?;
        let registry = OfficialRustProgramRegistry::new_operator_local();
        let program = (spec.register_program)(&registry, chain.subject)
            .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?;
        let permit = executor
            .prepare(
                RuntimeExecutionPreparation::new(&loaded, &chain.request(spec))
                    .with_loader(&loader, &evidence_authority, &evidence)
                    .with_authorization(&authorization_authority, &authorization)
                    .with_program(&registry, &program),
            )
            .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?;
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
    ) -> Result<RuntimeExecutionRequest<'context, 'subject>, OfficialAgentExecutionError> {
        Ok(RuntimeExecutionRequest::new(
            self.permit
                .take()
                .ok_or(OfficialAgentExecutionError::RuntimeRejected)?,
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
    input: &str,
    spec: &'static OfficialAgentExecutionSpec,
) -> Result<iamine_agent_runtime::EnforcedInputRecord, OfficialAgentExecutionError> {
    let input = chain
        .input_output_authority
        .attest_redacted_input(&chain.input_output_evidence, chain.subject, input)
        .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?;
    chain
        .input_output_authority
        .enforce_input(
            &chain.input_output_evidence,
            chain.subject,
            spec.input_classification,
            input,
        )
        .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)
}

fn scope_request(
    spec: &'static OfficialAgentExecutionSpec,
    classification: ScopeRequestClassification,
) -> ScopeRequestRef<'static> {
    ScopeRequestRef::new(
        spec.package_id,
        spec.task_type,
        spec.task_name,
        spec.operation,
        spec.input_classes,
        classification,
    )
}

fn permission_request(spec: &'static OfficialAgentExecutionSpec) -> PermissionRequestRef<'static> {
    PermissionRequestRef::new(
        spec.package_id,
        spec.operation,
        spec.required_categories,
        PermissionConfirmation::NotProvided,
    )
}

fn routing_request(
    spec: &'static OfficialAgentExecutionSpec,
) -> Result<RoutingSelectionRequestRef<'static>, OfficialAgentExecutionError> {
    Ok(RoutingSelectionRequestRef::new(
        spec.task_type,
        ResourceOperatingMode::LocalReadonly,
        RoutingResourceRequirements::new(1, 256, 20, RuntimeNetworkAvailability::None)
            .map_err(|_| OfficialAgentExecutionError::RuntimeRejected)?,
        RoutingCandidateRiskClass::Moderate,
    ))
}

fn routing_candidate<'subject>(
    subject: PackageReviewSubject<'subject>,
    scope: ScopeEvaluation,
    permission: PermissionEvaluation,
    compatibility: &'subject RuntimeCompatibilityEvidence<'subject>,
    sandbox: &'subject SandboxEnforcementEvidence<'subject>,
    spec: &'static OfficialAgentExecutionSpec,
) -> RoutingCandidateRef<'subject> {
    RoutingCandidateRef::new(
        spec.routing_candidate_id,
        spec.task_type,
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
