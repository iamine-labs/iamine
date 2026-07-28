mod support;

use std::{collections::HashSet, error::Error};

use iamine_agent_runtime::{
    inspect_runtime_foundation, DeclaredAgentPackage, ExecutionLifecycleAuthority,
    ExecutionLifecycleRecord, ExecutionLifecycleState, ExecutionLifecycleTransitionEvidence,
    HandoffDispatchEvidence, HandoffEnforcementAuthority, HandoffReason, HandoffRequest,
    HandoffTarget, OutOfScopeBlockedAction, OutOfScopeResponseAuthority, OutOfScopeResponseClass,
    OutOfScopeResponseError, OutOfScopeResponseErrorCode, OutOfScopeResponseEvidenceStatus,
    OutOfScopeResponseReason, OutOfScopeResponseRequirement, OutOfScopeResponseSource,
    OutOfScopeSourceReason, RuntimeFoundationStatus, RuntimeOwner, RuntimeOwnerState,
    OUT_OF_SCOPE_RESPONSE_CLASSES, OUT_OF_SCOPE_RESPONSE_REASONS,
    OUT_OF_SCOPE_RESPONSE_SCHEMA_VERSION,
};
use iamine_agents::{
    assess_package_load_yaml, evaluate_permissions, evaluate_scope, parse_and_validate_yaml,
    PackageLoadStatus, PermissionConfirmation, PermissionDefaultPolicy, PermissionEvaluation,
    PermissionPolicy, PermissionPolicySpec, PermissionReasonCode, PermissionRequestRef,
    ScopeEvaluation, ScopePolicy, ScopePolicySpec, ScopeReasonCode, ScopeRequestClassification,
    ScopeRequestRef,
};

use support::sandbox_chain::{prepare_sandbox, PackageFixture, VALID_MANIFEST};

const PACKAGE_ID: &str = "iamine.beta.node-doctor";
const LOCAL_READONLY: &[&str] = &["local_readonly"];
const SAFE_INPUTS: &[&str] = &["user_provided_text"];

type TestResult<T = ()> = Result<T, Box<dyn Error>>;

#[test]
fn canonical_classes_reasons_and_fixed_summaries_are_exact() {
    assert_eq!(
        OUT_OF_SCOPE_RESPONSE_CLASSES.map(OutOfScopeResponseClass::as_str),
        ["refuse", "clarify", "handoff", "blocked"]
    );
    assert_eq!(
        OUT_OF_SCOPE_RESPONSE_REASONS.map(OutOfScopeResponseReason::as_str),
        [
            "scope_mismatch",
            "permission_missing",
            "input_unsafe",
            "input_ambiguous",
            "risk_too_high",
            "resource_unavailable",
            "sandbox_unavailable",
            "policy_conflict",
        ]
    );

    let summaries = OUT_OF_SCOPE_RESPONSE_REASONS
        .map(OutOfScopeResponseReason::operator_summary)
        .map(|summary| summary.as_str())
        .into_iter()
        .collect::<HashSet<_>>();
    assert_eq!(summaries.len(), OUT_OF_SCOPE_RESPONSE_REASONS.len());
    assert!(summaries.iter().all(|summary| !summary.is_empty()));
}

#[test]
fn scope_clarification_and_refusal_preserve_fail_closed_semantics() -> TestResult {
    let authority = OutOfScopeResponseAuthority::new_operator_local();
    let clarification =
        authority.respond_to_scope(&scope_evaluation(ScopeRequestClassification::Ambiguous)?)?;
    assert_eq!(clarification.source(), OutOfScopeResponseSource::Scope);
    assert_eq!(
        clarification.source_reason(),
        OutOfScopeSourceReason::Scope(ScopeReasonCode::AmbiguousTask)
    );
    assert_eq!(
        clarification.response_class(),
        OutOfScopeResponseClass::Clarify
    );
    assert_eq!(
        clarification.response_reason(),
        OutOfScopeResponseReason::InputAmbiguous
    );
    assert!(clarification.operator_input_required());
    assert_safe_response(&clarification);

    let refusal =
        authority.respond_to_scope(&scope_evaluation(ScopeRequestClassification::Dangerous)?)?;
    assert_eq!(
        refusal.source_reason(),
        OutOfScopeSourceReason::Scope(ScopeReasonCode::DangerousTask)
    );
    assert_eq!(refusal.response_class(), OutOfScopeResponseClass::Refuse);
    assert_eq!(
        refusal.response_reason(),
        OutOfScopeResponseReason::RiskTooHigh
    );
    assert!(!refusal.operator_input_required());
    assert_safe_response(&refusal);
    Ok(())
}

#[test]
fn allowed_scope_and_undispatched_scope_handoff_do_not_emit_responses() -> TestResult {
    let authority = OutOfScopeResponseAuthority::new_operator_local();
    assert_response_error(
        authority.respond_to_scope(&scope_evaluation(
            ScopeRequestClassification::InScopeCandidate,
        )?),
        OutOfScopeResponseErrorCode::ResponseNotRequired,
        OutOfScopeResponseRequirement::NonAllowDecision,
    )?;
    assert_response_error(
        authority.respond_to_scope(&scope_evaluation(ScopeRequestClassification::CrossDomain)?),
        OutOfScopeResponseErrorCode::HandoffDispatchRequired,
        OutOfScopeResponseRequirement::HandoffDispatchEvidence,
    )?;
    Ok(())
}

#[test]
fn permission_confirmation_is_blocked_without_becoming_authorization() -> TestResult {
    let authority = OutOfScopeResponseAuthority::new_operator_local();
    let evaluation = permission_evaluation(
        "summarize_status",
        &["redacted_status_summary"],
        PermissionConfirmation::NotProvided,
    )?;
    let response = authority.respond_to_permission(&evaluation)?;

    assert_eq!(response.source(), OutOfScopeResponseSource::Permission);
    assert_eq!(
        response.source_reason(),
        OutOfScopeSourceReason::Permission(PermissionReasonCode::ConfirmationRequired)
    );
    assert_eq!(response.response_class(), OutOfScopeResponseClass::Blocked);
    assert_eq!(
        response.response_reason(),
        OutOfScopeResponseReason::PermissionMissing
    );
    assert!(response.operator_input_required());
    assert_safe_response(&response);
    Ok(())
}

#[test]
fn permission_refusals_distinguish_missing_permission_from_policy_conflict() -> TestResult {
    let authority = OutOfScopeResponseAuthority::new_operator_local();
    let undeclared = permission_evaluation(
        "invented_action",
        LOCAL_READONLY,
        PermissionConfirmation::NotProvided,
    )?;
    let missing = authority.respond_to_permission(&undeclared)?;
    assert_eq!(
        missing.source_reason(),
        OutOfScopeSourceReason::Permission(PermissionReasonCode::UndeclaredAction)
    );
    assert_eq!(
        missing.response_reason(),
        OutOfScopeResponseReason::PermissionMissing
    );

    let forbidden = permission_evaluation(
        "inspect_status",
        &["credential_access"],
        PermissionConfirmation::TrustedOrchestratorConfirmed,
    )?;
    let conflict = authority.respond_to_permission(&forbidden)?;
    assert_eq!(
        conflict.source_reason(),
        OutOfScopeSourceReason::Permission(PermissionReasonCode::ForbiddenCategory)
    );
    assert_eq!(
        conflict.response_reason(),
        OutOfScopeResponseReason::PolicyConflict
    );

    for response in [&missing, &conflict] {
        assert_eq!(response.response_class(), OutOfScopeResponseClass::Refuse);
        assert_safe_response(response);
    }
    Ok(())
}

#[test]
fn allowed_permission_and_undispatched_permission_handoff_do_not_emit_responses() -> TestResult {
    let authority = OutOfScopeResponseAuthority::new_operator_local();
    let allowed = permission_evaluation(
        "inspect_status",
        LOCAL_READONLY,
        PermissionConfirmation::NotProvided,
    )?;
    assert_response_error(
        authority.respond_to_permission(&allowed),
        OutOfScopeResponseErrorCode::ResponseNotRequired,
        OutOfScopeResponseRequirement::NonAllowDecision,
    )?;

    let scope = scope_evaluation(ScopeRequestClassification::CrossDomain)?;
    let policy = permission_policy()?;
    let handoff = evaluate_permissions(
        &policy,
        &scope,
        PermissionRequestRef::new(
            PACKAGE_ID,
            "inspect_status",
            LOCAL_READONLY,
            PermissionConfirmation::NotProvided,
        ),
    );
    assert_response_error(
        authority.respond_to_permission(&handoff),
        OutOfScopeResponseErrorCode::HandoffDispatchRequired,
        OutOfScopeResponseRequirement::HandoffDispatchEvidence,
    )?;
    Ok(())
}

#[test]
fn recorded_handoff_is_required_before_a_handoff_response_is_claimed() -> TestResult {
    let dispatch = dispatched_handoff(HandoffTarget::Orchestrator, HandoffReason::OutOfScope)?;
    let authority = OutOfScopeResponseAuthority::new_operator_local();
    let response = authority.respond_to_handoff(&dispatch);

    assert_eq!(
        response.schema_version(),
        OUT_OF_SCOPE_RESPONSE_SCHEMA_VERSION
    );
    assert_eq!(
        response.status(),
        OutOfScopeResponseEvidenceStatus::Recorded
    );
    assert_eq!(response.source(), OutOfScopeResponseSource::Handoff);
    assert_eq!(
        response.source_reason(),
        OutOfScopeSourceReason::Handoff(HandoffReason::OutOfScope)
    );
    assert_eq!(response.response_class(), OutOfScopeResponseClass::Handoff);
    assert_eq!(
        response.response_reason(),
        OutOfScopeResponseReason::ScopeMismatch
    );
    assert_eq!(response.handoff_target(), Some(HandoffTarget::Orchestrator));
    assert!(response.handoff_dispatch_recorded());
    assert!(response.local_execution_cancelled());
    assert_safe_response(&response);
    Ok(())
}

#[test]
fn every_handoff_reason_has_one_bounded_response_reason() -> TestResult {
    let cases = [
        (
            HandoffReason::OutOfScope,
            OutOfScopeResponseReason::ScopeMismatch,
        ),
        (
            HandoffReason::PermissionMissing,
            OutOfScopeResponseReason::PermissionMissing,
        ),
        (
            HandoffReason::RiskTooHigh,
            OutOfScopeResponseReason::RiskTooHigh,
        ),
        (
            HandoffReason::InputAmbiguous,
            OutOfScopeResponseReason::InputAmbiguous,
        ),
        (
            HandoffReason::OutputRequiresReview,
            OutOfScopeResponseReason::PolicyConflict,
        ),
        (
            HandoffReason::SandboxUnavailable,
            OutOfScopeResponseReason::SandboxUnavailable,
        ),
        (
            HandoffReason::TimeoutOrCancelled,
            OutOfScopeResponseReason::ResourceUnavailable,
        ),
        (
            HandoffReason::PolicyConflict,
            OutOfScopeResponseReason::PolicyConflict,
        ),
    ];

    for (handoff_reason, expected_response_reason) in cases {
        let dispatch = dispatched_handoff(HandoffTarget::BlockedState, handoff_reason)?;
        let response =
            OutOfScopeResponseAuthority::new_operator_local().respond_to_handoff(&dispatch);
        assert_eq!(response.response_reason(), expected_response_reason);
        assert_eq!(response.handoff_target(), Some(HandoffTarget::BlockedState));
        assert_safe_response(&response);
    }
    Ok(())
}

#[test]
fn response_evidence_is_authority_bound_and_debug_output_is_private() -> TestResult {
    let fixture = PackageFixture::valid()?;
    let package_id = fixture.package_id().to_owned();
    let authority = OutOfScopeResponseAuthority::new_operator_local();
    let foreign = OutOfScopeResponseAuthority::new_operator_local();
    let response = authority.respond_to_scope(&scope_evaluation(
        ScopeRequestClassification::PromptInjection,
    )?)?;

    assert!(authority.verifies_response(&response));
    assert!(!foreign.verifies_response(&response));

    for output in [format!("{authority:?}"), format!("{response:?}")] {
        assert!(output.contains("[redacted]"));
        assert!(!output.contains(&package_id));
        assert!(!output.contains("user_provided_text"));
        assert!(!output.contains("private operator prompt"));
    }
    Ok(())
}

#[test]
fn package_load_and_runtime_foundation_remain_fail_closed() -> TestResult {
    let package_report = assess_package_load_yaml(VALID_MANIFEST)?;
    let manifest = parse_and_validate_yaml(VALID_MANIFEST)?;
    let runtime_report = inspect_runtime_foundation(DeclaredAgentPackage::from_manifest(&manifest));
    let response_owner = runtime_report
        .owner_statuses()
        .iter()
        .find(|status| status.owner() == RuntimeOwner::OutOfScopeResponseEnforcement)
        .ok_or("out-of-scope response owner is missing")?;

    assert_eq!(package_report.status(), PackageLoadStatus::Blocked);
    assert!(!package_report.load_allowed());
    assert_eq!(runtime_report.status(), RuntimeFoundationStatus::Blocked);
    assert!(!runtime_report.package_access_available());
    assert!(!runtime_report.execution_available());
    assert_eq!(response_owner.state(), RuntimeOwnerState::Unavailable);
    Ok(())
}

fn assert_safe_response(response: &iamine_agent_runtime::OutOfScopeResponseEvidence) {
    assert!(response.response_recorded());
    assert!(response.operator_visible());
    assert_eq!(
        response.blocked_action(),
        OutOfScopeBlockedAction::ContinueLocalExecution
    );
    assert!(!response.response_delivered());
    assert!(!response.task_success());
    assert!(!response.scope_expanded());
    assert!(!response.permissions_expanded());
    assert!(!response.execution_authorized());
    assert!(!response.runtime_active());
    assert!(!response.transport_performed());
    assert!(!response.persisted());
    assert!(!response.audit_emitted());
}

fn assert_response_error<T>(
    result: Result<T, OutOfScopeResponseError>,
    code: OutOfScopeResponseErrorCode,
    requirement: OutOfScopeResponseRequirement,
) -> TestResult {
    let error = result
        .err()
        .ok_or("out-of-scope response unexpectedly succeeded")?;
    assert_eq!(error.code(), code);
    assert_eq!(error.requirement(), requirement);
    assert!(!error.to_string().is_empty());
    Ok(())
}

fn dispatched_handoff(
    target: HandoffTarget,
    reason: HandoffReason,
) -> TestResult<HandoffDispatchEvidence> {
    let fixture = PackageFixture::valid()?;
    let references = fixture.resolve()?;
    let subject = fixture.subject(&references);
    let sandbox = prepare_sandbox(subject)?;
    let lifecycle = ExecutionLifecycleAuthority::new_operator_local();
    let mut record = lifecycle.queue(&sandbox.authority, &sandbox.evidence, subject)?;
    let handoff_transition = advance_to_handoff(&lifecycle, &mut record)?;
    let authority = HandoffEnforcementAuthority::new_operator_local();
    let control = authority.prepare(
        &lifecycle,
        &record,
        &handoff_transition,
        HandoffRequest::new(target, reason),
    )?;
    Ok(authority.dispatch(&control, &lifecycle, &mut record)?)
}

fn advance_to_handoff(
    lifecycle: &ExecutionLifecycleAuthority,
    record: &mut ExecutionLifecycleRecord<'_>,
) -> TestResult<ExecutionLifecycleTransitionEvidence> {
    let _ = lifecycle.transition(record, 0, ExecutionLifecycleState::PermissionPending)?;
    let _ = lifecycle.transition(record, 1, ExecutionLifecycleState::ScopeCheck)?;
    Ok(lifecycle.transition(record, 2, ExecutionLifecycleState::HandoffRequired)?)
}

fn scope_evaluation(classification: ScopeRequestClassification) -> TestResult<ScopeEvaluation> {
    let policy = ScopePolicy::try_from(scope_policy_spec())?;
    Ok(evaluate_scope(
        &policy,
        ScopeRequestRef::new(
            PACKAGE_ID,
            "diagnostic_report",
            "inspect_node_status",
            "inspect_status",
            SAFE_INPUTS,
            classification,
        ),
    ))
}

fn permission_evaluation(
    action: &str,
    categories: &[&str],
    confirmation: PermissionConfirmation,
) -> TestResult<PermissionEvaluation> {
    let policy = permission_policy()?;
    let scope = scope_evaluation(ScopeRequestClassification::InScopeCandidate)?;
    Ok(evaluate_permissions(
        &policy,
        &scope,
        PermissionRequestRef::new(PACKAGE_ID, action, categories, confirmation),
    ))
}

fn permission_policy() -> TestResult<PermissionPolicy> {
    Ok(PermissionPolicy::try_from(PermissionPolicySpec {
        package_id: PACKAGE_ID.to_string(),
        permission_profile_id: "node_doctor_local_readonly_permissions".to_string(),
        default_policy: PermissionDefaultPolicy::Deny,
        approved_categories: strings(&[
            "lan_readonly_metadata",
            "local_readonly",
            "redacted_status_summary",
            "user_provided_text",
        ]),
        forbidden_categories: strings(&[
            "arbitrary_shell",
            "credential_access",
            "destructive_write",
            "mainnet_operation",
            "marketplace_publish",
            "model_download",
            "model_load",
            "network_mutation",
            "private_key_access",
            "service_mutation",
            "unrestricted_filesystem",
            "vm_or_container_mutation",
            "wallet_access",
        ]),
        approved_actions: strings(&["inspect_status", "summarize_status"]),
        blocked_actions: strings(&[
            "access_private_keys",
            "access_wallet",
            "collect_credentials",
            "delete_files",
            "download_models",
            "load_models",
            "mainnet_operation",
            "mutate_network",
            "mutate_vm_or_container",
            "publish_agent",
            "read_private_files",
            "restart_services",
            "reward_operation",
            "run_shell",
            "settlement_operation",
            "token_operation",
            "write_files",
        ]),
        confirmation_required_categories: strings(&["lan_readonly_metadata"]),
        confirmation_required_actions: strings(&["summarize_status"]),
    })?)
}

fn scope_policy_spec() -> ScopePolicySpec {
    ScopePolicySpec {
        package_id: PACKAGE_ID.to_string(),
        scope_id: "node_status_diagnostic".to_string(),
        task_types: strings(&["diagnostic_report"]),
        in_scope_tasks: strings(&["inspect_node_status"]),
        out_of_scope_tasks: strings(&["mutate_node_status"]),
        allowed_input_classes: strings(SAFE_INPUTS),
        forbidden_input_classes: strings(&[
            "credentials",
            "disk_uuids",
            "full_hostnames",
            "home_directories",
            "ip_addresses",
            "mac_addresses",
            "machine_ids",
            "personal_paths",
            "private_keys",
            "private_paths",
            "raw_process_lists",
            "serial_numbers",
            "usernames",
            "wallet_keys",
        ]),
        allowed_operations: strings(&["inspect_status"]),
        blocked_actions: strings(&[
            "change_settings",
            "delete_files",
            "download_models",
            "load_models",
            "mutate_vm_or_container",
            "publish_agent",
            "restart_services",
            "run_shell",
            "scan_network",
            "write_files",
        ]),
    }
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}
