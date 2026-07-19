use std::error::Error;

use iamine_agents::{
    audit_lifecycle_state, audit_permission_evaluation, audit_scope_evaluation,
    evaluate_permissions, evaluate_scope, AuditEventClass, AuditEventSource, AuditLifecycleState,
    AuditOutcome, AuditReasonCode, PackageLoadBlockerCode, PermissionConfirmation,
    PermissionDefaultPolicy, PermissionPolicy, PermissionPolicySpec, PermissionRequestRef,
    ScopeEvaluation, ScopePolicy, ScopePolicySpec, ScopeRequestClassification, ScopeRequestRef,
    AUDIT_EVENT_SCHEMA_VERSION, MAX_AUDIT_EVENTS_PER_PROJECTION,
};

const PACKAGE_ID: &str = "iamine.beta.node-doctor";
const INPUTS: &[&str] = &["user_provided_text"];

type TestResult = Result<(), Box<dyn Error>>;

#[test]
fn schema_classes_sources_outcomes_and_states_are_stable() {
    assert_eq!(AUDIT_EVENT_SCHEMA_VERSION, "1.0.0");
    assert_eq!(MAX_AUDIT_EVENTS_PER_PROJECTION, 2);
    assert_eq!(
        AuditEventClass::LifecycleObserved.as_str(),
        "lifecycle_observed"
    );
    assert_eq!(AuditEventClass::ScopeChecked.as_str(), "scope_checked");
    assert_eq!(
        AuditEventClass::PermissionChecked.as_str(),
        "permission_checked"
    );
    assert_eq!(
        AuditEventClass::RefusalRecorded.as_str(),
        "refusal_recorded"
    );
    assert_eq!(
        AuditEventClass::HandoffRequired.as_str(),
        "handoff_required"
    );
    assert_eq!(AuditEventSource::Lifecycle.as_str(), "lifecycle");
    assert_eq!(AuditEventSource::Scope.as_str(), "scope");
    assert_eq!(AuditEventSource::Permission.as_str(), "permission");
    assert_eq!(AuditOutcome::Observed.as_str(), "observed");
    assert_eq!(AuditOutcome::Allowed.as_str(), "allowed");
    assert_eq!(AuditOutcome::Refused.as_str(), "refused");
    assert_eq!(AuditOutcome::HandedOff.as_str(), "handed_off");
    assert_eq!(
        AuditLifecycleState::PermissionPending.as_str(),
        "permission_pending"
    );
    assert_eq!(AuditLifecycleState::ScopeCheck.as_str(), "scope_check");
    assert_eq!(AuditLifecycleState::Cancelled.as_str(), "cancelled");
    assert_eq!(AuditLifecycleState::Timeout.as_str(), "timeout");
}

#[test]
fn every_lifecycle_state_emits_bounded_observation_evidence() {
    let states = [
        AuditLifecycleState::Queued,
        AuditLifecycleState::PermissionPending,
        AuditLifecycleState::ScopeCheck,
        AuditLifecycleState::HandoffRequired,
        AuditLifecycleState::Running,
        AuditLifecycleState::Completed,
        AuditLifecycleState::Failed,
        AuditLifecycleState::Cancelled,
        AuditLifecycleState::Timeout,
        AuditLifecycleState::Blocked,
    ];

    for state in states {
        let events = audit_lifecycle_state(state);
        assert!(!events.is_empty());
        assert!(events.len() <= MAX_AUDIT_EVENTS_PER_PROJECTION);
        assert_eq!(events.iter().count(), events.len());
        assert_eq!(events.primary().schema_version(), "1.0.0");
        assert_eq!(events.primary().class(), AuditEventClass::LifecycleObserved);
        assert_eq!(events.primary().source(), AuditEventSource::Lifecycle);
        assert_eq!(events.primary().outcome(), AuditOutcome::Observed);
        assert_eq!(events.primary().lifecycle_state(), Some(state));
        assert_eq!(
            events.primary().reason(),
            AuditReasonCode::LifecycleStateObserved
        );

        if state == AuditLifecycleState::HandoffRequired {
            let handoff = events
                .secondary()
                .expect("handoff state emits handoff evidence");
            assert_eq!(handoff.class(), AuditEventClass::HandoffRequired);
            assert_eq!(handoff.outcome(), AuditOutcome::HandedOff);
            assert_eq!(handoff.lifecycle_state(), Some(state));
        } else {
            assert!(events.secondary().is_none());
        }
    }
}

#[test]
fn scope_allow_and_clarification_emit_only_the_check() -> TestResult {
    let cases = [
        (
            ScopeRequestClassification::InScopeCandidate,
            AuditOutcome::Allowed,
            "in_scope",
        ),
        (
            ScopeRequestClassification::Ambiguous,
            AuditOutcome::ClarificationRequired,
            "ambiguous_task",
        ),
    ];

    for (classification, outcome, reason) in cases {
        let evaluation = scope_evaluation(classification)?;
        let events = audit_scope_evaluation(&evaluation);
        assert_eq!(events.len(), 1);
        assert_eq!(events.primary().class(), AuditEventClass::ScopeChecked);
        assert_eq!(events.primary().source(), AuditEventSource::Scope);
        assert_eq!(events.primary().outcome(), outcome);
        assert_eq!(events.primary().reason().as_str(), reason);
        assert_eq!(events.primary().lifecycle_state(), None);
    }
    Ok(())
}

#[test]
fn scope_refusal_emits_check_then_refusal() -> TestResult {
    let evaluation = scope_evaluation(ScopeRequestClassification::Dangerous)?;
    let events = audit_scope_evaluation(&evaluation);

    assert_eq!(events.len(), 2);
    assert_eq!(events.primary().class(), AuditEventClass::ScopeChecked);
    assert_eq!(events.primary().outcome(), AuditOutcome::Refused);
    let refusal = events
        .secondary()
        .expect("refusal evidence must be present");
    assert_eq!(refusal.class(), AuditEventClass::RefusalRecorded);
    assert_eq!(refusal.source(), AuditEventSource::Scope);
    assert_eq!(refusal.outcome(), AuditOutcome::Refused);
    assert_eq!(refusal.reason().as_str(), "dangerous_task");
    Ok(())
}

#[test]
fn scope_handoff_emits_check_then_handoff() -> TestResult {
    let evaluation = scope_evaluation(ScopeRequestClassification::CrossDomain)?;
    let events = audit_scope_evaluation(&evaluation);

    assert_eq!(events.len(), 2);
    assert_eq!(events.primary().outcome(), AuditOutcome::HandedOff);
    let handoff = events
        .secondary()
        .expect("handoff evidence must be present");
    assert_eq!(handoff.class(), AuditEventClass::HandoffRequired);
    assert_eq!(handoff.source(), AuditEventSource::Scope);
    assert_eq!(handoff.outcome(), AuditOutcome::HandedOff);
    assert_eq!(handoff.reason().as_str(), "cross_domain_task");
    Ok(())
}

#[test]
fn permission_allow_and_confirmation_emit_only_the_check() -> TestResult {
    let policy = permission_policy()?;
    let scope = scope_evaluation(ScopeRequestClassification::InScopeCandidate)?;
    let cases = [
        (
            PermissionRequestRef::new(
                PACKAGE_ID,
                "inspect_status",
                &["local_readonly"],
                PermissionConfirmation::NotProvided,
            ),
            AuditOutcome::Allowed,
            "permitted",
        ),
        (
            PermissionRequestRef::new(
                PACKAGE_ID,
                "summarize_status",
                &["redacted_status_summary"],
                PermissionConfirmation::NotProvided,
            ),
            AuditOutcome::ConfirmationRequired,
            "confirmation_required",
        ),
    ];

    for (request, outcome, reason) in cases {
        let evaluation = evaluate_permissions(&policy, &scope, request);
        let events = audit_permission_evaluation(&evaluation);
        assert_eq!(events.len(), 1);
        assert_eq!(events.primary().class(), AuditEventClass::PermissionChecked);
        assert_eq!(events.primary().source(), AuditEventSource::Permission);
        assert_eq!(events.primary().outcome(), outcome);
        assert_eq!(events.primary().reason().as_str(), reason);
    }
    Ok(())
}

#[test]
fn permission_refusal_emits_check_then_refusal() -> TestResult {
    let policy = permission_policy()?;
    let scope = scope_evaluation(ScopeRequestClassification::InScopeCandidate)?;
    let evaluation = evaluate_permissions(
        &policy,
        &scope,
        PermissionRequestRef::new(
            PACKAGE_ID,
            "run_shell",
            &["local_readonly"],
            PermissionConfirmation::TrustedOrchestratorConfirmed,
        ),
    );
    let events = audit_permission_evaluation(&evaluation);

    assert_eq!(events.len(), 2);
    assert_eq!(events.primary().outcome(), AuditOutcome::Refused);
    let refusal = events
        .secondary()
        .expect("refusal evidence must be present");
    assert_eq!(refusal.class(), AuditEventClass::RefusalRecorded);
    assert_eq!(refusal.source(), AuditEventSource::Permission);
    assert_eq!(refusal.reason().as_str(), "blocked_action");
    Ok(())
}

#[test]
fn permission_handoff_preserves_failed_scope_boundary() -> TestResult {
    let policy = permission_policy()?;
    let scope = scope_evaluation(ScopeRequestClassification::PromptInjection)?;
    let evaluation = evaluate_permissions(
        &policy,
        &scope,
        PermissionRequestRef::new(
            PACKAGE_ID,
            "inspect_status",
            &["local_readonly"],
            PermissionConfirmation::TrustedOrchestratorConfirmed,
        ),
    );
    let events = audit_permission_evaluation(&evaluation);

    assert_eq!(events.len(), 2);
    assert_eq!(events.primary().outcome(), AuditOutcome::HandedOff);
    let handoff = events
        .secondary()
        .expect("handoff evidence must be present");
    assert_eq!(handoff.class(), AuditEventClass::HandoffRequired);
    assert_eq!(handoff.source(), AuditEventSource::Permission);
    assert_eq!(handoff.reason().as_str(), "scope_gate_not_passed");
    Ok(())
}

#[test]
fn event_projection_is_deterministic_and_debug_output_is_redacted() -> TestResult {
    let policy = scope_policy()?;
    let request = ScopeRequestRef::new(
        "iamine.private.do-not-log-this-package",
        "diagnostic_report",
        "do_not_log_this_task",
        "do_not_log_this_operation",
        &["do_not_log_this_input"],
        ScopeRequestClassification::InScopeCandidate,
    );
    let evaluation = evaluate_scope(&policy, request);
    let first = audit_scope_evaluation(&evaluation);
    let second = audit_scope_evaluation(&evaluation);
    let debug = format!("{first:?}");

    assert_eq!(first, second);
    for sensitive in [
        "do-not-log-this-package",
        "do_not_log_this_task",
        "do_not_log_this_operation",
        "do_not_log_this_input",
    ] {
        assert!(!debug.contains(sensitive));
    }
    Ok(())
}

#[test]
fn package_load_audit_blockers_remain_explicit() {
    assert_eq!(
        PackageLoadBlockerCode::AuditPolicyValidatorUnavailable.as_str(),
        "audit_policy_validator_unavailable"
    );
    assert_eq!(
        PackageLoadBlockerCode::AuditEventEnforcementUnavailable.as_str(),
        "audit_event_enforcement_unavailable"
    );
}

fn scope_evaluation(
    classification: ScopeRequestClassification,
) -> Result<ScopeEvaluation, Box<dyn Error>> {
    let policy = scope_policy()?;
    Ok(evaluate_scope(
        &policy,
        ScopeRequestRef::new(
            PACKAGE_ID,
            "diagnostic_report",
            "inspect_node_status",
            "inspect_status",
            INPUTS,
            classification,
        ),
    ))
}

fn scope_policy() -> Result<ScopePolicy, Box<dyn Error>> {
    Ok(ScopePolicy::try_from(ScopePolicySpec {
        package_id: PACKAGE_ID.to_string(),
        scope_id: "node_status_diagnostic".to_string(),
        task_types: strings(&["diagnostic_report"]),
        in_scope_tasks: strings(&["inspect_node_status"]),
        out_of_scope_tasks: strings(&["mutate_node_status"]),
        allowed_input_classes: strings(INPUTS),
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
    })?)
}

fn permission_policy() -> Result<PermissionPolicy, Box<dyn Error>> {
    Ok(PermissionPolicy::try_from(PermissionPolicySpec {
        package_id: PACKAGE_ID.to_string(),
        permission_profile_id: "node_doctor_local_readonly_permissions".to_string(),
        default_policy: PermissionDefaultPolicy::Deny,
        approved_categories: strings(&["local_readonly", "redacted_status_summary"]),
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
        confirmation_required_categories: Vec::new(),
        confirmation_required_actions: strings(&["summarize_status"]),
    })?)
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| (*value).to_string()).collect()
}
