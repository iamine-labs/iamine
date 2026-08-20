use std::io;

use iamine_core::{
    InterfaceOperation, InterfaceOperationId, InterfaceRequest, LocalAuthorizationAuditKind,
    LocalAuthorizationAuthority, LocalAuthorizationDenialCode, LocalAuthorizationError,
    LocalAuthorizationIntent, LocalAuthorizationPolicy, LocalControlAuditRequirement,
    LocalControlAuthorizationHandoff, LocalControlAuthorizationRequirement, LocalControlClient,
    LocalControlIngress, LocalControlMediaType, LocalControlMethod, LocalControlOrigin,
    LocalControlPeer, LocalControlReplayRequirement, LocalControlRequest, LocalControlRequestId,
    LocalControlRoute, LocalControlTransport, LocalControlValidatedRequest, LocalSessionClient,
    LOCAL_AUTHORIZATION_SCHEMA_VERSION, MAX_LOCAL_AUTHORIZATION_REPLAY_RECORDS,
    MAX_LOCAL_AUTHORIZATION_SESSIONS,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const REQUEST_A: &str = "45c3a273-f010-4d63-99cb-6fd29c553c48";
const REQUEST_B: &str = "9e4a684c-c833-46b5-a2ae-d29159d76e7a";
const REQUEST_C: &str = "acdb65be-fbde-43d0-9298-7ae9ee59e85a";

fn policy(
    session_ttl_ticks: u64,
    evidence_ttl_ticks: u64,
    max_sessions: usize,
    max_replay_records: usize,
) -> Result<LocalAuthorizationPolicy, LocalAuthorizationError> {
    LocalAuthorizationPolicy::try_new(
        session_ttl_ticks,
        evidence_ttl_ticks,
        max_sessions,
        max_replay_records,
    )
}

fn request(
    request_id: &str,
    operation_id: InterfaceOperationId,
) -> Result<LocalControlValidatedRequest, Box<dyn std::error::Error>> {
    let request = LocalControlRequest::with_request_id(
        LocalControlRequestId::try_from(request_id.to_string())?,
        InterfaceRequest::new(InterfaceOperation::new(operation_id), ()),
    );
    Ok(LocalControlIngress {
        transport: LocalControlTransport::LoopbackHttp,
        peer: LocalControlPeer::Ipv4Loopback,
        client: LocalControlClient::BrowserDashboard,
        origin: LocalControlOrigin::SameOrigin,
        method: LocalControlMethod::Post,
        route: LocalControlRoute::Operations,
        media_type: LocalControlMediaType::ApplicationJson,
        encoded_body_bytes: 256,
    }
    .validate(&request)?)
}

fn denial_code(
    decision: &iamine_core::LocalAuthorizationDecision,
) -> Option<LocalAuthorizationDenialCode> {
    decision.denial().map(|denial| denial.code())
}

#[test]
fn policy_rejects_zero_unbounded_and_longer_evidence_lifetimes() {
    assert_eq!(
        policy(0, 1, 1, 1),
        Err(LocalAuthorizationError::InvalidPolicy)
    );
    assert_eq!(
        policy(10, 11, 1, 1),
        Err(LocalAuthorizationError::InvalidPolicy)
    );
    assert_eq!(
        policy(10, 1, MAX_LOCAL_AUTHORIZATION_SESSIONS + 1, 1),
        Err(LocalAuthorizationError::InvalidPolicy)
    );
    assert_eq!(
        policy(10, 1, 1, MAX_LOCAL_AUTHORIZATION_REPLAY_RECORDS + 1),
        Err(LocalAuthorizationError::InvalidPolicy)
    );
}

#[test]
fn session_issuance_is_authority_bound_bounded_and_audited() -> TestResult {
    let configured = policy(100, 10, 1, 8)?;
    let (authority, issuer) = LocalAuthorizationAuthority::new_operator_local(configured);
    let (foreign, foreign_issuer) = LocalAuthorizationAuthority::new_operator_local(configured);

    assert_eq!(
        authority
            .issue_session(&foreign_issuer, LocalSessionClient::BrowserDashboard, 10)
            .err(),
        Some(LocalAuthorizationError::IssuerMismatch)
    );

    let issuance = authority.issue_session(&issuer, LocalSessionClient::BrowserDashboard, 10)?;
    assert_eq!(
        issuance.session().schema_version(),
        LOCAL_AUTHORIZATION_SCHEMA_VERSION
    );
    assert_eq!(issuance.session().issued_at_tick(), 10);
    assert_eq!(issuance.session().expires_at_tick(), 110);
    assert!(!issuance.session().persisted());
    assert!(!issuance.session().serializable());
    assert!(!issuance.session().authorizes_action());
    assert_eq!(
        issuance.audit().kind(),
        LocalAuthorizationAuditKind::SessionIssued
    );
    assert!(!issuance.audit().persisted());
    assert!(!issuance.audit().emitted());

    assert_eq!(
        authority
            .issue_session(&issuer, LocalSessionClient::LocalNative, 11)
            .err(),
        Some(LocalAuthorizationError::SessionCapacityExceeded)
    );
    assert!(foreign
        .issue_session(&foreign_issuer, LocalSessionClient::LocalNative, 11)
        .is_ok());
    Ok(())
}

#[test]
fn read_only_request_uses_session_and_still_requires_owner_dispatch() -> TestResult {
    let (authority, issuer) =
        LocalAuthorizationAuthority::new_operator_local(policy(100, 10, 2, 8)?);
    let issuance = authority.issue_session(&issuer, LocalSessionClient::BrowserDashboard, 10)?;
    let read = request(REQUEST_A, InterfaceOperationId::NodeEvidenceRead)?;
    let decision = authority.decide(
        issuance.session(),
        &read,
        LocalAuthorizationIntent::Proceed,
        11,
    )?;

    assert!(decision.is_approved());
    assert_eq!(
        decision.audit().kind(),
        LocalAuthorizationAuditKind::RequestApproved
    );
    assert_eq!(decision.audit().request_id(), Some(REQUEST_A));
    let evidence = decision
        .evidence()
        .ok_or_else(|| io::Error::other("read decision must contain evidence"))?;
    assert_eq!(
        evidence.requirement(),
        LocalControlAuthorizationRequirement::ReadOnlySession
    );
    assert_eq!(
        evidence.replay_requirement(),
        LocalControlReplayRequirement::NotRequired
    );
    assert_eq!(
        evidence.audit_requirement(),
        LocalControlAuditRequirement::RequestDecision
    );
    assert!(!evidence.authorizes_owner_action());

    let consumed = authority.consume(issuance.session(), &read, decision, 12)?;
    assert!(consumed.local_gate_satisfied());
    assert!(!consumed.authorizes_owner_action());
    assert!(!consumed.agent_runtime_authorization_required());
    assert_eq!(
        consumed.consumption_audit().kind(),
        LocalAuthorizationAuditKind::EvidenceConsumed
    );
    assert_eq!(
        consumed.authorization_audit().kind(),
        LocalAuthorizationAuditKind::RequestApproved
    );
    Ok(())
}

#[test]
fn mutation_requires_confirmation_and_rejects_request_replay() -> TestResult {
    let (authority, issuer) =
        LocalAuthorizationAuthority::new_operator_local(policy(100, 10, 2, 8)?);
    let issuance = authority.issue_session(&issuer, LocalSessionClient::BrowserDashboard, 10)?;
    let first = request(REQUEST_A, InterfaceOperationId::HardwareRefreshPlan)?;
    let missing_confirmation = authority.decide(
        issuance.session(),
        &first,
        LocalAuthorizationIntent::Proceed,
        11,
    )?;
    assert_eq!(
        denial_code(&missing_confirmation),
        Some(LocalAuthorizationDenialCode::ConfirmationRequired)
    );
    assert_eq!(
        missing_confirmation
            .denial()
            .map(|denial| denial.interface_problem().code),
        Some(iamine_core::InterfaceProblemCode::PermissionRequired)
    );

    let replay = authority.decide(
        issuance.session(),
        &first,
        LocalAuthorizationIntent::Confirm,
        12,
    )?;
    assert_eq!(
        denial_code(&replay),
        Some(LocalAuthorizationDenialCode::ReplayDetected)
    );

    let second = request(REQUEST_B, InterfaceOperationId::HardwareRefreshPlan)?;
    let approved = authority.decide(
        issuance.session(),
        &second,
        LocalAuthorizationIntent::Confirm,
        13,
    )?;
    let evidence = approved
        .evidence()
        .ok_or_else(|| io::Error::other("confirmed mutation must contain evidence"))?;
    assert_eq!(
        evidence.replay_requirement(),
        LocalControlReplayRequirement::SingleUseAuthorizationEvidence
    );
    let consumed = authority.consume(issuance.session(), &second, approved, 14)?;
    assert!(consumed.local_gate_satisfied());
    assert!(!consumed.authorizes_owner_action());

    let replay_after_consumption = authority.decide(
        issuance.session(),
        &second,
        LocalAuthorizationIntent::Confirm,
        15,
    )?;
    assert_eq!(
        denial_code(&replay_after_consumption),
        Some(LocalAuthorizationDenialCode::ReplayDetected)
    );
    Ok(())
}

#[test]
fn explicit_denial_is_fail_closed_audited_and_non_retryable_by_request_id() -> TestResult {
    let (authority, issuer) =
        LocalAuthorizationAuthority::new_operator_local(policy(100, 10, 2, 8)?);
    let issuance = authority.issue_session(&issuer, LocalSessionClient::BrowserDashboard, 10)?;
    let mutation = request(REQUEST_A, InterfaceOperationId::WorkerLifecycle)?;
    let denied = authority.decide(
        issuance.session(),
        &mutation,
        LocalAuthorizationIntent::Deny,
        11,
    )?;

    assert_eq!(
        denial_code(&denied),
        Some(LocalAuthorizationDenialCode::ExplicitlyDenied)
    );
    assert_eq!(
        denied.audit().kind(),
        LocalAuthorizationAuditKind::RequestDenied
    );
    assert_eq!(
        denied.audit().denial_code(),
        Some(LocalAuthorizationDenialCode::ExplicitlyDenied)
    );
    assert!(!denied.audit().authorizes_action());

    let replay = authority.decide(
        issuance.session(),
        &mutation,
        LocalAuthorizationIntent::Confirm,
        12,
    )?;
    assert_eq!(
        denial_code(&replay),
        Some(LocalAuthorizationDenialCode::ReplayDetected)
    );
    Ok(())
}

#[test]
fn expired_revoked_and_foreign_sessions_are_denied() -> TestResult {
    let configured = policy(10, 5, 2, 8)?;
    let (authority, issuer) = LocalAuthorizationAuthority::new_operator_local(configured);
    let (foreign, foreign_issuer) = LocalAuthorizationAuthority::new_operator_local(configured);
    let issuance = authority.issue_session(&issuer, LocalSessionClient::BrowserDashboard, 10)?;
    let foreign_issuance =
        foreign.issue_session(&foreign_issuer, LocalSessionClient::BrowserDashboard, 10)?;
    let read = request(REQUEST_A, InterfaceOperationId::NodeEvidenceRead)?;

    let foreign_session = authority.decide(
        foreign_issuance.session(),
        &read,
        LocalAuthorizationIntent::Proceed,
        11,
    )?;
    assert_eq!(
        denial_code(&foreign_session),
        Some(LocalAuthorizationDenialCode::SessionAuthorityMismatch)
    );

    let revocation = authority.revoke_session(&issuer, issuance.session(), 12)?;
    assert_eq!(
        revocation.kind(),
        LocalAuthorizationAuditKind::SessionRevoked
    );
    let revoked = authority.decide(
        issuance.session(),
        &read,
        LocalAuthorizationIntent::Proceed,
        13,
    )?;
    assert_eq!(
        denial_code(&revoked),
        Some(LocalAuthorizationDenialCode::SessionRevoked)
    );

    let fresh = authority.issue_session(&issuer, LocalSessionClient::BrowserDashboard, 20)?;
    let expired = authority.decide(
        fresh.session(),
        &read,
        LocalAuthorizationIntent::Proceed,
        30,
    )?;
    assert_eq!(
        denial_code(&expired),
        Some(LocalAuthorizationDenialCode::SessionExpired)
    );
    Ok(())
}

#[test]
fn clock_regression_and_lifetime_overflow_fail_closed() -> TestResult {
    let configured = policy(100, 10, 2, 8)?;
    let (authority, issuer) = LocalAuthorizationAuthority::new_operator_local(configured);
    let issuance = authority.issue_session(&issuer, LocalSessionClient::BrowserDashboard, 10)?;
    let read = request(REQUEST_A, InterfaceOperationId::NodeEvidenceRead)?;
    assert_eq!(
        authority
            .decide(
                issuance.session(),
                &read,
                LocalAuthorizationIntent::Proceed,
                9
            )
            .err(),
        Some(LocalAuthorizationError::ClockRegressed)
    );

    let (overflowing, overflow_issuer) =
        LocalAuthorizationAuthority::new_operator_local(configured);
    assert_eq!(
        overflowing
            .issue_session(
                &overflow_issuer,
                LocalSessionClient::BrowserDashboard,
                u64::MAX
            )
            .err(),
        Some(LocalAuthorizationError::LifetimeOverflow)
    );
    Ok(())
}

#[test]
fn evidence_is_bound_to_session_request_operation_and_expiry() -> TestResult {
    let configured = policy(100, 5, 2, 8)?;
    let (authority, issuer) = LocalAuthorizationAuthority::new_operator_local(configured);
    let first_session =
        authority.issue_session(&issuer, LocalSessionClient::BrowserDashboard, 10)?;
    let second_session =
        authority.issue_session(&issuer, LocalSessionClient::BrowserDashboard, 10)?;
    let first = request(REQUEST_A, InterfaceOperationId::NodeEvidenceRead)?;
    let other = request(REQUEST_B, InterfaceOperationId::ClusterStatusRead)?;

    let decision = authority.decide(
        first_session.session(),
        &first,
        LocalAuthorizationIntent::Proceed,
        11,
    )?;
    assert_eq!(
        authority
            .consume(second_session.session(), &other, decision, 12)
            .err(),
        Some(LocalAuthorizationError::EvidenceMismatch)
    );

    let expiring = authority.decide(
        first_session.session(),
        &request(REQUEST_C, InterfaceOperationId::TaskStatsRead)?,
        LocalAuthorizationIntent::Proceed,
        13,
    )?;
    assert_eq!(
        authority
            .consume(
                first_session.session(),
                &request(REQUEST_C, InterfaceOperationId::TaskStatsRead)?,
                expiring,
                18
            )
            .err(),
        Some(LocalAuthorizationError::EvidenceExpired)
    );
    Ok(())
}

#[test]
fn contradictory_transport_handoff_is_denied_before_authorization() -> TestResult {
    let (authority, issuer) =
        LocalAuthorizationAuthority::new_operator_local(policy(100, 10, 2, 8)?);
    let issuance = authority.issue_session(&issuer, LocalSessionClient::BrowserDashboard, 10)?;
    let mut read = request(REQUEST_A, InterfaceOperationId::NodeEvidenceRead)?;
    read.authorization = LocalControlAuthorizationHandoff {
        operation: InterfaceOperation::new(InterfaceOperationId::NodeEvidenceRead),
        requirement: LocalControlAuthorizationRequirement::RuntimeMutation,
        replay: LocalControlReplayRequirement::SingleUseAuthorizationEvidence,
        audit: LocalControlAuditRequirement::RequestDecisionAndAuthorization,
    };
    let decision = authority.decide(
        issuance.session(),
        &read,
        LocalAuthorizationIntent::Confirm,
        11,
    )?;
    assert_eq!(
        denial_code(&decision),
        Some(LocalAuthorizationDenialCode::RequestContractMismatch)
    );
    Ok(())
}

#[test]
fn agent_local_approval_cannot_replace_agent_runtime_authority() -> TestResult {
    let (authority, issuer) =
        LocalAuthorizationAuthority::new_operator_local(policy(100, 10, 2, 8)?);
    let issuance = authority.issue_session(&issuer, LocalSessionClient::BrowserDashboard, 10)?;
    let agent = request(REQUEST_A, InterfaceOperationId::AgentExecution)?;
    let decision = authority.decide(
        issuance.session(),
        &agent,
        LocalAuthorizationIntent::Confirm,
        11,
    )?;
    let evidence = decision
        .evidence()
        .ok_or_else(|| io::Error::other("agent confirmation must contain evidence"))?;
    assert!(evidence.agent_runtime_authorization_required());
    assert!(!evidence.authorizes_owner_action());

    let consumed = authority.consume(issuance.session(), &agent, decision, 12)?;
    assert!(consumed.agent_runtime_authorization_required());
    assert!(!consumed.authorizes_owner_action());
    Ok(())
}

#[test]
fn debug_and_audit_surfaces_redact_capabilities_and_request_ids() -> TestResult {
    let (authority, issuer) =
        LocalAuthorizationAuthority::new_operator_local(policy(100, 10, 2, 8)?);
    let issuance = authority.issue_session(&issuer, LocalSessionClient::BrowserDashboard, 10)?;
    let read = request(REQUEST_A, InterfaceOperationId::NodeEvidenceRead)?;
    let decision = authority.decide(
        issuance.session(),
        &read,
        LocalAuthorizationIntent::Proceed,
        11,
    )?;
    let debug = format!(
        "{authority:?}{issuer:?}{:?}{decision:?}",
        issuance.session()
    );

    assert!(!debug.contains(REQUEST_A));
    assert!(!debug.contains("127.0.0.1"));
    assert!(!debug.contains("Bearer"));
    assert!(!decision.audit().contains_payload());
    assert!(!decision.audit().persisted());
    assert!(!decision.audit().emitted());
    assert!(!decision.audit().authorizes_action());
    Ok(())
}
