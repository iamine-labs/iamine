use std::{fmt, sync::Arc};

use crate::{
    InterfaceOperation, InterfaceOperationClass, InterfaceOperatorAction, InterfaceProblem,
    InterfaceProblemCode, LocalControlAuditRequirement, LocalControlAuthorizationRequirement,
    LocalControlReplayRequirement, LocalControlRequestId,
};
use thiserror::Error;

pub const LOCAL_AUTHORIZATION_SCHEMA_VERSION: &str = "1.0.0";
pub const MAX_LOCAL_AUTHORIZATION_SESSIONS: usize = 32;
pub const MAX_LOCAL_AUTHORIZATION_REPLAY_RECORDS: usize = 4096;

#[derive(Debug)]
pub(crate) struct LocalAuthorizationAuthorityIdentity;

#[derive(Debug)]
pub(crate) struct LocalSessionIdentity;

#[derive(Debug)]
pub(crate) struct LocalAuthorizationEvidenceIdentity;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum LocalAuthorizationError {
    #[error("local authorization policy is invalid")]
    InvalidPolicy,
    #[error("local authorization lifetime overflows its clock")]
    LifetimeOverflow,
    #[error("local authorization clock regressed")]
    ClockRegressed,
    #[error("local session issuer does not belong to this authority")]
    IssuerMismatch,
    #[error("local session capacity is exhausted")]
    SessionCapacityExceeded,
    #[error("local session is unavailable")]
    SessionUnavailable,
    #[error("local authorization evidence does not match its request")]
    EvidenceMismatch,
    #[error("local authorization evidence expired")]
    EvidenceExpired,
    #[error("local authorization evidence is unavailable or already consumed")]
    EvidenceUnavailable,
    #[error("local authorization state is unavailable")]
    StateUnavailable,
}

impl LocalAuthorizationError {
    pub const fn interface_problem(self) -> InterfaceProblem {
        match self {
            Self::ClockRegressed | Self::EvidenceExpired => InterfaceProblem::new(
                InterfaceProblemCode::EvidenceStale,
                InterfaceOperatorAction::Retry,
            ),
            Self::IssuerMismatch | Self::SessionUnavailable => InterfaceProblem::new(
                InterfaceProblemCode::PermissionRequired,
                InterfaceOperatorAction::AuthenticateLocally,
            ),
            Self::SessionCapacityExceeded => InterfaceProblem::new(
                InterfaceProblemCode::OwnerUnavailable,
                InterfaceOperatorAction::Retry,
            ),
            Self::EvidenceMismatch | Self::EvidenceUnavailable => InterfaceProblem::new(
                InterfaceProblemCode::PolicyBlocked,
                InterfaceOperatorAction::None,
            ),
            Self::InvalidPolicy | Self::LifetimeOverflow | Self::StateUnavailable => {
                InterfaceProblem::new(
                    InterfaceProblemCode::InternalFailure,
                    InterfaceOperatorAction::ContactSupport,
                )
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalAuthorizationPolicy {
    session_ttl_ticks: u64,
    evidence_ttl_ticks: u64,
    max_sessions: usize,
    max_replay_records: usize,
}

impl LocalAuthorizationPolicy {
    pub fn try_new(
        session_ttl_ticks: u64,
        evidence_ttl_ticks: u64,
        max_sessions: usize,
        max_replay_records: usize,
    ) -> Result<Self, LocalAuthorizationError> {
        if session_ttl_ticks == 0
            || evidence_ttl_ticks == 0
            || evidence_ttl_ticks > session_ttl_ticks
            || max_sessions == 0
            || max_sessions > MAX_LOCAL_AUTHORIZATION_SESSIONS
            || max_replay_records == 0
            || max_replay_records > MAX_LOCAL_AUTHORIZATION_REPLAY_RECORDS
        {
            return Err(LocalAuthorizationError::InvalidPolicy);
        }
        Ok(Self {
            session_ttl_ticks,
            evidence_ttl_ticks,
            max_sessions,
            max_replay_records,
        })
    }

    pub const fn session_ttl_ticks(self) -> u64 {
        self.session_ttl_ticks
    }

    pub const fn evidence_ttl_ticks(self) -> u64 {
        self.evidence_ttl_ticks
    }

    pub const fn max_sessions(self) -> usize {
        self.max_sessions
    }

    pub const fn max_replay_records(self) -> usize {
        self.max_replay_records
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalSessionClient {
    BrowserDashboard,
    LocalNative,
}

pub struct LocalSessionIssuer {
    pub(crate) authority: Arc<LocalAuthorizationAuthorityIdentity>,
}

impl LocalSessionIssuer {
    pub(crate) fn new(authority: Arc<LocalAuthorizationAuthorityIdentity>) -> Self {
        Self { authority }
    }
}

impl fmt::Debug for LocalSessionIssuer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalSessionIssuer")
            .field("authority", &"[redacted]")
            .finish()
    }
}

#[must_use]
pub struct LocalSessionEvidence {
    pub(crate) authority: Arc<LocalAuthorizationAuthorityIdentity>,
    pub(crate) identity: Arc<LocalSessionIdentity>,
    client: LocalSessionClient,
    issued_at_tick: u64,
    expires_at_tick: u64,
}

impl LocalSessionEvidence {
    pub(crate) fn new(
        authority: Arc<LocalAuthorizationAuthorityIdentity>,
        identity: Arc<LocalSessionIdentity>,
        client: LocalSessionClient,
        issued_at_tick: u64,
        expires_at_tick: u64,
    ) -> Self {
        Self {
            authority,
            identity,
            client,
            issued_at_tick,
            expires_at_tick,
        }
    }

    pub const fn schema_version(&self) -> &'static str {
        LOCAL_AUTHORIZATION_SCHEMA_VERSION
    }

    pub const fn client(&self) -> LocalSessionClient {
        self.client
    }

    pub const fn issued_at_tick(&self) -> u64 {
        self.issued_at_tick
    }

    pub const fn expires_at_tick(&self) -> u64 {
        self.expires_at_tick
    }

    pub const fn persisted(&self) -> bool {
        false
    }

    pub const fn serializable(&self) -> bool {
        false
    }

    pub const fn authorizes_action(&self) -> bool {
        false
    }
}

impl fmt::Debug for LocalSessionEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalSessionEvidence")
            .field("schema_version", &self.schema_version())
            .field("authority", &"[redacted]")
            .field("identity", &"[redacted]")
            .field("client", &self.client)
            .field("issued_at_tick", &self.issued_at_tick)
            .field("expires_at_tick", &self.expires_at_tick)
            .field("persisted", &false)
            .field("serializable", &false)
            .field("authorizes_action", &false)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalAuthorizationIntent {
    Proceed,
    Confirm,
    Deny,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalAuthorizationDenialCode {
    SessionAuthorityMismatch,
    SessionUnknown,
    SessionExpired,
    SessionRevoked,
    RequestContractMismatch,
    ConfirmationRequired,
    ExplicitlyDenied,
    ReplayDetected,
    ReplayCapacityExceeded,
}

impl LocalAuthorizationDenialCode {
    pub const fn interface_problem(self) -> InterfaceProblem {
        match self {
            Self::SessionAuthorityMismatch
            | Self::SessionUnknown
            | Self::SessionExpired
            | Self::SessionRevoked => InterfaceProblem::new(
                InterfaceProblemCode::PermissionRequired,
                InterfaceOperatorAction::AuthenticateLocally,
            ),
            Self::ConfirmationRequired => InterfaceProblem::new(
                InterfaceProblemCode::PermissionRequired,
                InterfaceOperatorAction::RequestAuthorization,
            ),
            Self::RequestContractMismatch => InterfaceProblem::new(
                InterfaceProblemCode::InvalidRequest,
                InterfaceOperatorAction::None,
            ),
            Self::ExplicitlyDenied | Self::ReplayDetected => InterfaceProblem::new(
                InterfaceProblemCode::PolicyBlocked,
                InterfaceOperatorAction::None,
            ),
            Self::ReplayCapacityExceeded => InterfaceProblem::new(
                InterfaceProblemCode::OwnerUnavailable,
                InterfaceOperatorAction::Retry,
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalAuthorizationAuditKind {
    SessionIssued,
    SessionRevoked,
    RequestApproved,
    RequestDenied,
    EvidenceConsumed,
}

pub struct LocalAuthorizationAuditHandoff {
    kind: LocalAuthorizationAuditKind,
    at_tick: u64,
    request_id: Option<LocalControlRequestId>,
    operation: Option<InterfaceOperation>,
    denial_code: Option<LocalAuthorizationDenialCode>,
}

impl LocalAuthorizationAuditHandoff {
    pub(crate) fn session(kind: LocalAuthorizationAuditKind, at_tick: u64) -> Self {
        Self {
            kind,
            at_tick,
            request_id: None,
            operation: None,
            denial_code: None,
        }
    }

    pub(crate) fn request(
        kind: LocalAuthorizationAuditKind,
        at_tick: u64,
        request_id: LocalControlRequestId,
        operation: InterfaceOperation,
        denial_code: Option<LocalAuthorizationDenialCode>,
    ) -> Self {
        Self {
            kind,
            at_tick,
            request_id: Some(request_id),
            operation: Some(operation),
            denial_code,
        }
    }

    pub const fn schema_version(&self) -> &'static str {
        LOCAL_AUTHORIZATION_SCHEMA_VERSION
    }

    pub const fn kind(&self) -> LocalAuthorizationAuditKind {
        self.kind
    }

    pub const fn at_tick(&self) -> u64 {
        self.at_tick
    }

    pub fn request_id(&self) -> Option<&str> {
        self.request_id.as_ref().map(LocalControlRequestId::as_str)
    }

    pub const fn operation(&self) -> Option<InterfaceOperation> {
        self.operation
    }

    pub const fn denial_code(&self) -> Option<LocalAuthorizationDenialCode> {
        self.denial_code
    }

    pub const fn persisted(&self) -> bool {
        false
    }

    pub const fn emitted(&self) -> bool {
        false
    }

    pub const fn contains_payload(&self) -> bool {
        false
    }

    pub const fn authorizes_action(&self) -> bool {
        false
    }
}

impl fmt::Debug for LocalAuthorizationAuditHandoff {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalAuthorizationAuditHandoff")
            .field("schema_version", &self.schema_version())
            .field("kind", &self.kind)
            .field("at_tick", &self.at_tick)
            .field(
                "request_id",
                &self.request_id.as_ref().map(|_| "[redacted]"),
            )
            .field("operation", &self.operation)
            .field("denial_code", &self.denial_code)
            .field("persisted", &false)
            .field("emitted", &false)
            .field("contains_payload", &false)
            .field("authorizes_action", &false)
            .finish()
    }
}

#[must_use]
pub struct LocalSessionIssuance {
    session: LocalSessionEvidence,
    audit: LocalAuthorizationAuditHandoff,
}

impl LocalSessionIssuance {
    pub(crate) fn new(
        session: LocalSessionEvidence,
        audit: LocalAuthorizationAuditHandoff,
    ) -> Self {
        Self { session, audit }
    }

    pub const fn session(&self) -> &LocalSessionEvidence {
        &self.session
    }

    pub const fn audit(&self) -> &LocalAuthorizationAuditHandoff {
        &self.audit
    }

    pub fn into_parts(self) -> (LocalSessionEvidence, LocalAuthorizationAuditHandoff) {
        (self.session, self.audit)
    }
}

#[must_use]
pub struct LocalAuthorizationEvidence {
    pub(crate) authority: Arc<LocalAuthorizationAuthorityIdentity>,
    pub(crate) identity: Arc<LocalAuthorizationEvidenceIdentity>,
    pub(crate) session: Arc<LocalSessionIdentity>,
    pub(crate) request_id: LocalControlRequestId,
    operation: InterfaceOperation,
    requirement: LocalControlAuthorizationRequirement,
    replay: LocalControlReplayRequirement,
    audit: LocalControlAuditRequirement,
    issued_at_tick: u64,
    expires_at_tick: u64,
}

impl LocalAuthorizationEvidence {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        authority: Arc<LocalAuthorizationAuthorityIdentity>,
        identity: Arc<LocalAuthorizationEvidenceIdentity>,
        session: Arc<LocalSessionIdentity>,
        request_id: LocalControlRequestId,
        operation: InterfaceOperation,
        requirement: LocalControlAuthorizationRequirement,
        replay: LocalControlReplayRequirement,
        audit: LocalControlAuditRequirement,
        issued_at_tick: u64,
        expires_at_tick: u64,
    ) -> Self {
        Self {
            authority,
            identity,
            session,
            request_id,
            operation,
            requirement,
            replay,
            audit,
            issued_at_tick,
            expires_at_tick,
        }
    }

    pub const fn schema_version(&self) -> &'static str {
        LOCAL_AUTHORIZATION_SCHEMA_VERSION
    }

    pub fn request_id(&self) -> &str {
        self.request_id.as_str()
    }

    pub const fn operation(&self) -> InterfaceOperation {
        self.operation
    }

    pub const fn requirement(&self) -> LocalControlAuthorizationRequirement {
        self.requirement
    }

    pub const fn replay_requirement(&self) -> LocalControlReplayRequirement {
        self.replay
    }

    pub const fn audit_requirement(&self) -> LocalControlAuditRequirement {
        self.audit
    }

    pub const fn issued_at_tick(&self) -> u64 {
        self.issued_at_tick
    }

    pub const fn expires_at_tick(&self) -> u64 {
        self.expires_at_tick
    }

    pub const fn authorizes_owner_action(&self) -> bool {
        false
    }

    pub const fn agent_runtime_authorization_required(&self) -> bool {
        matches!(
            self.operation.class(),
            InterfaceOperationClass::AgentOperation
        )
    }
}

impl fmt::Debug for LocalAuthorizationEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalAuthorizationEvidence")
            .field("schema_version", &self.schema_version())
            .field("authority", &"[redacted]")
            .field("identity", &"[redacted]")
            .field("session", &"[redacted]")
            .field("request_id", &"[redacted]")
            .field("operation", &self.operation)
            .field("requirement", &self.requirement)
            .field("replay", &self.replay)
            .field("audit", &self.audit)
            .field("issued_at_tick", &self.issued_at_tick)
            .field("expires_at_tick", &self.expires_at_tick)
            .field("authorizes_owner_action", &false)
            .finish()
    }
}

#[derive(Debug)]
pub struct LocalAuthorizationDenial {
    code: LocalAuthorizationDenialCode,
}

impl LocalAuthorizationDenial {
    pub(crate) const fn new(code: LocalAuthorizationDenialCode) -> Self {
        Self { code }
    }

    pub const fn code(&self) -> LocalAuthorizationDenialCode {
        self.code
    }

    pub const fn interface_problem(&self) -> InterfaceProblem {
        self.code.interface_problem()
    }

    pub const fn authorizes_action(&self) -> bool {
        false
    }
}

#[derive(Debug)]
pub enum LocalAuthorizationDecision {
    Approved {
        evidence: LocalAuthorizationEvidence,
        audit: LocalAuthorizationAuditHandoff,
    },
    Denied {
        denial: LocalAuthorizationDenial,
        audit: LocalAuthorizationAuditHandoff,
    },
}

impl LocalAuthorizationDecision {
    pub const fn is_approved(&self) -> bool {
        matches!(self, Self::Approved { .. })
    }

    pub const fn evidence(&self) -> Option<&LocalAuthorizationEvidence> {
        match self {
            Self::Approved { evidence, .. } => Some(evidence),
            Self::Denied { .. } => None,
        }
    }

    pub const fn denial(&self) -> Option<&LocalAuthorizationDenial> {
        match self {
            Self::Approved { .. } => None,
            Self::Denied { denial, .. } => Some(denial),
        }
    }

    pub const fn audit(&self) -> &LocalAuthorizationAuditHandoff {
        match self {
            Self::Approved { audit, .. } | Self::Denied { audit, .. } => audit,
        }
    }
}

#[must_use]
pub struct LocalAuthorizationConsumption {
    _authority: Arc<LocalAuthorizationAuthorityIdentity>,
    _session: Arc<LocalSessionIdentity>,
    request_id: LocalControlRequestId,
    operation: InterfaceOperation,
    authorization_audit: LocalAuthorizationAuditHandoff,
    consumption_audit: LocalAuthorizationAuditHandoff,
}

impl LocalAuthorizationConsumption {
    pub(crate) fn new(
        authority: Arc<LocalAuthorizationAuthorityIdentity>,
        session: Arc<LocalSessionIdentity>,
        request_id: LocalControlRequestId,
        operation: InterfaceOperation,
        authorization_audit: LocalAuthorizationAuditHandoff,
        consumption_audit: LocalAuthorizationAuditHandoff,
    ) -> Self {
        Self {
            _authority: authority,
            _session: session,
            request_id,
            operation,
            authorization_audit,
            consumption_audit,
        }
    }

    pub fn request_id(&self) -> &str {
        self.request_id.as_str()
    }

    pub const fn operation(&self) -> InterfaceOperation {
        self.operation
    }

    pub const fn authorization_audit(&self) -> &LocalAuthorizationAuditHandoff {
        &self.authorization_audit
    }

    pub const fn consumption_audit(&self) -> &LocalAuthorizationAuditHandoff {
        &self.consumption_audit
    }

    pub const fn local_gate_satisfied(&self) -> bool {
        true
    }

    pub const fn authorizes_owner_action(&self) -> bool {
        false
    }

    pub const fn agent_runtime_authorization_required(&self) -> bool {
        matches!(
            self.operation.class(),
            InterfaceOperationClass::AgentOperation
        )
    }
}

impl fmt::Debug for LocalAuthorizationConsumption {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalAuthorizationConsumption")
            .field("authority", &"[redacted]")
            .field("session", &"[redacted]")
            .field("request_id", &"[redacted]")
            .field("operation", &self.operation)
            .field("authorization_audit", &self.authorization_audit)
            .field("consumption_audit", &self.consumption_audit)
            .field("local_gate_satisfied", &true)
            .field("authorizes_owner_action", &false)
            .finish()
    }
}
