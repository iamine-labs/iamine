use std::{fmt, sync::Arc, sync::Mutex};

use crate::{
    LocalControlAuthorizationHandoff, LocalControlAuthorizationRequirement,
    LocalControlReplayRequirement, LocalControlValidatedRequest,
};

use super::types::{
    LocalAuthorizationAuditHandoff, LocalAuthorizationAuditKind,
    LocalAuthorizationAuthorityIdentity, LocalAuthorizationConsumption, LocalAuthorizationDecision,
    LocalAuthorizationDenial, LocalAuthorizationDenialCode, LocalAuthorizationError,
    LocalAuthorizationEvidence, LocalAuthorizationEvidenceIdentity, LocalAuthorizationIntent,
    LocalAuthorizationPolicy, LocalSessionClient, LocalSessionEvidence, LocalSessionIdentity,
    LocalSessionIssuance, LocalSessionIssuer,
};

struct LocalSessionRecord {
    identity: Arc<LocalSessionIdentity>,
    client: LocalSessionClient,
    expires_at_tick: u64,
    revoked: bool,
}

struct LocalReplayRecord {
    request_id: String,
    expires_at_tick: u64,
    evidence: Option<Arc<LocalAuthorizationEvidenceIdentity>>,
}

#[derive(Default)]
struct LocalAuthorizationState {
    last_tick: Option<u64>,
    sessions: Vec<LocalSessionRecord>,
    replay_records: Vec<LocalReplayRecord>,
}

impl LocalAuthorizationState {
    fn observe_and_prune(&mut self, now_tick: u64) -> Result<(), LocalAuthorizationError> {
        if self.last_tick.is_some_and(|last| now_tick < last) {
            return Err(LocalAuthorizationError::ClockRegressed);
        }
        self.last_tick = Some(now_tick);
        self.sessions
            .retain(|record| now_tick < record.expires_at_tick);
        self.replay_records
            .retain(|record| now_tick < record.expires_at_tick);
        Ok(())
    }
}

/// Operator-local owner for dashboard session and request authorization.
///
/// The authority emits only opaque, in-process evidence. It cannot bind a
/// listener, dispatch an owner operation, authorize an agent runtime, persist
/// state, or emit an external audit event.
pub struct LocalAuthorizationAuthority {
    identity: Arc<LocalAuthorizationAuthorityIdentity>,
    policy: LocalAuthorizationPolicy,
    state: Mutex<LocalAuthorizationState>,
}

impl LocalAuthorizationAuthority {
    pub fn new_operator_local(policy: LocalAuthorizationPolicy) -> (Self, LocalSessionIssuer) {
        let identity = Arc::new(LocalAuthorizationAuthorityIdentity);
        let issuer = LocalSessionIssuer::new(Arc::clone(&identity));
        (
            Self {
                identity,
                policy,
                state: Mutex::new(LocalAuthorizationState::default()),
            },
            issuer,
        )
    }

    pub const fn policy(&self) -> LocalAuthorizationPolicy {
        self.policy
    }

    pub fn issue_session(
        &self,
        issuer: &LocalSessionIssuer,
        client: LocalSessionClient,
        now_tick: u64,
    ) -> Result<LocalSessionIssuance, LocalAuthorizationError> {
        if !Arc::ptr_eq(&self.identity, &issuer.authority) {
            return Err(LocalAuthorizationError::IssuerMismatch);
        }
        let expires_at_tick = now_tick
            .checked_add(self.policy.session_ttl_ticks())
            .ok_or(LocalAuthorizationError::LifetimeOverflow)?;
        let mut state = self.lock_state()?;
        state.observe_and_prune(now_tick)?;
        if state.sessions.len() >= self.policy.max_sessions() {
            return Err(LocalAuthorizationError::SessionCapacityExceeded);
        }

        let session_identity = Arc::new(LocalSessionIdentity);
        state.sessions.push(LocalSessionRecord {
            identity: Arc::clone(&session_identity),
            client,
            expires_at_tick,
            revoked: false,
        });
        let session = LocalSessionEvidence::new(
            Arc::clone(&self.identity),
            session_identity,
            client,
            now_tick,
            expires_at_tick,
        );
        let audit = LocalAuthorizationAuditHandoff::session(
            LocalAuthorizationAuditKind::SessionIssued,
            now_tick,
        );
        Ok(LocalSessionIssuance::new(session, audit))
    }

    pub fn revoke_session(
        &self,
        issuer: &LocalSessionIssuer,
        session: &LocalSessionEvidence,
        now_tick: u64,
    ) -> Result<LocalAuthorizationAuditHandoff, LocalAuthorizationError> {
        if !Arc::ptr_eq(&self.identity, &issuer.authority) {
            return Err(LocalAuthorizationError::IssuerMismatch);
        }
        let mut state = self.lock_state()?;
        state.observe_and_prune(now_tick)?;
        if Self::session_denial(&self.identity, &state, session, now_tick).is_some() {
            return Err(LocalAuthorizationError::SessionUnavailable);
        }
        let record = state
            .sessions
            .iter_mut()
            .find(|record| Arc::ptr_eq(&record.identity, &session.identity))
            .ok_or(LocalAuthorizationError::SessionUnavailable)?;
        record.revoked = true;
        Ok(LocalAuthorizationAuditHandoff::session(
            LocalAuthorizationAuditKind::SessionRevoked,
            now_tick,
        ))
    }

    pub fn decide(
        &self,
        session: &LocalSessionEvidence,
        request: &LocalControlValidatedRequest,
        intent: LocalAuthorizationIntent,
        now_tick: u64,
    ) -> Result<LocalAuthorizationDecision, LocalAuthorizationError> {
        let mut state = self.lock_state()?;
        state.observe_and_prune(now_tick)?;

        if let Some(code) = Self::session_denial(&self.identity, &state, session, now_tick) {
            return Ok(Self::denied(request, code, now_tick));
        }

        let expected =
            LocalControlAuthorizationHandoff::for_operation(request.authorization.operation);
        if request.authorization != expected {
            return Ok(Self::denied(
                request,
                LocalAuthorizationDenialCode::RequestContractMismatch,
                now_tick,
            ));
        }

        let single_use = matches!(
            expected.replay,
            LocalControlReplayRequirement::SingleUseAuthorizationEvidence
        );
        if single_use
            && state
                .replay_records
                .iter()
                .any(|record| record.request_id == request.request_id.as_str())
        {
            return Ok(Self::denied(
                request,
                LocalAuthorizationDenialCode::ReplayDetected,
                now_tick,
            ));
        }
        if single_use && state.replay_records.len() >= self.policy.max_replay_records() {
            return Ok(Self::denied(
                request,
                LocalAuthorizationDenialCode::ReplayCapacityExceeded,
                now_tick,
            ));
        }

        let denial = match intent {
            LocalAuthorizationIntent::Deny => Some(LocalAuthorizationDenialCode::ExplicitlyDenied),
            LocalAuthorizationIntent::Proceed
                if !matches!(
                    expected.requirement,
                    LocalControlAuthorizationRequirement::ReadOnlySession
                ) =>
            {
                Some(LocalAuthorizationDenialCode::ConfirmationRequired)
            }
            LocalAuthorizationIntent::Proceed | LocalAuthorizationIntent::Confirm => None,
        };

        if let Some(code) = denial {
            if single_use {
                state.replay_records.push(LocalReplayRecord {
                    request_id: request.request_id.as_str().to_string(),
                    expires_at_tick: session.expires_at_tick(),
                    evidence: None,
                });
            }
            return Ok(Self::denied(request, code, now_tick));
        }

        let expires_at_tick = now_tick
            .checked_add(self.policy.evidence_ttl_ticks())
            .ok_or(LocalAuthorizationError::LifetimeOverflow)?
            .min(session.expires_at_tick());
        let evidence_identity = Arc::new(LocalAuthorizationEvidenceIdentity);
        if single_use {
            state.replay_records.push(LocalReplayRecord {
                request_id: request.request_id.as_str().to_string(),
                expires_at_tick: session.expires_at_tick(),
                evidence: Some(Arc::clone(&evidence_identity)),
            });
        }
        let evidence = LocalAuthorizationEvidence::new(
            Arc::clone(&self.identity),
            evidence_identity,
            Arc::clone(&session.identity),
            request.request_id.clone(),
            expected.operation,
            expected.requirement,
            expected.replay,
            expected.audit,
            now_tick,
            expires_at_tick,
        );
        let audit = LocalAuthorizationAuditHandoff::request(
            LocalAuthorizationAuditKind::RequestApproved,
            now_tick,
            request.request_id.clone(),
            expected.operation,
            None,
        );
        Ok(LocalAuthorizationDecision::Approved { evidence, audit })
    }

    pub fn consume(
        &self,
        session: &LocalSessionEvidence,
        request: &LocalControlValidatedRequest,
        decision: LocalAuthorizationDecision,
        now_tick: u64,
    ) -> Result<LocalAuthorizationConsumption, LocalAuthorizationError> {
        let LocalAuthorizationDecision::Approved {
            evidence,
            audit: authorization_audit,
        } = decision
        else {
            return Err(LocalAuthorizationError::EvidenceUnavailable);
        };
        let mut state = self.lock_state()?;
        state.observe_and_prune(now_tick)?;
        if Self::session_denial(&self.identity, &state, session, now_tick).is_some() {
            return Err(LocalAuthorizationError::SessionUnavailable);
        }
        let expected =
            LocalControlAuthorizationHandoff::for_operation(request.authorization.operation);
        let evidence_matches = request.authorization == expected
            && Arc::ptr_eq(&self.identity, &evidence.authority)
            && Arc::ptr_eq(&session.identity, &evidence.session)
            && evidence.request_id.as_str() == request.request_id.as_str()
            && evidence.operation() == expected.operation
            && evidence.requirement() == expected.requirement
            && evidence.replay_requirement() == expected.replay
            && evidence.audit_requirement() == expected.audit
            && authorization_audit.kind() == LocalAuthorizationAuditKind::RequestApproved
            && authorization_audit.request_id() == Some(request.request_id.as_str())
            && authorization_audit.operation() == Some(expected.operation)
            && authorization_audit.denial_code().is_none();
        if !evidence_matches {
            return Err(LocalAuthorizationError::EvidenceMismatch);
        }
        if now_tick >= evidence.expires_at_tick() {
            return Err(LocalAuthorizationError::EvidenceExpired);
        }

        if matches!(
            expected.replay,
            LocalControlReplayRequirement::SingleUseAuthorizationEvidence
        ) {
            let record = state
                .replay_records
                .iter_mut()
                .find(|record| record.request_id == request.request_id.as_str())
                .ok_or(LocalAuthorizationError::EvidenceUnavailable)?;
            let matches_identity = record
                .evidence
                .as_ref()
                .is_some_and(|identity| Arc::ptr_eq(identity, &evidence.identity));
            if !matches_identity {
                return Err(LocalAuthorizationError::EvidenceUnavailable);
            }
            record.evidence = None;
        }

        let audit = LocalAuthorizationAuditHandoff::request(
            LocalAuthorizationAuditKind::EvidenceConsumed,
            now_tick,
            request.request_id.clone(),
            expected.operation,
            None,
        );
        Ok(LocalAuthorizationConsumption::new(
            Arc::clone(&self.identity),
            Arc::clone(&session.identity),
            request.request_id.clone(),
            expected.operation,
            authorization_audit,
            audit,
        ))
    }

    fn denied(
        request: &LocalControlValidatedRequest,
        code: LocalAuthorizationDenialCode,
        now_tick: u64,
    ) -> LocalAuthorizationDecision {
        let audit = LocalAuthorizationAuditHandoff::request(
            LocalAuthorizationAuditKind::RequestDenied,
            now_tick,
            request.request_id.clone(),
            request.authorization.operation,
            Some(code),
        );
        LocalAuthorizationDecision::Denied {
            denial: LocalAuthorizationDenial::new(code),
            audit,
        }
    }

    fn session_denial(
        authority: &Arc<LocalAuthorizationAuthorityIdentity>,
        state: &LocalAuthorizationState,
        session: &LocalSessionEvidence,
        now_tick: u64,
    ) -> Option<LocalAuthorizationDenialCode> {
        if !Arc::ptr_eq(authority, &session.authority) {
            return Some(LocalAuthorizationDenialCode::SessionAuthorityMismatch);
        }
        if now_tick >= session.expires_at_tick() {
            return Some(LocalAuthorizationDenialCode::SessionExpired);
        }
        let Some(record) = state
            .sessions
            .iter()
            .find(|record| Arc::ptr_eq(&record.identity, &session.identity))
        else {
            return Some(LocalAuthorizationDenialCode::SessionUnknown);
        };
        if record.client != session.client() || record.expires_at_tick != session.expires_at_tick()
        {
            return Some(LocalAuthorizationDenialCode::SessionUnknown);
        }
        if record.revoked {
            return Some(LocalAuthorizationDenialCode::SessionRevoked);
        }
        None
    }

    fn lock_state(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, LocalAuthorizationState>, LocalAuthorizationError> {
        self.state
            .lock()
            .map_err(|_| LocalAuthorizationError::StateUnavailable)
    }
}

impl fmt::Debug for LocalAuthorizationAuthority {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LocalAuthorizationAuthority")
            .field("identity", &"[redacted]")
            .field("policy", &self.policy)
            .field("state", &"[redacted]")
            .finish()
    }
}
