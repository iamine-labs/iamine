use crate::{PermissionReasonCode, ScopeReasonCode};

use super::AuditLifecycleState;

pub const AUDIT_EVENT_SCHEMA_VERSION: &str = "1.0.0";
pub const MAX_AUDIT_EVENTS_PER_PROJECTION: usize = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuditEventClass {
    LifecycleObserved,
    ScopeChecked,
    PermissionChecked,
    RefusalRecorded,
    HandoffRequired,
}

impl AuditEventClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LifecycleObserved => "lifecycle_observed",
            Self::ScopeChecked => "scope_checked",
            Self::PermissionChecked => "permission_checked",
            Self::RefusalRecorded => "refusal_recorded",
            Self::HandoffRequired => "handoff_required",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuditEventSource {
    Lifecycle,
    Scope,
    Permission,
}

impl AuditEventSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Lifecycle => "lifecycle",
            Self::Scope => "scope",
            Self::Permission => "permission",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuditOutcome {
    Observed,
    Allowed,
    ClarificationRequired,
    ConfirmationRequired,
    Refused,
    HandedOff,
}

impl AuditOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Observed => "observed",
            Self::Allowed => "allowed",
            Self::ClarificationRequired => "clarification_required",
            Self::ConfirmationRequired => "confirmation_required",
            Self::Refused => "refused",
            Self::HandedOff => "handed_off",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuditReasonCode {
    LifecycleStateObserved,
    Scope(ScopeReasonCode),
    Permission(PermissionReasonCode),
}

impl AuditReasonCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LifecycleStateObserved => "lifecycle_state_observed",
            Self::Scope(reason) => reason.as_str(),
            Self::Permission(reason) => reason.as_str(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct AgentAuditEvent {
    class: AuditEventClass,
    source: AuditEventSource,
    outcome: AuditOutcome,
    reason: AuditReasonCode,
    lifecycle_state: Option<AuditLifecycleState>,
}

impl AgentAuditEvent {
    pub(crate) const fn new(
        class: AuditEventClass,
        source: AuditEventSource,
        outcome: AuditOutcome,
        reason: AuditReasonCode,
        lifecycle_state: Option<AuditLifecycleState>,
    ) -> Self {
        Self {
            class,
            source,
            outcome,
            reason,
            lifecycle_state,
        }
    }

    pub const fn schema_version(&self) -> &'static str {
        AUDIT_EVENT_SCHEMA_VERSION
    }

    pub const fn class(&self) -> AuditEventClass {
        self.class
    }

    pub const fn source(&self) -> AuditEventSource {
        self.source
    }

    pub const fn outcome(&self) -> AuditOutcome {
        self.outcome
    }

    pub const fn reason(&self) -> AuditReasonCode {
        self.reason
    }

    pub const fn lifecycle_state(&self) -> Option<AuditLifecycleState> {
        self.lifecycle_state
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct AuditEventSet {
    primary: AgentAuditEvent,
    secondary: Option<AgentAuditEvent>,
}

impl AuditEventSet {
    pub(crate) const fn single(primary: AgentAuditEvent) -> Self {
        Self {
            primary,
            secondary: None,
        }
    }

    pub(crate) const fn pair(primary: AgentAuditEvent, secondary: AgentAuditEvent) -> Self {
        Self {
            primary,
            secondary: Some(secondary),
        }
    }

    pub const fn len(&self) -> usize {
        if self.secondary.is_some() {
            2
        } else {
            1
        }
    }

    pub const fn is_empty(&self) -> bool {
        false
    }

    pub const fn primary(&self) -> &AgentAuditEvent {
        &self.primary
    }

    pub const fn secondary(&self) -> Option<&AgentAuditEvent> {
        self.secondary.as_ref()
    }

    pub fn iter(&self) -> impl Iterator<Item = &AgentAuditEvent> {
        std::iter::once(&self.primary).chain(self.secondary.iter())
    }
}
