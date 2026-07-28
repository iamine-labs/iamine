use std::{fmt, sync::Arc};

use iamine_agents::{AuditEventSet, AuditEventSource, AuditOutcome};

use crate::execution_lifecycle::ExecutionIdentity;

use super::AuditEventEnforcementRequirement;

pub const AUDIT_EVENT_ENFORCEMENT_SCHEMA_VERSION: &str = "iamine.agent.audit_event.enforced-0.1";

#[derive(Debug)]
pub(crate) struct AuditEventEnforcementAuthorityIdentity;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuditEventEnforcementEvidenceStatus {
    Established,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuditEventEnforcementBlockedAction {
    TreatAsExecutionAuthorization,
}

impl AuditEventEnforcementBlockedAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TreatAsExecutionAuthorization => "treat_as_execution_authorization",
        }
    }
}

const TYPED_GATE_REQUIREMENTS: [AuditEventEnforcementRequirement; 3] = [
    AuditEventEnforcementRequirement::AuditAuthority,
    AuditEventEnforcementRequirement::TypedProjection,
    AuditEventEnforcementRequirement::BoundedEventSet,
];

const LIFECYCLE_REQUIREMENTS: [AuditEventEnforcementRequirement; 5] = [
    AuditEventEnforcementRequirement::AuditAuthority,
    AuditEventEnforcementRequirement::TypedProjection,
    AuditEventEnforcementRequirement::BoundedEventSet,
    AuditEventEnforcementRequirement::LifecycleAuthority,
    AuditEventEnforcementRequirement::ExecutionIdentity,
];

#[must_use]
pub struct AuditEventEnforcementEvidence {
    authority: Arc<AuditEventEnforcementAuthorityIdentity>,
    events: AuditEventSet,
    execution: Option<Arc<ExecutionIdentity>>,
    lifecycle_revision: Option<u8>,
    upstream_authority_bound: bool,
}

impl AuditEventEnforcementEvidence {
    pub(crate) fn typed_gate(
        authority: Arc<AuditEventEnforcementAuthorityIdentity>,
        events: AuditEventSet,
    ) -> Self {
        Self {
            authority,
            events,
            execution: None,
            lifecycle_revision: None,
            upstream_authority_bound: false,
        }
    }

    pub(crate) fn authority_bound_lifecycle(
        authority: Arc<AuditEventEnforcementAuthorityIdentity>,
        execution: Arc<ExecutionIdentity>,
        events: AuditEventSet,
        lifecycle_revision: u8,
    ) -> Self {
        Self {
            authority,
            events,
            execution: Some(execution),
            lifecycle_revision: Some(lifecycle_revision),
            upstream_authority_bound: true,
        }
    }

    pub const fn schema_version(&self) -> &'static str {
        AUDIT_EVENT_ENFORCEMENT_SCHEMA_VERSION
    }

    pub const fn status(&self) -> AuditEventEnforcementEvidenceStatus {
        AuditEventEnforcementEvidenceStatus::Established
    }

    pub const fn requirements(&self) -> &'static [AuditEventEnforcementRequirement] {
        if self.upstream_authority_bound {
            &LIFECYCLE_REQUIREMENTS
        } else {
            &TYPED_GATE_REQUIREMENTS
        }
    }

    pub const fn events(&self) -> &AuditEventSet {
        &self.events
    }

    pub const fn source(&self) -> AuditEventSource {
        self.events.primary().source()
    }

    pub const fn outcome(&self) -> AuditOutcome {
        self.events.primary().outcome()
    }

    pub const fn event_count(&self) -> usize {
        self.events.len()
    }

    pub const fn lifecycle_revision(&self) -> Option<u8> {
        self.lifecycle_revision
    }

    pub const fn upstream_authority_bound(&self) -> bool {
        self.upstream_authority_bound
    }

    pub const fn blocked_action(&self) -> AuditEventEnforcementBlockedAction {
        AuditEventEnforcementBlockedAction::TreatAsExecutionAuthorization
    }

    pub const fn event_recorded(&self) -> bool {
        true
    }

    pub const fn execution_authorized(&self) -> bool {
        false
    }

    pub const fn side_effect_verified(&self) -> bool {
        false
    }

    pub const fn package_loaded(&self) -> bool {
        false
    }

    pub const fn runtime_active(&self) -> bool {
        false
    }

    pub const fn transport_started(&self) -> bool {
        false
    }

    pub const fn persisted(&self) -> bool {
        false
    }

    pub const fn external_event_emitted(&self) -> bool {
        false
    }

    pub(crate) const fn authority(&self) -> &Arc<AuditEventEnforcementAuthorityIdentity> {
        &self.authority
    }

    pub(crate) const fn execution(&self) -> Option<&Arc<ExecutionIdentity>> {
        self.execution.as_ref()
    }
}

impl fmt::Debug for AuditEventEnforcementEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let event_classes = self
            .events
            .iter()
            .map(|event| event.class().as_str())
            .collect::<Vec<_>>();

        formatter
            .debug_struct("AuditEventEnforcementEvidence")
            .field("schema_version", &self.schema_version())
            .field("status", &self.status())
            .field("requirements", &self.requirements())
            .field("authority", &"[redacted]")
            .field("execution", &self.execution.as_ref().map(|_| "[redacted]"))
            .field("source", &self.source().as_str())
            .field("outcome", &self.outcome().as_str())
            .field("event_classes", &event_classes)
            .field("event_count", &self.event_count())
            .field("lifecycle_revision", &self.lifecycle_revision)
            .field("upstream_authority_bound", &self.upstream_authority_bound)
            .field("blocked_action", &self.blocked_action().as_str())
            .field("execution_authorized", &false)
            .field("side_effect_verified", &false)
            .field("package_loaded", &false)
            .field("runtime_active", &false)
            .field("transport_started", &false)
            .field("persisted", &false)
            .field("external_event_emitted", &false)
            .finish()
    }
}
