use std::{fmt, sync::Arc};

use crate::HandoffTarget;

use super::policy::OutOfScopeResponsePlan;
use super::{
    OutOfScopeBlockedAction, OutOfScopeOperatorSummary, OutOfScopeResponseAuthorityIdentity,
    OutOfScopeResponseClass, OutOfScopeResponseReason, OutOfScopeResponseSource,
    OutOfScopeSourceReason,
};

pub const OUT_OF_SCOPE_RESPONSE_SCHEMA_VERSION: &str =
    "iamine.agent.out_of_scope_response.enforced-0.1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum OutOfScopeResponseEvidenceStatus {
    Recorded,
}

#[must_use]
pub struct OutOfScopeResponseEvidence {
    authority: Arc<OutOfScopeResponseAuthorityIdentity>,
    plan: OutOfScopeResponsePlan,
    handoff_dispatch_recorded: bool,
    local_execution_cancelled: bool,
}

impl OutOfScopeResponseEvidence {
    pub(crate) fn new(
        authority: Arc<OutOfScopeResponseAuthorityIdentity>,
        plan: OutOfScopeResponsePlan,
        handoff_dispatch_recorded: bool,
        local_execution_cancelled: bool,
    ) -> Self {
        Self {
            authority,
            plan,
            handoff_dispatch_recorded,
            local_execution_cancelled,
        }
    }

    pub const fn schema_version(&self) -> &'static str {
        OUT_OF_SCOPE_RESPONSE_SCHEMA_VERSION
    }

    pub const fn status(&self) -> OutOfScopeResponseEvidenceStatus {
        OutOfScopeResponseEvidenceStatus::Recorded
    }

    pub const fn source(&self) -> OutOfScopeResponseSource {
        self.plan.source()
    }

    pub const fn source_reason(&self) -> OutOfScopeSourceReason {
        self.plan.source_reason()
    }

    pub const fn response_class(&self) -> OutOfScopeResponseClass {
        self.plan.response_class()
    }

    pub const fn response_reason(&self) -> OutOfScopeResponseReason {
        self.plan.response_reason()
    }

    pub const fn operator_summary(&self) -> OutOfScopeOperatorSummary {
        self.response_reason().operator_summary()
    }

    pub const fn handoff_target(&self) -> Option<HandoffTarget> {
        self.plan.handoff_target()
    }

    pub const fn blocked_action(&self) -> OutOfScopeBlockedAction {
        OutOfScopeBlockedAction::ContinueLocalExecution
    }

    pub const fn response_recorded(&self) -> bool {
        true
    }

    pub const fn operator_visible(&self) -> bool {
        true
    }

    pub const fn operator_input_required(&self) -> bool {
        matches!(
            self.response_class(),
            OutOfScopeResponseClass::Clarify | OutOfScopeResponseClass::Blocked
        )
    }

    pub const fn handoff_dispatch_recorded(&self) -> bool {
        self.handoff_dispatch_recorded
    }

    pub const fn local_execution_cancelled(&self) -> bool {
        self.local_execution_cancelled
    }

    pub const fn response_delivered(&self) -> bool {
        false
    }

    pub const fn task_success(&self) -> bool {
        false
    }

    pub const fn scope_expanded(&self) -> bool {
        false
    }

    pub const fn permissions_expanded(&self) -> bool {
        false
    }

    pub const fn execution_authorized(&self) -> bool {
        false
    }

    pub const fn runtime_active(&self) -> bool {
        false
    }

    pub const fn transport_performed(&self) -> bool {
        false
    }

    pub const fn persisted(&self) -> bool {
        false
    }

    pub const fn audit_emitted(&self) -> bool {
        false
    }

    pub(crate) const fn authority(&self) -> &Arc<OutOfScopeResponseAuthorityIdentity> {
        &self.authority
    }
}

impl fmt::Debug for OutOfScopeResponseEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutOfScopeResponseEvidence")
            .field("schema_version", &self.schema_version())
            .field("status", &self.status())
            .field("authority", &"[redacted]")
            .field("source", &self.source().as_str())
            .field("source_reason", &self.source_reason().as_str())
            .field("response_class", &self.response_class().as_str())
            .field("response_reason", &self.response_reason().as_str())
            .field("operator_summary", &self.operator_summary().as_str())
            .field(
                "handoff_target",
                &self.handoff_target().map(HandoffTarget::as_str),
            )
            .field("blocked_action", &self.blocked_action().as_str())
            .field("handoff_dispatch_recorded", &self.handoff_dispatch_recorded)
            .field("local_execution_cancelled", &self.local_execution_cancelled)
            .field("response_delivered", &false)
            .field("task_success", &false)
            .field("scope_expanded", &false)
            .field("permissions_expanded", &false)
            .field("execution_authorized", &false)
            .field("runtime_active", &false)
            .field("transport_performed", &false)
            .field("persisted", &false)
            .field("audit_emitted", &false)
            .finish()
    }
}
