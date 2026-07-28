use std::{fmt, sync::Arc};

use crate::{
    execution_lifecycle::ExecutionIdentity, ExecutionLifecycleState,
    ExecutionLifecycleTransitionEvidence,
};

use super::{
    HandoffAuthorityIdentity, HandoffBlockedAction, HandoffControl, HandoffControlIdentity,
    HandoffOperatorSummary, HandoffReason, HandoffTarget,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum HandoffDispatchEvidenceStatus {
    Recorded,
}

pub const HANDOFF_DISPATCH_SCHEMA_VERSION: &str = "iamine.agent.handoff.dispatch-0.1";

#[must_use]
pub struct HandoffDispatchEvidence {
    authority: Arc<HandoffAuthorityIdentity>,
    control: Arc<HandoffControlIdentity>,
    execution: Arc<ExecutionIdentity>,
    target: HandoffTarget,
    reason: HandoffReason,
    operator_summary: HandoffOperatorSummary,
    blocked_action: HandoffBlockedAction,
    transition: ExecutionLifecycleTransitionEvidence,
}

impl HandoffDispatchEvidence {
    pub(crate) fn new(
        authority: Arc<HandoffAuthorityIdentity>,
        control: &HandoffControl,
        transition: ExecutionLifecycleTransitionEvidence,
    ) -> Self {
        Self {
            authority,
            control: Arc::clone(control.identity()),
            execution: Arc::clone(control.execution()),
            target: control.target(),
            reason: control.reason(),
            operator_summary: control.operator_summary(),
            blocked_action: control.blocked_action(),
            transition,
        }
    }

    pub const fn schema_version(&self) -> &'static str {
        HANDOFF_DISPATCH_SCHEMA_VERSION
    }

    pub const fn status(&self) -> HandoffDispatchEvidenceStatus {
        HandoffDispatchEvidenceStatus::Recorded
    }

    pub const fn target(&self) -> HandoffTarget {
        self.target
    }

    pub const fn reason(&self) -> HandoffReason {
        self.reason
    }

    pub const fn operator_summary(&self) -> HandoffOperatorSummary {
        self.operator_summary
    }

    pub const fn blocked_action(&self) -> HandoffBlockedAction {
        self.blocked_action
    }

    pub const fn terminal_state(&self) -> ExecutionLifecycleState {
        self.transition.to()
    }

    pub const fn lifecycle_revision(&self) -> u8 {
        self.transition.revision()
    }

    pub const fn dispatch_recorded(&self) -> bool {
        true
    }

    pub const fn local_execution_cancelled(&self) -> bool {
        matches!(self.terminal_state(), ExecutionLifecycleState::Cancelled)
    }

    pub const fn transport_performed(&self) -> bool {
        false
    }

    pub const fn concrete_target_selected(&self) -> bool {
        false
    }

    pub const fn target_execution_started(&self) -> bool {
        false
    }

    pub const fn human_approval_completed(&self) -> bool {
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

    pub const fn persisted(&self) -> bool {
        false
    }

    pub const fn audit_emitted(&self) -> bool {
        false
    }

    pub const fn lifecycle_transition(&self) -> &ExecutionLifecycleTransitionEvidence {
        &self.transition
    }

    pub(crate) const fn authority(&self) -> &Arc<HandoffAuthorityIdentity> {
        &self.authority
    }

    pub(crate) const fn control(&self) -> &Arc<HandoffControlIdentity> {
        &self.control
    }

    pub(crate) const fn execution(&self) -> &Arc<ExecutionIdentity> {
        &self.execution
    }
}

impl fmt::Debug for HandoffDispatchEvidence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HandoffDispatchEvidence")
            .field("schema_version", &self.schema_version())
            .field("status", &self.status())
            .field("authority", &"[redacted]")
            .field("control", &"[redacted]")
            .field("execution", &"[redacted]")
            .field("target", &self.target.as_str())
            .field("reason", &self.reason.as_str())
            .field("operator_summary", &self.operator_summary.as_str())
            .field("blocked_action", &self.blocked_action.as_str())
            .field("terminal_state", &self.terminal_state().as_str())
            .field("lifecycle_revision", &self.lifecycle_revision())
            .field("transport_performed", &false)
            .field("concrete_target_selected", &false)
            .field("target_execution_started", &false)
            .field("human_approval_completed", &false)
            .field("scope_expanded", &false)
            .field("permissions_expanded", &false)
            .field("execution_authorized", &false)
            .field("runtime_active", &false)
            .field("persisted", &false)
            .field("audit_emitted", &false)
            .finish()
    }
}
