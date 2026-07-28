use std::{fmt, sync::Arc};

use crate::execution_lifecycle::{ExecutionIdentity, LifecycleAuthorityIdentity};

use super::{
    HandoffBlockedAction, HandoffOperatorSummary, HandoffReason, HandoffRequest, HandoffTarget,
};

#[derive(Debug)]
pub(crate) struct HandoffAuthorityIdentity;

#[derive(Debug)]
pub(crate) struct HandoffControlIdentity;

#[must_use]
pub struct HandoffControl {
    authority: Arc<HandoffAuthorityIdentity>,
    identity: Arc<HandoffControlIdentity>,
    lifecycle_authority: Arc<LifecycleAuthorityIdentity>,
    execution: Arc<ExecutionIdentity>,
    request: HandoffRequest,
    lifecycle_revision: u8,
}

impl HandoffControl {
    pub(crate) fn new(
        authority: Arc<HandoffAuthorityIdentity>,
        lifecycle_authority: Arc<LifecycleAuthorityIdentity>,
        execution: Arc<ExecutionIdentity>,
        request: HandoffRequest,
        lifecycle_revision: u8,
    ) -> Self {
        Self {
            authority,
            identity: Arc::new(HandoffControlIdentity),
            lifecycle_authority,
            execution,
            request,
            lifecycle_revision,
        }
    }

    pub const fn target(&self) -> HandoffTarget {
        self.request.target()
    }

    pub const fn reason(&self) -> HandoffReason {
        self.request.reason()
    }

    pub const fn operator_summary(&self) -> HandoffOperatorSummary {
        self.request.operator_summary()
    }

    pub const fn blocked_action(&self) -> HandoffBlockedAction {
        self.request.blocked_action()
    }

    pub const fn lifecycle_revision(&self) -> u8 {
        self.lifecycle_revision
    }

    pub const fn prepared(&self) -> bool {
        true
    }

    pub const fn dispatch_recorded(&self) -> bool {
        false
    }

    pub const fn transport_performed(&self) -> bool {
        false
    }

    pub const fn concrete_target_selected(&self) -> bool {
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

    pub(crate) const fn authority(&self) -> &Arc<HandoffAuthorityIdentity> {
        &self.authority
    }

    pub(crate) const fn identity(&self) -> &Arc<HandoffControlIdentity> {
        &self.identity
    }

    pub(crate) const fn lifecycle_authority(&self) -> &Arc<LifecycleAuthorityIdentity> {
        &self.lifecycle_authority
    }

    pub(crate) const fn execution(&self) -> &Arc<ExecutionIdentity> {
        &self.execution
    }
}

impl fmt::Debug for HandoffControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HandoffControl")
            .field("authority", &"[redacted]")
            .field("identity", &"[redacted]")
            .field("lifecycle_authority", &"[redacted]")
            .field("execution", &"[redacted]")
            .field("target", &self.target().as_str())
            .field("reason", &self.reason().as_str())
            .field("lifecycle_revision", &self.lifecycle_revision)
            .field("dispatch_recorded", &false)
            .field("transport_performed", &false)
            .field("execution_authorized", &false)
            .finish()
    }
}
