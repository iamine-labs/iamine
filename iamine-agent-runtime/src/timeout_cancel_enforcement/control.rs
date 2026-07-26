use std::{
    fmt,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use crate::execution_lifecycle::{ExecutionIdentity, LifecycleAuthorityIdentity};

use super::{AgentTimeoutClass, CancellationSource, TimeoutCancelPolicy};

#[derive(Debug)]
pub(crate) struct TimeoutCancelAuthorityIdentity;

#[derive(Debug)]
pub(crate) struct TimeoutCancelControlIdentity;

#[must_use]
pub struct TimeoutCancelControl {
    authority: Arc<TimeoutCancelAuthorityIdentity>,
    identity: Arc<TimeoutCancelControlIdentity>,
    lifecycle_authority: Arc<LifecycleAuthorityIdentity>,
    execution: Arc<ExecutionIdentity>,
    policy: TimeoutCancelPolicy,
    cancellation: Arc<AtomicU8>,
}

impl TimeoutCancelControl {
    pub(crate) fn new(
        authority: Arc<TimeoutCancelAuthorityIdentity>,
        lifecycle_authority: Arc<LifecycleAuthorityIdentity>,
        execution: Arc<ExecutionIdentity>,
        policy: TimeoutCancelPolicy,
    ) -> Self {
        Self {
            authority,
            identity: Arc::new(TimeoutCancelControlIdentity),
            lifecycle_authority,
            execution,
            policy,
            cancellation: Arc::new(AtomicU8::new(0)),
        }
    }

    pub const fn policy(&self) -> TimeoutCancelPolicy {
        self.policy
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

    pub fn cancellation_handle(&self) -> CancellationHandle {
        CancellationHandle {
            authority: Arc::clone(&self.authority),
            control: Arc::clone(&self.identity),
            execution: Arc::clone(&self.execution),
            state: Arc::clone(&self.cancellation),
        }
    }

    pub(crate) const fn authority(&self) -> &Arc<TimeoutCancelAuthorityIdentity> {
        &self.authority
    }

    pub(crate) const fn identity(&self) -> &Arc<TimeoutCancelControlIdentity> {
        &self.identity
    }

    pub(crate) const fn lifecycle_authority(&self) -> &Arc<LifecycleAuthorityIdentity> {
        &self.lifecycle_authority
    }

    pub(crate) const fn execution(&self) -> &Arc<ExecutionIdentity> {
        &self.execution
    }

    pub(crate) const fn cancellation(&self) -> &Arc<AtomicU8> {
        &self.cancellation
    }
}

impl fmt::Debug for TimeoutCancelControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TimeoutCancelControl")
            .field("authority", &"[redacted]")
            .field("identity", &"[redacted]")
            .field("lifecycle_authority", &"[redacted]")
            .field("execution", &"[redacted]")
            .field("policy", &"[redacted]")
            .field("execution_authorized", &false)
            .field("runtime_active", &false)
            .field("persisted", &false)
            .finish()
    }
}

#[derive(Clone)]
#[must_use]
pub struct CancellationHandle {
    authority: Arc<TimeoutCancelAuthorityIdentity>,
    control: Arc<TimeoutCancelControlIdentity>,
    execution: Arc<ExecutionIdentity>,
    state: Arc<AtomicU8>,
}

impl CancellationHandle {
    pub fn requested_source(&self) -> Option<CancellationSource> {
        CancellationSource::decode(self.state.load(Ordering::Acquire))
    }

    pub fn cancellation_requested(&self) -> bool {
        self.requested_source().is_some()
    }

    pub fn cancellation_enforced(&self) -> bool {
        self.state.load(Ordering::Acquire) & CancellationSource::ENFORCED_MASK != 0
    }

    pub(crate) const fn authority(&self) -> &Arc<TimeoutCancelAuthorityIdentity> {
        &self.authority
    }

    pub(crate) const fn control(&self) -> &Arc<TimeoutCancelControlIdentity> {
        &self.control
    }

    pub(crate) const fn execution(&self) -> &Arc<ExecutionIdentity> {
        &self.execution
    }

    pub(crate) const fn state(&self) -> &Arc<AtomicU8> {
        &self.state
    }
}

impl fmt::Debug for CancellationHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CancellationHandle")
            .field("authority", &"[redacted]")
            .field("control", &"[redacted]")
            .field("execution", &"[redacted]")
            .field("signal", &"[redacted]")
            .finish()
    }
}

#[must_use]
pub struct AgentTimeoutHandle {
    authority: Arc<TimeoutCancelAuthorityIdentity>,
    control: Arc<TimeoutCancelControlIdentity>,
    execution: Arc<ExecutionIdentity>,
    timeout_class: AgentTimeoutClass,
    lifecycle_revision: u8,
    started_at: Instant,
    deadline: Instant,
}

impl AgentTimeoutHandle {
    pub(crate) fn new(
        control: &TimeoutCancelControl,
        timeout_class: AgentTimeoutClass,
        lifecycle_revision: u8,
        started_at: Instant,
    ) -> Option<Self> {
        let duration = Duration::from_millis(control.policy().timeout_ms(timeout_class));
        let deadline = started_at.checked_add(duration)?;
        Some(Self {
            authority: Arc::clone(control.authority()),
            control: Arc::clone(control.identity()),
            execution: Arc::clone(control.execution()),
            timeout_class,
            lifecycle_revision,
            started_at,
            deadline,
        })
    }

    pub const fn timeout_class(&self) -> AgentTimeoutClass {
        self.timeout_class
    }

    pub const fn lifecycle_revision(&self) -> u8 {
        self.lifecycle_revision
    }

    pub const fn started_at(&self) -> Instant {
        self.started_at
    }

    pub const fn deadline(&self) -> Instant {
        self.deadline
    }

    pub fn expired_at(&self, observed_at: Instant) -> bool {
        observed_at >= self.deadline
    }

    pub fn expired(&self) -> bool {
        self.expired_at(Instant::now())
    }

    pub(crate) const fn authority(&self) -> &Arc<TimeoutCancelAuthorityIdentity> {
        &self.authority
    }

    pub(crate) const fn control(&self) -> &Arc<TimeoutCancelControlIdentity> {
        &self.control
    }

    pub(crate) const fn execution(&self) -> &Arc<ExecutionIdentity> {
        &self.execution
    }
}

impl fmt::Debug for AgentTimeoutHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AgentTimeoutHandle")
            .field("authority", &"[redacted]")
            .field("control", &"[redacted]")
            .field("execution", &"[redacted]")
            .field("timeout_class", &self.timeout_class.as_str())
            .field("lifecycle_revision", &self.lifecycle_revision)
            .field("clock", &"[redacted]")
            .finish()
    }
}
