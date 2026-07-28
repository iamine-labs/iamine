use std::fmt;

use iamine_agents::{PermissionPolicy, PermissionRequestRef, ScopePolicy, ScopeRequestRef};

use crate::{
    AuditEventEnforcementAuthority, AuditEventEnforcementEvidence, ExecutionLifecycleAuthority,
    ExecutionLifecycleRecord, InputOutputEnforcementAuthority, InputOutputEnforcementEvidence,
    PackageReviewAuthority, PackageReviewEvidence, PackageReviewSubject,
    RoutingCandidateSelectionAuthority, RoutingCandidateSelectionEvidence,
    RuntimeCompatibilityAuthority, RuntimeCompatibilityEvidence, SandboxEnforcementAuthority,
    SandboxEnforcementEvidence, TimeoutCancelAuthority, TimeoutCancelControl,
};

#[must_use]
pub struct ExecutionAuthorizationRequest<'context, 'subject> {
    subject: PackageReviewSubject<'subject>,
    scope_policy: &'context ScopePolicy,
    scope_request: ScopeRequestRef<'context>,
    permission_policy: &'context PermissionPolicy,
    permission_request: PermissionRequestRef<'context>,
    review: Option<(
        &'context PackageReviewAuthority,
        &'context PackageReviewEvidence<'subject>,
    )>,
    compatibility: Option<(
        &'context RuntimeCompatibilityAuthority,
        &'context RuntimeCompatibilityEvidence<'subject>,
    )>,
    input_output: Option<(
        &'context InputOutputEnforcementAuthority,
        &'context InputOutputEnforcementEvidence<'subject>,
    )>,
    sandbox: Option<(
        &'context SandboxEnforcementAuthority,
        &'context SandboxEnforcementEvidence<'subject>,
    )>,
    lifecycle: Option<(
        &'context ExecutionLifecycleAuthority,
        &'context ExecutionLifecycleRecord<'subject>,
    )>,
    timeout_cancel: Option<(
        &'context TimeoutCancelAuthority,
        &'context TimeoutCancelControl,
    )>,
    routing: Option<(
        &'context RoutingCandidateSelectionAuthority,
        &'context RoutingCandidateSelectionEvidence,
    )>,
    audit: Option<(
        &'context AuditEventEnforcementAuthority,
        &'context AuditEventEnforcementEvidence,
        &'context AuditEventEnforcementEvidence,
        &'context AuditEventEnforcementEvidence,
    )>,
}

impl<'context, 'subject> ExecutionAuthorizationRequest<'context, 'subject> {
    pub const fn new(
        subject: PackageReviewSubject<'subject>,
        scope_policy: &'context ScopePolicy,
        scope_request: ScopeRequestRef<'context>,
        permission_policy: &'context PermissionPolicy,
        permission_request: PermissionRequestRef<'context>,
    ) -> Self {
        Self {
            subject,
            scope_policy,
            scope_request,
            permission_policy,
            permission_request,
            review: None,
            compatibility: None,
            input_output: None,
            sandbox: None,
            lifecycle: None,
            timeout_cancel: None,
            routing: None,
            audit: None,
        }
    }

    pub fn with_package_review(
        mut self,
        authority: &'context PackageReviewAuthority,
        evidence: &'context PackageReviewEvidence<'subject>,
    ) -> Self {
        self.review = Some((authority, evidence));
        self
    }

    pub fn with_runtime_compatibility(
        mut self,
        authority: &'context RuntimeCompatibilityAuthority,
        evidence: &'context RuntimeCompatibilityEvidence<'subject>,
    ) -> Self {
        self.compatibility = Some((authority, evidence));
        self
    }

    pub fn with_input_output(
        mut self,
        authority: &'context InputOutputEnforcementAuthority,
        evidence: &'context InputOutputEnforcementEvidence<'subject>,
    ) -> Self {
        self.input_output = Some((authority, evidence));
        self
    }

    pub fn with_sandbox(
        mut self,
        authority: &'context SandboxEnforcementAuthority,
        evidence: &'context SandboxEnforcementEvidence<'subject>,
    ) -> Self {
        self.sandbox = Some((authority, evidence));
        self
    }

    pub fn with_lifecycle(
        mut self,
        authority: &'context ExecutionLifecycleAuthority,
        record: &'context ExecutionLifecycleRecord<'subject>,
    ) -> Self {
        self.lifecycle = Some((authority, record));
        self
    }

    pub fn with_timeout_cancel(
        mut self,
        authority: &'context TimeoutCancelAuthority,
        control: &'context TimeoutCancelControl,
    ) -> Self {
        self.timeout_cancel = Some((authority, control));
        self
    }

    pub fn with_routing(
        mut self,
        authority: &'context RoutingCandidateSelectionAuthority,
        evidence: &'context RoutingCandidateSelectionEvidence,
    ) -> Self {
        self.routing = Some((authority, evidence));
        self
    }

    pub fn with_audit(
        mut self,
        authority: &'context AuditEventEnforcementAuthority,
        scope: &'context AuditEventEnforcementEvidence,
        permission: &'context AuditEventEnforcementEvidence,
        lifecycle: &'context AuditEventEnforcementEvidence,
    ) -> Self {
        self.audit = Some((authority, scope, permission, lifecycle));
        self
    }

    pub(crate) const fn subject(&self) -> PackageReviewSubject<'subject> {
        self.subject
    }

    pub(crate) const fn scope_policy(&self) -> &ScopePolicy {
        self.scope_policy
    }

    pub(crate) const fn scope_request(&self) -> ScopeRequestRef<'context> {
        self.scope_request
    }

    pub(crate) const fn permission_policy(&self) -> &PermissionPolicy {
        self.permission_policy
    }

    pub(crate) const fn permission_request(&self) -> PermissionRequestRef<'context> {
        self.permission_request
    }

    pub(crate) const fn review(
        &self,
    ) -> Option<(
        &'context PackageReviewAuthority,
        &'context PackageReviewEvidence<'subject>,
    )> {
        self.review
    }

    pub(crate) const fn compatibility(
        &self,
    ) -> Option<(
        &'context RuntimeCompatibilityAuthority,
        &'context RuntimeCompatibilityEvidence<'subject>,
    )> {
        self.compatibility
    }

    pub(crate) const fn input_output(
        &self,
    ) -> Option<(
        &'context InputOutputEnforcementAuthority,
        &'context InputOutputEnforcementEvidence<'subject>,
    )> {
        self.input_output
    }

    pub(crate) const fn sandbox(
        &self,
    ) -> Option<(
        &'context SandboxEnforcementAuthority,
        &'context SandboxEnforcementEvidence<'subject>,
    )> {
        self.sandbox
    }

    pub(crate) const fn lifecycle(
        &self,
    ) -> Option<(
        &'context ExecutionLifecycleAuthority,
        &'context ExecutionLifecycleRecord<'subject>,
    )> {
        self.lifecycle
    }

    pub(crate) const fn timeout_cancel(
        &self,
    ) -> Option<(
        &'context TimeoutCancelAuthority,
        &'context TimeoutCancelControl,
    )> {
        self.timeout_cancel
    }

    pub(crate) const fn routing(
        &self,
    ) -> Option<(
        &'context RoutingCandidateSelectionAuthority,
        &'context RoutingCandidateSelectionEvidence,
    )> {
        self.routing
    }

    pub(crate) const fn audit(
        &self,
    ) -> Option<(
        &'context AuditEventEnforcementAuthority,
        &'context AuditEventEnforcementEvidence,
        &'context AuditEventEnforcementEvidence,
        &'context AuditEventEnforcementEvidence,
    )> {
        self.audit
    }
}

impl fmt::Debug for ExecutionAuthorizationRequest<'_, '_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ExecutionAuthorizationRequest")
            .field("subject", &"[redacted]")
            .field("scope_policy", &"[redacted]")
            .field("scope_request", &"[redacted]")
            .field("permission_policy", &"[redacted]")
            .field("permission_request", &"[redacted]")
            .field("review_present", &self.review.is_some())
            .field("compatibility_present", &self.compatibility.is_some())
            .field("input_output_present", &self.input_output.is_some())
            .field("sandbox_present", &self.sandbox.is_some())
            .field("lifecycle_present", &self.lifecycle.is_some())
            .field("timeout_cancel_present", &self.timeout_cancel.is_some())
            .field("routing_present", &self.routing.is_some())
            .field("audit_present", &self.audit.is_some())
            .finish()
    }
}
