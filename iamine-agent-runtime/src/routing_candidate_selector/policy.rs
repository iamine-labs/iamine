use std::fmt;

use iamine_agents::{PermissionEvaluation, ResourceOperatingMode, ScopeEvaluation};

use crate::{
    PackageReviewSubject, RuntimeCompatibilityEvidence, RuntimeNetworkAvailability,
    SandboxEnforcementEvidence,
};

use super::{
    RoutingCandidateSelectorError, RoutingCandidateSelectorErrorCode,
    RoutingCandidateSelectorRequirement,
};

pub const MAX_ROUTING_CANDIDATES: usize = 64;
pub const MAX_ROUTING_CANDIDATE_ID_BYTES: usize = 128;
pub const MAX_ROUTING_TASK_TYPE_BYTES: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RoutingCandidateSelectionOutcome {
    CandidateSelected,
    MultipleCandidates,
    NoCandidate,
    HandoffRequired,
    Blocked,
}

pub const ROUTING_CANDIDATE_SELECTION_OUTCOMES: [RoutingCandidateSelectionOutcome; 5] = [
    RoutingCandidateSelectionOutcome::CandidateSelected,
    RoutingCandidateSelectionOutcome::MultipleCandidates,
    RoutingCandidateSelectionOutcome::NoCandidate,
    RoutingCandidateSelectionOutcome::HandoffRequired,
    RoutingCandidateSelectionOutcome::Blocked,
];

impl RoutingCandidateSelectionOutcome {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CandidateSelected => "candidate_selected",
            Self::MultipleCandidates => "multiple_candidates",
            Self::NoCandidate => "no_candidate",
            Self::HandoffRequired => "handoff_required",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RoutingCandidateExclusionReason {
    ScopeMismatch,
    PermissionMismatch,
    ResourceMismatch,
    RiskTooHigh,
    NodeIncompatible,
    SandboxUnavailable,
    PolicyConflict,
    MetadataUnknown,
}

pub const ROUTING_CANDIDATE_EXCLUSION_REASONS: [RoutingCandidateExclusionReason; 8] = [
    RoutingCandidateExclusionReason::ScopeMismatch,
    RoutingCandidateExclusionReason::PermissionMismatch,
    RoutingCandidateExclusionReason::ResourceMismatch,
    RoutingCandidateExclusionReason::RiskTooHigh,
    RoutingCandidateExclusionReason::NodeIncompatible,
    RoutingCandidateExclusionReason::SandboxUnavailable,
    RoutingCandidateExclusionReason::PolicyConflict,
    RoutingCandidateExclusionReason::MetadataUnknown,
];

impl RoutingCandidateExclusionReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ScopeMismatch => "scope_mismatch",
            Self::PermissionMismatch => "permission_mismatch",
            Self::ResourceMismatch => "resource_mismatch",
            Self::RiskTooHigh => "risk_too_high",
            Self::NodeIncompatible => "node_incompatible",
            Self::SandboxUnavailable => "sandbox_unavailable",
            Self::PolicyConflict => "policy_conflict",
            Self::MetadataUnknown => "metadata_unknown",
        }
    }

    pub(crate) const fn index(self) -> usize {
        match self {
            Self::ScopeMismatch => 0,
            Self::PermissionMismatch => 1,
            Self::ResourceMismatch => 2,
            Self::RiskTooHigh => 3,
            Self::NodeIncompatible => 4,
            Self::SandboxUnavailable => 5,
            Self::PolicyConflict => 6,
            Self::MetadataUnknown => 7,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RoutingSelectionBlockedAction {
    ContinueLocalExecution,
}

impl RoutingSelectionBlockedAction {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ContinueLocalExecution => "continue_local_execution",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RoutingCandidateRiskClass {
    Low,
    Moderate,
    High,
    Prohibited,
}

impl RoutingCandidateRiskClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Moderate => "moderate",
            Self::High => "high",
            Self::Prohibited => "prohibited",
        }
    }

    pub(crate) const fn rank(self) -> u8 {
        match self {
            Self::Low => 0,
            Self::Moderate => 1,
            Self::High => 2,
            Self::Prohibited => 3,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum RoutingCandidateAvailability {
    Available,
    Busy,
    Unavailable,
    Unknown,
}

impl RoutingCandidateAvailability {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Busy => "busy",
            Self::Unavailable => "unavailable",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Copy)]
#[non_exhaustive]
pub enum RoutingCandidateCompatibility<'a> {
    Compatible(&'a RuntimeCompatibilityEvidence<'a>),
    Incompatible,
    Unknown,
}

impl fmt::Debug for RoutingCandidateCompatibility<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Compatible(_) => "Compatible([redacted])",
            Self::Incompatible => "Incompatible",
            Self::Unknown => "Unknown",
        })
    }
}

#[derive(Clone, Copy)]
#[non_exhaustive]
pub enum RoutingCandidateSandbox<'a> {
    Prepared(&'a SandboxEnforcementEvidence<'a>),
    Unavailable,
    Unknown,
}

impl fmt::Debug for RoutingCandidateSandbox<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Prepared(_) => "Prepared([redacted])",
            Self::Unavailable => "Unavailable",
            Self::Unknown => "Unknown",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoutingResourceRequirements {
    logical_cores: u16,
    memory_mb: u64,
    storage_mb: u64,
    network: RuntimeNetworkAvailability,
}

impl RoutingResourceRequirements {
    pub fn new(
        logical_cores: u16,
        memory_mb: u64,
        storage_mb: u64,
        network: RuntimeNetworkAvailability,
    ) -> Result<Self, RoutingCandidateSelectorError> {
        if logical_cores == 0 {
            return Err(RoutingCandidateSelectorError::new(
                RoutingCandidateSelectorErrorCode::ZeroLogicalCoreRequirement,
                RoutingCandidateSelectorRequirement::ResourceRequirements,
            ));
        }
        if memory_mb == 0 {
            return Err(RoutingCandidateSelectorError::new(
                RoutingCandidateSelectorErrorCode::ZeroMemoryRequirement,
                RoutingCandidateSelectorRequirement::ResourceRequirements,
            ));
        }
        if storage_mb == 0 {
            return Err(RoutingCandidateSelectorError::new(
                RoutingCandidateSelectorErrorCode::ZeroStorageRequirement,
                RoutingCandidateSelectorRequirement::ResourceRequirements,
            ));
        }
        Ok(Self {
            logical_cores,
            memory_mb,
            storage_mb,
            network,
        })
    }

    pub const fn logical_cores(self) -> u16 {
        self.logical_cores
    }

    pub const fn memory_mb(self) -> u64 {
        self.memory_mb
    }

    pub const fn storage_mb(self) -> u64 {
        self.storage_mb
    }

    pub const fn network(self) -> RuntimeNetworkAvailability {
        self.network
    }
}

#[derive(Clone, Copy)]
pub struct RoutingSelectionRequestRef<'a> {
    task_type: &'a str,
    operating_mode: ResourceOperatingMode,
    resources: RoutingResourceRequirements,
    maximum_risk: RoutingCandidateRiskClass,
}

impl<'a> RoutingSelectionRequestRef<'a> {
    pub const fn new(
        task_type: &'a str,
        operating_mode: ResourceOperatingMode,
        resources: RoutingResourceRequirements,
        maximum_risk: RoutingCandidateRiskClass,
    ) -> Self {
        Self {
            task_type,
            operating_mode,
            resources,
            maximum_risk,
        }
    }

    pub const fn task_type(self) -> &'a str {
        self.task_type
    }

    pub const fn operating_mode(self) -> ResourceOperatingMode {
        self.operating_mode
    }

    pub const fn resources(self) -> RoutingResourceRequirements {
        self.resources
    }

    pub const fn maximum_risk(self) -> RoutingCandidateRiskClass {
        self.maximum_risk
    }
}

impl fmt::Debug for RoutingSelectionRequestRef<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RoutingSelectionRequestRef")
            .field("task_type", &"[redacted]")
            .field("operating_mode", &self.operating_mode.as_str())
            .field("resources", &"[redacted]")
            .field("maximum_risk", &self.maximum_risk.as_str())
            .finish()
    }
}

#[derive(Clone, Copy)]
pub struct RoutingCandidateRef<'a> {
    candidate_id: &'a str,
    task_type: &'a str,
    risk_class: RoutingCandidateRiskClass,
    availability: RoutingCandidateAvailability,
    subject: PackageReviewSubject<'a>,
    scope: ScopeEvaluation,
    permission: PermissionEvaluation,
    compatibility: RoutingCandidateCompatibility<'a>,
    sandbox: RoutingCandidateSandbox<'a>,
}

impl<'a> RoutingCandidateRef<'a> {
    #[allow(clippy::too_many_arguments)]
    pub const fn new(
        candidate_id: &'a str,
        task_type: &'a str,
        risk_class: RoutingCandidateRiskClass,
        availability: RoutingCandidateAvailability,
        subject: PackageReviewSubject<'a>,
        scope: ScopeEvaluation,
        permission: PermissionEvaluation,
        compatibility: RoutingCandidateCompatibility<'a>,
        sandbox: RoutingCandidateSandbox<'a>,
    ) -> Self {
        Self {
            candidate_id,
            task_type,
            risk_class,
            availability,
            subject,
            scope,
            permission,
            compatibility,
            sandbox,
        }
    }

    pub const fn candidate_id(self) -> &'a str {
        self.candidate_id
    }

    pub const fn task_type(self) -> &'a str {
        self.task_type
    }

    pub const fn risk_class(self) -> RoutingCandidateRiskClass {
        self.risk_class
    }

    pub const fn availability(self) -> RoutingCandidateAvailability {
        self.availability
    }

    pub const fn subject(self) -> PackageReviewSubject<'a> {
        self.subject
    }

    pub const fn scope(self) -> ScopeEvaluation {
        self.scope
    }

    pub const fn permission(self) -> PermissionEvaluation {
        self.permission
    }

    pub const fn compatibility(self) -> RoutingCandidateCompatibility<'a> {
        self.compatibility
    }

    pub const fn sandbox(self) -> RoutingCandidateSandbox<'a> {
        self.sandbox
    }
}

impl fmt::Debug for RoutingCandidateRef<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RoutingCandidateRef")
            .field("candidate_id", &"[redacted]")
            .field("task_type", &"[redacted]")
            .field("risk_class", &self.risk_class.as_str())
            .field("availability", &self.availability.as_str())
            .field("subject", &"[redacted]")
            .field("scope", &"[redacted]")
            .field("permission", &"[redacted]")
            .field("compatibility", &self.compatibility)
            .field("sandbox", &self.sandbox)
            .finish()
    }
}
