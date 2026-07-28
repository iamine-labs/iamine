use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuditEventEnforcementRequirement {
    AuditAuthority,
    TypedProjection,
    BoundedEventSet,
    LifecycleAuthority,
    ExecutionIdentity,
}

impl AuditEventEnforcementRequirement {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AuditAuthority => "audit_authority",
            Self::TypedProjection => "typed_projection",
            Self::BoundedEventSet => "bounded_event_set",
            Self::LifecycleAuthority => "lifecycle_authority",
            Self::ExecutionIdentity => "execution_identity",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuditEventEnforcementErrorCode {
    LifecycleRecordNotVerified,
}

impl AuditEventEnforcementErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LifecycleRecordNotVerified => "lifecycle_record_not_verified",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::LifecycleRecordNotVerified => {
                "execution lifecycle record was not verified by the supplied authority"
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditEventEnforcementError {
    code: AuditEventEnforcementErrorCode,
    requirement: AuditEventEnforcementRequirement,
}

impl AuditEventEnforcementError {
    pub(crate) const fn new(
        code: AuditEventEnforcementErrorCode,
        requirement: AuditEventEnforcementRequirement,
    ) -> Self {
        Self { code, requirement }
    }

    pub const fn code(self) -> AuditEventEnforcementErrorCode {
        self.code
    }

    pub const fn requirement(self) -> AuditEventEnforcementRequirement {
        self.requirement
    }
}

impl fmt::Display for AuditEventEnforcementError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.message())
    }
}

impl std::error::Error for AuditEventEnforcementError {}
