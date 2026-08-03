use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const INTERFACE_CONTRACT_SCHEMA_VERSION: &str = "1.0.0";
pub const MAX_INTERFACE_WARNINGS: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum InterfaceContractError {
    #[error("interface contract schema is unsupported")]
    UnsupportedSchema,
    #[error("interface operation class does not match its operation")]
    OperationClassMismatch,
    #[error("interface warning limit exceeded")]
    TooManyWarnings,
    #[error("mock provenance cannot be authoritative")]
    MockCannotBeAuthoritative,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct InterfaceSchemaVersion(String);

impl InterfaceSchemaVersion {
    pub fn current() -> Self {
        Self(INTERFACE_CONTRACT_SCHEMA_VERSION.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for InterfaceSchemaVersion {
    type Error = InterfaceContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value == INTERFACE_CONTRACT_SCHEMA_VERSION {
            Ok(Self(value))
        } else {
            Err(InterfaceContractError::UnsupportedSchema)
        }
    }
}

impl From<InterfaceSchemaVersion> for String {
    fn from(value: InterfaceSchemaVersion) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceOperationClass {
    ReadOnlyDiagnostic,
    ReadOnlyOperational,
    PlannedMutation,
    RuntimeMutation,
    AgentOperation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceOperationId {
    NodeEvidenceRead,
    HardwareProfileRead,
    NodeConfigStatusRead,
    NodeIdentityStatusRead,
    ClusterStatusRead,
    TaskStatsRead,
    TaskTraceRead,
    ModelCatalogRead,
    SupportBundlePlanRead,
    NodeConfigMigrationPlan,
    NodeConfigRollbackPlan,
    IdentityInitializationPlan,
    HardwareRefreshPlan,
    WorkerLifecycle,
    AgentPermission,
    AgentExecution,
    AgentCancellation,
}

impl InterfaceOperationId {
    pub fn class(self) -> InterfaceOperationClass {
        match self {
            Self::NodeEvidenceRead
            | Self::HardwareProfileRead
            | Self::NodeConfigStatusRead
            | Self::NodeIdentityStatusRead => InterfaceOperationClass::ReadOnlyDiagnostic,
            Self::ClusterStatusRead
            | Self::TaskStatsRead
            | Self::TaskTraceRead
            | Self::ModelCatalogRead => InterfaceOperationClass::ReadOnlyOperational,
            Self::SupportBundlePlanRead
            | Self::NodeConfigMigrationPlan
            | Self::NodeConfigRollbackPlan
            | Self::IdentityInitializationPlan
            | Self::HardwareRefreshPlan => InterfaceOperationClass::PlannedMutation,
            Self::WorkerLifecycle => InterfaceOperationClass::RuntimeMutation,
            Self::AgentPermission | Self::AgentExecution | Self::AgentCancellation => {
                InterfaceOperationClass::AgentOperation
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawInterfaceOperation", into = "RawInterfaceOperation")]
pub struct InterfaceOperation {
    id: InterfaceOperationId,
    class: InterfaceOperationClass,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct RawInterfaceOperation {
    id: InterfaceOperationId,
    class: InterfaceOperationClass,
}

impl InterfaceOperation {
    pub fn new(id: InterfaceOperationId) -> Self {
        Self {
            id,
            class: id.class(),
        }
    }

    pub fn id(self) -> InterfaceOperationId {
        self.id
    }

    pub fn class(self) -> InterfaceOperationClass {
        self.class
    }
}

impl TryFrom<RawInterfaceOperation> for InterfaceOperation {
    type Error = InterfaceContractError;

    fn try_from(value: RawInterfaceOperation) -> Result<Self, Self::Error> {
        if value.id.class() != value.class {
            return Err(InterfaceContractError::OperationClassMismatch);
        }
        Ok(Self {
            id: value.id,
            class: value.class,
        })
    }
}

impl From<InterfaceOperation> for RawInterfaceOperation {
    fn from(value: InterfaceOperation) -> Self {
        Self {
            id: value.id,
            class: value.class,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InterfaceRequest<T> {
    pub schema_version: InterfaceSchemaVersion,
    pub operation: InterfaceOperation,
    pub payload: T,
}

impl<T> InterfaceRequest<T> {
    pub fn new(operation: InterfaceOperation, payload: T) -> Self {
        Self {
            schema_version: InterfaceSchemaVersion::current(),
            operation,
            payload,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceOutcomeStatus {
    Success,
    Attention,
    Blocked,
    Unavailable,
    Stale,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceProblemCode {
    InvalidRequest,
    UnsupportedSchema,
    MalformedPayload,
    PermissionRequired,
    PolicyBlocked,
    OwnerUnavailable,
    EvidenceStale,
    EvidenceUnknown,
    InternalFailure,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceOperatorAction {
    None,
    Retry,
    ReviewOwnerEvidence,
    AuthenticateLocally,
    RequestAuthorization,
    ContactSupport,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InterfaceProblem {
    pub code: InterfaceProblemCode,
    pub operator_action: InterfaceOperatorAction,
}

impl InterfaceProblem {
    pub const fn new(code: InterfaceProblemCode, operator_action: InterfaceOperatorAction) -> Self {
        Self {
            code,
            operator_action,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceWarningCode {
    PartialEvidence,
    RedactedField,
    FallbackObservation,
    DeprecatedField,
    FreshnessBoundary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InterfaceWarning {
    pub code: InterfaceWarningCode,
    pub operator_action: InterfaceOperatorAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Vec<InterfaceWarning>", into = "Vec<InterfaceWarning>")]
pub struct InterfaceWarnings(Vec<InterfaceWarning>);

impl InterfaceWarnings {
    pub fn empty() -> Self {
        Self(Vec::new())
    }

    pub fn try_from_items(items: Vec<InterfaceWarning>) -> Result<Self, InterfaceContractError> {
        if items.len() > MAX_INTERFACE_WARNINGS {
            return Err(InterfaceContractError::TooManyWarnings);
        }
        Ok(Self(items))
    }

    pub fn as_slice(&self) -> &[InterfaceWarning] {
        &self.0
    }
}

impl TryFrom<Vec<InterfaceWarning>> for InterfaceWarnings {
    type Error = InterfaceContractError;

    fn try_from(value: Vec<InterfaceWarning>) -> Result<Self, Self::Error> {
        Self::try_from_items(value)
    }
}

impl From<InterfaceWarnings> for Vec<InterfaceWarning> {
    fn from(value: InterfaceWarnings) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceProvenanceSource {
    OwnerModule,
    MockFixture,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceEvidenceScope {
    CurrentSnapshot,
    PointInTime,
    PlannedOperation,
    NoEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceRedaction {
    Applied,
    NotRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawInterfaceProvenance", into = "RawInterfaceProvenance")]
pub struct InterfaceProvenance {
    source: InterfaceProvenanceSource,
    evidence_scope: InterfaceEvidenceScope,
    redaction: InterfaceRedaction,
    authoritative: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct RawInterfaceProvenance {
    source: InterfaceProvenanceSource,
    evidence_scope: InterfaceEvidenceScope,
    redaction: InterfaceRedaction,
    authoritative: bool,
}

impl InterfaceProvenance {
    pub fn owner(evidence_scope: InterfaceEvidenceScope, redaction: InterfaceRedaction) -> Self {
        Self {
            source: InterfaceProvenanceSource::OwnerModule,
            evidence_scope,
            redaction,
            authoritative: true,
        }
    }

    pub fn mock(evidence_scope: InterfaceEvidenceScope) -> Self {
        Self {
            source: InterfaceProvenanceSource::MockFixture,
            evidence_scope,
            redaction: InterfaceRedaction::Applied,
            authoritative: false,
        }
    }

    pub fn source(self) -> InterfaceProvenanceSource {
        self.source
    }

    pub fn evidence_scope(self) -> InterfaceEvidenceScope {
        self.evidence_scope
    }

    pub fn redaction(self) -> InterfaceRedaction {
        self.redaction
    }

    pub fn is_authoritative(self) -> bool {
        self.authoritative
    }
}

impl TryFrom<RawInterfaceProvenance> for InterfaceProvenance {
    type Error = InterfaceContractError;

    fn try_from(value: RawInterfaceProvenance) -> Result<Self, Self::Error> {
        if value.source == InterfaceProvenanceSource::MockFixture && value.authoritative {
            return Err(InterfaceContractError::MockCannotBeAuthoritative);
        }
        Ok(Self {
            source: value.source,
            evidence_scope: value.evidence_scope,
            redaction: value.redaction,
            authoritative: value.authoritative,
        })
    }
}

impl From<InterfaceProvenance> for RawInterfaceProvenance {
    fn from(value: InterfaceProvenance) -> Self {
        Self {
            source: value.source,
            evidence_scope: value.evidence_scope,
            redaction: value.redaction,
            authoritative: value.authoritative,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceOutcome<T> {
    Success {
        data: T,
        provenance: InterfaceProvenance,
        warnings: InterfaceWarnings,
    },
    Attention {
        data: T,
        provenance: InterfaceProvenance,
        warnings: InterfaceWarnings,
    },
    Blocked {
        problem: InterfaceProblem,
        provenance: InterfaceProvenance,
        warnings: InterfaceWarnings,
    },
    Unavailable {
        problem: InterfaceProblem,
        provenance: InterfaceProvenance,
        warnings: InterfaceWarnings,
    },
    Stale {
        data: T,
        provenance: InterfaceProvenance,
        warnings: InterfaceWarnings,
    },
    Unknown {
        problem: InterfaceProblem,
        provenance: InterfaceProvenance,
        warnings: InterfaceWarnings,
    },
}

impl<T> InterfaceOutcome<T> {
    pub fn status(&self) -> InterfaceOutcomeStatus {
        match self {
            Self::Success { .. } => InterfaceOutcomeStatus::Success,
            Self::Attention { .. } => InterfaceOutcomeStatus::Attention,
            Self::Blocked { .. } => InterfaceOutcomeStatus::Blocked,
            Self::Unavailable { .. } => InterfaceOutcomeStatus::Unavailable,
            Self::Stale { .. } => InterfaceOutcomeStatus::Stale,
            Self::Unknown { .. } => InterfaceOutcomeStatus::Unknown,
        }
    }

    pub fn data(&self) -> Option<&T> {
        match self {
            Self::Success { data, .. }
            | Self::Attention { data, .. }
            | Self::Stale { data, .. } => Some(data),
            Self::Blocked { .. } | Self::Unavailable { .. } | Self::Unknown { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct InterfaceResponse<T> {
    pub schema_version: InterfaceSchemaVersion,
    pub operation: InterfaceOperation,
    pub outcome: InterfaceOutcome<T>,
}

impl<T> InterfaceResponse<T> {
    pub fn new(operation: InterfaceOperation, outcome: InterfaceOutcome<T>) -> Self {
        Self {
            schema_version: InterfaceSchemaVersion::current(),
            operation,
            outcome,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceEventStream {
    NodeState,
    OperationLifecycle,
    Audit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct InterfaceEventIdentity {
    pub stream: InterfaceEventStream,
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InterfaceEventPayload {
    SnapshotReconciled {
        operation: InterfaceOperation,
    },
    OperationStarted {
        operation: InterfaceOperation,
    },
    OperationFinished {
        operation: InterfaceOperation,
        status: InterfaceOutcomeStatus,
    },
    OperationRejected {
        operation: InterfaceOperation,
        problem: InterfaceProblem,
    },
    PermissionRequested {
        operation: InterfaceOperation,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InterfaceEvent {
    pub schema_version: InterfaceSchemaVersion,
    pub identity: InterfaceEventIdentity,
    pub payload: InterfaceEventPayload,
}

impl InterfaceEvent {
    pub fn new(
        stream: InterfaceEventStream,
        sequence: u64,
        payload: InterfaceEventPayload,
    ) -> Self {
        Self {
            schema_version: InterfaceSchemaVersion::current(),
            identity: InterfaceEventIdentity { stream, sequence },
            payload,
        }
    }

    pub fn authorizes_action(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn read_operation() -> InterfaceOperation {
        InterfaceOperation::new(InterfaceOperationId::NodeEvidenceRead)
    }

    #[test]
    fn request_and_response_use_the_current_schema() {
        let request = InterfaceRequest::new(read_operation(), ());
        let response = InterfaceResponse::new(
            read_operation(),
            InterfaceOutcome::Success {
                data: "evidence",
                provenance: InterfaceProvenance::owner(
                    InterfaceEvidenceScope::CurrentSnapshot,
                    InterfaceRedaction::Applied,
                ),
                warnings: InterfaceWarnings::empty(),
            },
        );

        let request_json = serde_json::to_value(request).unwrap();
        let response_json = serde_json::to_value(response).unwrap();
        assert_eq!(
            request_json["schema_version"],
            INTERFACE_CONTRACT_SCHEMA_VERSION
        );
        assert_eq!(
            response_json["schema_version"],
            INTERFACE_CONTRACT_SCHEMA_VERSION
        );
    }

    #[test]
    fn incompatible_schema_is_rejected_during_deserialization() {
        let payload = json!({
            "schema_version": "9.0.0",
            "operation": {
                "id": "node_evidence_read",
                "class": "read_only_diagnostic"
            },
            "payload": null
        });

        assert!(serde_json::from_value::<InterfaceRequest<()>>(payload).is_err());
    }

    #[test]
    fn operation_class_mismatch_is_rejected() {
        let payload = json!({
            "id": "node_evidence_read",
            "class": "runtime_mutation"
        });

        assert!(serde_json::from_value::<InterfaceOperation>(payload).is_err());
    }

    #[test]
    fn warnings_are_bounded() {
        let warnings = vec![
            InterfaceWarning {
                code: InterfaceWarningCode::PartialEvidence,
                operator_action: InterfaceOperatorAction::ReviewOwnerEvidence,
            };
            MAX_INTERFACE_WARNINGS + 1
        ];

        assert_eq!(
            InterfaceWarnings::try_from_items(warnings),
            Err(InterfaceContractError::TooManyWarnings)
        );
    }

    #[test]
    fn blocked_outcome_has_no_data_even_with_warnings() {
        let outcome = InterfaceOutcome::<String>::Blocked {
            problem: InterfaceProblem::new(
                InterfaceProblemCode::PolicyBlocked,
                InterfaceOperatorAction::RequestAuthorization,
            ),
            provenance: InterfaceProvenance::owner(
                InterfaceEvidenceScope::NoEvidence,
                InterfaceRedaction::NotRequired,
            ),
            warnings: InterfaceWarnings::empty(),
        };

        assert_eq!(outcome.status(), InterfaceOutcomeStatus::Blocked);
        assert!(outcome.data().is_none());
    }

    #[test]
    fn mock_provenance_is_explicitly_non_authoritative() {
        let provenance = InterfaceProvenance::mock(InterfaceEvidenceScope::CurrentSnapshot);
        assert_eq!(provenance.source(), InterfaceProvenanceSource::MockFixture);
        assert!(!provenance.is_authoritative());

        let payload = json!({
            "source": "mock_fixture",
            "evidence_scope": "current_snapshot",
            "redaction": "applied",
            "authoritative": true
        });
        assert!(serde_json::from_value::<InterfaceProvenance>(payload).is_err());
    }

    #[test]
    fn events_are_ordered_and_cannot_authorize_actions() {
        let event = InterfaceEvent::new(
            InterfaceEventStream::OperationLifecycle,
            7,
            InterfaceEventPayload::PermissionRequested {
                operation: InterfaceOperation::new(InterfaceOperationId::AgentPermission),
            },
        );

        assert_eq!(event.identity.sequence, 7);
        assert!(!event.authorizes_action());
        let serialized = serde_json::to_string(&event).unwrap();
        assert!(!serialized.contains("authorize"));
    }
}
