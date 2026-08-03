pub mod errors;
pub mod interface_contracts;
pub mod message;
pub mod node;
pub mod node_upgrade_rollback;
mod release_validation;
pub mod result;
pub mod signed_autoupdate;
pub mod supply_chain_security;
pub mod task;

pub use errors::{IaMineError, IaMineResult};
pub use interface_contracts::{
    InterfaceContractError, InterfaceEvent, InterfaceEventIdentity, InterfaceEventPayload,
    InterfaceEventStream, InterfaceEvidenceScope, InterfaceOperation, InterfaceOperationClass,
    InterfaceOperationId, InterfaceOperatorAction, InterfaceOutcome, InterfaceOutcomeStatus,
    InterfaceProblem, InterfaceProblemCode, InterfaceProvenance, InterfaceProvenanceSource,
    InterfaceRedaction, InterfaceRequest, InterfaceResponse, InterfaceSchemaVersion,
    InterfaceWarning, InterfaceWarningCode, InterfaceWarnings, INTERFACE_CONTRACT_SCHEMA_VERSION,
    MAX_INTERFACE_WARNINGS,
};
pub use message::IaMineMessage;
pub use node::{NodeCapabilities, NodeReputation};
pub use node_upgrade_rollback::{
    NodeRollbackArtifact, NodeRollbackArtifactKind, NodeRollbackSignatureStatus,
    NodeRollbackSignatureVerification, NodeUpgradeRollbackCandidate, NodeUpgradeRollbackDecision,
    NodeUpgradeRollbackDecisionReason, NodeUpgradeRollbackMode, NodeUpgradeRollbackPlan,
    NodeUpgradeRollbackPolicy, NodeUpgradeRollbackRejectReason, NodeUpgradeState,
    MAX_ALLOWED_ROLLBACK_VERSIONS, MAX_ROLLBACK_ARTIFACTS, MAX_ROLLBACK_TRUSTED_SIGNING_KEYS,
};
pub use result::TaskResult;
pub use signed_autoupdate::{
    SignatureVerification, SignatureVerificationStatus, SignedAutoupdateCandidate,
    SignedAutoupdateDecision, SignedAutoupdateDecisionReason, SignedAutoupdateMode,
    SignedAutoupdatePolicy, SignedAutoupdateRejectReason, SignedReleaseArtifact,
    SignedReleaseArtifactKind, DEFAULT_MAX_ROLLOUT_PERCENT, MAX_RELEASE_ARTIFACTS,
    MAX_TRUSTED_SIGNING_KEYS,
};
pub use supply_chain_security::{
    SupplyChainArtifact, SupplyChainArtifactKind, SupplyChainBuildEvidence, SupplyChainCheckStatus,
    SupplyChainDecision, SupplyChainDecisionReason, SupplyChainDependencyEvidence,
    SupplyChainProvenanceVerification, SupplyChainRejectReason, SupplyChainReleaseCandidate,
    SupplyChainSecurityMode, SupplyChainSecurityPolicy, SupplyChainSourceEvidence,
    MAX_SUPPLY_CHAIN_ARTIFACTS, MAX_TRUSTED_BUILDERS,
};
pub use task::{Task, TaskStatus, TaskType};

pub fn sha256_hex(data: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::new().chain_update(data).finalize())
}

pub fn sha256_bytes(data: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    Sha256::new().chain_update(data).finalize().into()
}
