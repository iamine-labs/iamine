pub mod errors;
pub mod message;
pub mod node;
pub mod result;
pub mod signed_autoupdate;
pub mod supply_chain_security;
pub mod task;

pub use errors::{IaMineError, IaMineResult};
pub use message::IaMineMessage;
pub use node::{NodeCapabilities, NodeReputation};
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
