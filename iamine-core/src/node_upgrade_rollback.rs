use crate::release_validation::valid_sha256_hex;
use serde::{Deserialize, Serialize};

pub const MAX_ROLLBACK_ARTIFACTS: usize = 8;
pub const MAX_ROLLBACK_TRUSTED_SIGNING_KEYS: usize = 8;
pub const MAX_ALLOWED_ROLLBACK_VERSIONS: usize = 16;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeUpgradeRollbackMode {
    #[default]
    Disabled,
    ControlledRecovery,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeUpgradeRollbackPolicy {
    pub mode: NodeUpgradeRollbackMode,
    pub trusted_signing_keys: Vec<String>,
    pub allowed_rollback_versions: Vec<String>,
    pub require_operator_confirmation: bool,
}

impl Default for NodeUpgradeRollbackPolicy {
    fn default() -> Self {
        Self {
            mode: NodeUpgradeRollbackMode::Disabled,
            trusted_signing_keys: Vec::new(),
            allowed_rollback_versions: Vec::new(),
            require_operator_confirmation: true,
        }
    }
}

impl NodeUpgradeRollbackPolicy {
    pub fn controlled(
        trusted_signing_keys: Vec<String>,
        allowed_rollback_versions: Vec<String>,
    ) -> Self {
        Self {
            mode: NodeUpgradeRollbackMode::ControlledRecovery,
            trusted_signing_keys,
            allowed_rollback_versions,
            require_operator_confirmation: true,
        }
    }

    pub fn evaluate(
        &self,
        candidate: &NodeUpgradeRollbackCandidate,
    ) -> NodeUpgradeRollbackDecision {
        if self.mode == NodeUpgradeRollbackMode::Disabled {
            return NodeUpgradeRollbackDecision::reject(
                NodeUpgradeRollbackRejectReason::PolicyDisabled,
            );
        }

        if self.trusted_signing_keys.len() > MAX_ROLLBACK_TRUSTED_SIGNING_KEYS {
            return NodeUpgradeRollbackDecision::reject(
                NodeUpgradeRollbackRejectReason::TooManyTrustedSigningKeys,
            );
        }

        if self
            .trusted_signing_keys
            .iter()
            .all(|key| key.trim().is_empty())
        {
            return NodeUpgradeRollbackDecision::reject(
                NodeUpgradeRollbackRejectReason::NoTrustedSigningKeys,
            );
        }

        if self.allowed_rollback_versions.len() > MAX_ALLOWED_ROLLBACK_VERSIONS {
            return NodeUpgradeRollbackDecision::reject(
                NodeUpgradeRollbackRejectReason::TooManyAllowedRollbackVersions,
            );
        }

        if self
            .allowed_rollback_versions
            .iter()
            .all(|version| version.trim().is_empty())
        {
            return NodeUpgradeRollbackDecision::reject(
                NodeUpgradeRollbackRejectReason::NoAllowedRollbackVersions,
            );
        }

        let current_version = candidate.current_version.trim();
        let failed_upgrade_version = candidate.failed_upgrade_version.trim();
        let rollback_version = candidate.rollback_version.trim();

        if current_version.is_empty() {
            return NodeUpgradeRollbackDecision::reject(
                NodeUpgradeRollbackRejectReason::MissingCurrentVersion,
            );
        }

        if failed_upgrade_version.is_empty() {
            return NodeUpgradeRollbackDecision::reject(
                NodeUpgradeRollbackRejectReason::MissingFailedUpgradeVersion,
            );
        }

        if rollback_version.is_empty() {
            return NodeUpgradeRollbackDecision::reject(
                NodeUpgradeRollbackRejectReason::MissingRollbackVersion,
            );
        }

        if current_version != failed_upgrade_version {
            return NodeUpgradeRollbackDecision::reject(
                NodeUpgradeRollbackRejectReason::CurrentVersionMismatchFailedUpgrade,
            );
        }

        if current_version == rollback_version {
            return NodeUpgradeRollbackDecision::reject(
                NodeUpgradeRollbackRejectReason::RollbackVersionMatchesCurrent,
            );
        }

        if !self.is_allowed_rollback_version(rollback_version) {
            return NodeUpgradeRollbackDecision::reject(
                NodeUpgradeRollbackRejectReason::RollbackVersionNotAllowed,
            );
        }

        match candidate.upgrade_state {
            NodeUpgradeState::Failed | NodeUpgradeState::Incompatible => {}
            NodeUpgradeState::Healthy => {
                return NodeUpgradeRollbackDecision::reject(
                    NodeUpgradeRollbackRejectReason::UpgradeNotFailed,
                );
            }
            NodeUpgradeState::Unknown => {
                return NodeUpgradeRollbackDecision::reject(
                    NodeUpgradeRollbackRejectReason::UpgradeStateUnknown,
                );
            }
        }

        if self.require_operator_confirmation && !candidate.operator_confirmed {
            return NodeUpgradeRollbackDecision::reject(
                NodeUpgradeRollbackRejectReason::OperatorConfirmationRequired,
            );
        }

        if !candidate.active_tasks_drained {
            return NodeUpgradeRollbackDecision::reject(
                NodeUpgradeRollbackRejectReason::ActiveTasksNotDrained,
            );
        }

        if !candidate.pre_upgrade_snapshot_available {
            return NodeUpgradeRollbackDecision::reject(
                NodeUpgradeRollbackRejectReason::MissingPreUpgradeSnapshot,
            );
        }

        if !candidate.config_backup_available {
            return NodeUpgradeRollbackDecision::reject(
                NodeUpgradeRollbackRejectReason::MissingConfigBackup,
            );
        }

        if candidate.artifacts.is_empty() {
            return NodeUpgradeRollbackDecision::reject(
                NodeUpgradeRollbackRejectReason::NoRollbackArtifacts,
            );
        }

        if candidate.artifacts.len() > MAX_ROLLBACK_ARTIFACTS {
            return NodeUpgradeRollbackDecision::reject(
                NodeUpgradeRollbackRejectReason::TooManyRollbackArtifacts,
            );
        }

        let mut restorable_artifact_ids = Vec::new();
        for artifact in &candidate.artifacts {
            if let Some(reason) = self.artifact_rejection_reason(rollback_version, artifact) {
                return NodeUpgradeRollbackDecision::reject(reason);
            }
            if artifact.artifact_kind.is_restorable() {
                restorable_artifact_ids.push(artifact.artifact_id.clone());
            }
        }

        if restorable_artifact_ids.is_empty() {
            return NodeUpgradeRollbackDecision::reject(
                NodeUpgradeRollbackRejectReason::NoRestorableRollbackArtifact,
            );
        }

        NodeUpgradeRollbackDecision::accept(NodeUpgradeRollbackPlan {
            rollback_version: rollback_version.to_string(),
            restorable_artifact_ids,
        })
    }

    fn artifact_rejection_reason(
        &self,
        rollback_version: &str,
        artifact: &NodeRollbackArtifact,
    ) -> Option<NodeUpgradeRollbackRejectReason> {
        if artifact.artifact_id.trim().is_empty() {
            return Some(NodeUpgradeRollbackRejectReason::MissingArtifactId);
        }

        let artifact_version = artifact.release_version.trim();
        if artifact_version.is_empty() {
            return Some(NodeUpgradeRollbackRejectReason::MissingArtifactVersion);
        }

        if artifact_version != rollback_version {
            return Some(NodeUpgradeRollbackRejectReason::ArtifactVersionMismatch);
        }

        if !valid_sha256_hex(&artifact.digest_sha256_hex) {
            return Some(NodeUpgradeRollbackRejectReason::ArtifactDigestInvalid);
        }

        match artifact.signature.status {
            NodeRollbackSignatureStatus::Verified => {
                if self.is_trusted_signing_key(&artifact.signature.signing_key_id) {
                    None
                } else {
                    Some(NodeUpgradeRollbackRejectReason::UntrustedSigningKey)
                }
            }
            NodeRollbackSignatureStatus::Missing => {
                Some(NodeUpgradeRollbackRejectReason::UnsignedArtifact)
            }
            NodeRollbackSignatureStatus::Invalid => {
                Some(NodeUpgradeRollbackRejectReason::ArtifactSignatureInvalid)
            }
            NodeRollbackSignatureStatus::UntrustedKey => {
                Some(NodeUpgradeRollbackRejectReason::UntrustedSigningKey)
            }
        }
    }

    fn is_trusted_signing_key(&self, signing_key_id: &str) -> bool {
        let signing_key_id = signing_key_id.trim();
        !signing_key_id.is_empty()
            && self
                .trusted_signing_keys
                .iter()
                .any(|trusted| trusted.trim() == signing_key_id)
    }

    fn is_allowed_rollback_version(&self, rollback_version: &str) -> bool {
        let rollback_version = rollback_version.trim();
        !rollback_version.is_empty()
            && self
                .allowed_rollback_versions
                .iter()
                .any(|allowed| allowed.trim() == rollback_version)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeUpgradeRollbackCandidate {
    pub current_version: String,
    pub failed_upgrade_version: String,
    pub rollback_version: String,
    pub upgrade_state: NodeUpgradeState,
    pub operator_confirmed: bool,
    pub active_tasks_drained: bool,
    pub pre_upgrade_snapshot_available: bool,
    pub config_backup_available: bool,
    pub artifacts: Vec<NodeRollbackArtifact>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeUpgradeState {
    Healthy,
    Failed,
    Incompatible,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeRollbackArtifact {
    pub artifact_id: String,
    pub artifact_kind: NodeRollbackArtifactKind,
    pub release_version: String,
    pub digest_sha256_hex: String,
    pub signature: NodeRollbackSignatureVerification,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeRollbackArtifactKind {
    NodeBinary,
    PackageArchive,
    Installer,
    Manifest,
    Other(String),
}

impl NodeRollbackArtifactKind {
    fn is_restorable(&self) -> bool {
        matches!(
            self,
            Self::NodeBinary | Self::PackageArchive | Self::Installer
        )
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeRollbackSignatureVerification {
    pub signing_key_id: String,
    pub status: NodeRollbackSignatureStatus,
}

impl NodeRollbackSignatureVerification {
    pub fn verified(signing_key_id: impl Into<String>) -> Self {
        Self {
            signing_key_id: signing_key_id.into(),
            status: NodeRollbackSignatureStatus::Verified,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeRollbackSignatureStatus {
    Verified,
    Missing,
    Invalid,
    UntrustedKey,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeUpgradeRollbackDecision {
    pub accepted: bool,
    pub reason: NodeUpgradeRollbackDecisionReason,
    pub plan: Option<NodeUpgradeRollbackPlan>,
}

impl NodeUpgradeRollbackDecision {
    pub fn accept(plan: NodeUpgradeRollbackPlan) -> Self {
        Self {
            accepted: true,
            reason: NodeUpgradeRollbackDecisionReason::Accepted,
            plan: Some(plan),
        }
    }

    pub fn reject(reason: NodeUpgradeRollbackRejectReason) -> Self {
        Self {
            accepted: false,
            reason: NodeUpgradeRollbackDecisionReason::Rejected(reason),
            plan: None,
        }
    }

    pub fn reason_code(&self) -> &'static str {
        self.reason.as_str()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NodeUpgradeRollbackPlan {
    pub rollback_version: String,
    pub restorable_artifact_ids: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeUpgradeRollbackDecisionReason {
    Accepted,
    Rejected(NodeUpgradeRollbackRejectReason),
}

impl NodeUpgradeRollbackDecisionReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected(reason) => reason.as_str(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum NodeUpgradeRollbackRejectReason {
    PolicyDisabled,
    NoTrustedSigningKeys,
    TooManyTrustedSigningKeys,
    NoAllowedRollbackVersions,
    TooManyAllowedRollbackVersions,
    MissingCurrentVersion,
    MissingFailedUpgradeVersion,
    MissingRollbackVersion,
    CurrentVersionMismatchFailedUpgrade,
    RollbackVersionMatchesCurrent,
    RollbackVersionNotAllowed,
    UpgradeNotFailed,
    UpgradeStateUnknown,
    OperatorConfirmationRequired,
    ActiveTasksNotDrained,
    MissingPreUpgradeSnapshot,
    MissingConfigBackup,
    NoRollbackArtifacts,
    TooManyRollbackArtifacts,
    MissingArtifactId,
    MissingArtifactVersion,
    ArtifactVersionMismatch,
    ArtifactDigestInvalid,
    UnsignedArtifact,
    ArtifactSignatureInvalid,
    UntrustedSigningKey,
    NoRestorableRollbackArtifact,
}

impl NodeUpgradeRollbackRejectReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PolicyDisabled => "policy_disabled",
            Self::NoTrustedSigningKeys => "no_trusted_signing_keys",
            Self::TooManyTrustedSigningKeys => "too_many_trusted_signing_keys",
            Self::NoAllowedRollbackVersions => "no_allowed_rollback_versions",
            Self::TooManyAllowedRollbackVersions => "too_many_allowed_rollback_versions",
            Self::MissingCurrentVersion => "missing_current_version",
            Self::MissingFailedUpgradeVersion => "missing_failed_upgrade_version",
            Self::MissingRollbackVersion => "missing_rollback_version",
            Self::CurrentVersionMismatchFailedUpgrade => "current_version_mismatch_failed_upgrade",
            Self::RollbackVersionMatchesCurrent => "rollback_version_matches_current",
            Self::RollbackVersionNotAllowed => "rollback_version_not_allowed",
            Self::UpgradeNotFailed => "upgrade_not_failed",
            Self::UpgradeStateUnknown => "upgrade_state_unknown",
            Self::OperatorConfirmationRequired => "operator_confirmation_required",
            Self::ActiveTasksNotDrained => "active_tasks_not_drained",
            Self::MissingPreUpgradeSnapshot => "missing_pre_upgrade_snapshot",
            Self::MissingConfigBackup => "missing_config_backup",
            Self::NoRollbackArtifacts => "no_rollback_artifacts",
            Self::TooManyRollbackArtifacts => "too_many_rollback_artifacts",
            Self::MissingArtifactId => "missing_artifact_id",
            Self::MissingArtifactVersion => "missing_artifact_version",
            Self::ArtifactVersionMismatch => "artifact_version_mismatch",
            Self::ArtifactDigestInvalid => "artifact_digest_invalid",
            Self::UnsignedArtifact => "unsigned_artifact",
            Self::ArtifactSignatureInvalid => "artifact_signature_invalid",
            Self::UntrustedSigningKey => "untrusted_signing_key",
            Self::NoRestorableRollbackArtifact => "no_restorable_rollback_artifact",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TRUSTED_KEY: &str = "iamine-release-key-v1";
    const CURRENT_VERSION: &str = "0.10.1";
    const FAILED_VERSION: &str = "0.10.1";
    const ROLLBACK_VERSION: &str = "0.10.0";

    fn digest() -> String {
        "a".repeat(64)
    }

    fn policy() -> NodeUpgradeRollbackPolicy {
        NodeUpgradeRollbackPolicy::controlled(
            vec![TRUSTED_KEY.to_string()],
            vec![ROLLBACK_VERSION.to_string()],
        )
    }

    fn artifact(kind: NodeRollbackArtifactKind) -> NodeRollbackArtifact {
        NodeRollbackArtifact {
            artifact_id: "iamine-node-darwin-arm64-prev".to_string(),
            artifact_kind: kind,
            release_version: ROLLBACK_VERSION.to_string(),
            digest_sha256_hex: digest(),
            signature: NodeRollbackSignatureVerification::verified(TRUSTED_KEY),
        }
    }

    fn candidate() -> NodeUpgradeRollbackCandidate {
        NodeUpgradeRollbackCandidate {
            current_version: CURRENT_VERSION.to_string(),
            failed_upgrade_version: FAILED_VERSION.to_string(),
            rollback_version: ROLLBACK_VERSION.to_string(),
            upgrade_state: NodeUpgradeState::Failed,
            operator_confirmed: true,
            active_tasks_drained: true,
            pre_upgrade_snapshot_available: true,
            config_backup_available: true,
            artifacts: vec![artifact(NodeRollbackArtifactKind::PackageArchive)],
        }
    }

    #[test]
    fn default_policy_rejects_rollback() {
        let decision = NodeUpgradeRollbackPolicy::default().evaluate(&candidate());

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "policy_disabled");
        assert!(decision.plan.is_none());
    }

    #[test]
    fn controlled_policy_accepts_failed_upgrade_with_verified_artifact() {
        let decision = policy().evaluate(&candidate());

        assert!(decision.accepted);
        assert_eq!(decision.reason_code(), "accepted");
        let plan = decision
            .plan
            .expect("accepted rollback should include a plan");
        assert_eq!(plan.rollback_version, ROLLBACK_VERSION);
        assert_eq!(
            plan.restorable_artifact_ids,
            vec!["iamine-node-darwin-arm64-prev".to_string()]
        );
    }

    #[test]
    fn controlled_policy_accepts_incompatible_upgrade_state() {
        let mut candidate = candidate();
        candidate.upgrade_state = NodeUpgradeState::Incompatible;

        let decision = policy().evaluate(&candidate);

        assert!(decision.accepted);
    }

    #[test]
    fn controlled_policy_rejects_healthy_upgrade() {
        let mut candidate = candidate();
        candidate.upgrade_state = NodeUpgradeState::Healthy;

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "upgrade_not_failed");
    }

    #[test]
    fn controlled_policy_rejects_unknown_upgrade_state() {
        let mut candidate = candidate();
        candidate.upgrade_state = NodeUpgradeState::Unknown;

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "upgrade_state_unknown");
    }

    #[test]
    fn controlled_policy_rejects_missing_operator_confirmation() {
        let mut candidate = candidate();
        candidate.operator_confirmed = false;

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "operator_confirmation_required");
    }

    #[test]
    fn controlled_policy_rejects_active_tasks_not_drained() {
        let mut candidate = candidate();
        candidate.active_tasks_drained = false;

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "active_tasks_not_drained");
    }

    #[test]
    fn controlled_policy_rejects_missing_pre_upgrade_snapshot() {
        let mut candidate = candidate();
        candidate.pre_upgrade_snapshot_available = false;

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "missing_pre_upgrade_snapshot");
    }

    #[test]
    fn controlled_policy_rejects_missing_config_backup() {
        let mut candidate = candidate();
        candidate.config_backup_available = false;

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "missing_config_backup");
    }

    #[test]
    fn controlled_policy_rejects_disallowed_rollback_version() {
        let mut candidate = candidate();
        candidate.rollback_version = "0.9.9".to_string();
        candidate.artifacts[0].release_version = "0.9.9".to_string();

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "rollback_version_not_allowed");
    }

    #[test]
    fn controlled_policy_rejects_rollback_to_current_version() {
        let mut candidate = candidate();
        candidate.rollback_version = CURRENT_VERSION.to_string();
        candidate.artifacts[0].release_version = CURRENT_VERSION.to_string();

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "rollback_version_matches_current");
    }

    #[test]
    fn controlled_policy_rejects_current_version_mismatch_failed_upgrade() {
        let mut candidate = candidate();
        candidate.failed_upgrade_version = "0.10.2".to_string();

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(
            decision.reason_code(),
            "current_version_mismatch_failed_upgrade"
        );
    }

    #[test]
    fn controlled_policy_rejects_unsigned_artifact() {
        let mut candidate = candidate();
        candidate.artifacts[0].signature.status = NodeRollbackSignatureStatus::Missing;

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "unsigned_artifact");
    }

    #[test]
    fn controlled_policy_rejects_untrusted_signing_key() {
        let mut candidate = candidate();
        candidate.artifacts[0].signature =
            NodeRollbackSignatureVerification::verified("other-release-key");

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "untrusted_signing_key");
    }

    #[test]
    fn controlled_policy_rejects_invalid_artifact_digest() {
        let mut candidate = candidate();
        candidate.artifacts[0].digest_sha256_hex = "not-a-sha256".to_string();

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "artifact_digest_invalid");
    }

    #[test]
    fn controlled_policy_rejects_artifact_version_mismatch() {
        let mut candidate = candidate();
        candidate.artifacts[0].release_version = "0.9.9".to_string();

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "artifact_version_mismatch");
    }

    #[test]
    fn controlled_policy_rejects_manifest_only_rollback() {
        let mut candidate = candidate();
        candidate.artifacts = vec![artifact(NodeRollbackArtifactKind::Manifest)];

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "no_restorable_rollback_artifact");
    }

    #[test]
    fn controlled_policy_rejects_too_many_artifacts() {
        let mut candidate = candidate();
        candidate.artifacts = (0..=MAX_ROLLBACK_ARTIFACTS)
            .map(|idx| {
                let mut artifact = artifact(NodeRollbackArtifactKind::PackageArchive);
                artifact.artifact_id = format!("iamine-node-prev-{idx}");
                artifact
            })
            .collect();

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "too_many_rollback_artifacts");
    }

    #[test]
    fn controlled_policy_rejects_unbounded_policy_lists() {
        let keys = (0..=MAX_ROLLBACK_TRUSTED_SIGNING_KEYS)
            .map(|idx| format!("iamine-release-key-{idx}"))
            .collect();
        let policy =
            NodeUpgradeRollbackPolicy::controlled(keys, vec![ROLLBACK_VERSION.to_string()]);

        let decision = policy.evaluate(&candidate());

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "too_many_trusted_signing_keys");
    }

    #[test]
    fn controlled_policy_rejects_unbounded_allowed_versions() {
        let versions = (0..=MAX_ALLOWED_ROLLBACK_VERSIONS)
            .map(|idx| format!("0.10.{idx}"))
            .collect();
        let policy = NodeUpgradeRollbackPolicy::controlled(vec![TRUSTED_KEY.to_string()], versions);

        let decision = policy.evaluate(&candidate());

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "too_many_allowed_rollback_versions");
    }
}
