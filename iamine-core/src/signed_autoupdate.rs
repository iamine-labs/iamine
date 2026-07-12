use crate::release_validation::valid_sha256_hex;
use serde::{Deserialize, Serialize};

pub const DEFAULT_MAX_ROLLOUT_PERCENT: u8 = 10;
pub const MAX_RELEASE_ARTIFACTS: usize = 16;
pub const MAX_TRUSTED_SIGNING_KEYS: usize = 8;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum SignedAutoupdateMode {
    #[default]
    Disabled,
    ControlledRollout,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedAutoupdatePolicy {
    pub mode: SignedAutoupdateMode,
    pub trusted_signing_keys: Vec<String>,
    pub max_rollout_percent: u8,
}

impl Default for SignedAutoupdatePolicy {
    fn default() -> Self {
        Self {
            mode: SignedAutoupdateMode::Disabled,
            trusted_signing_keys: Vec::new(),
            max_rollout_percent: DEFAULT_MAX_ROLLOUT_PERCENT,
        }
    }
}

impl SignedAutoupdatePolicy {
    pub fn controlled(trusted_signing_keys: Vec<String>, max_rollout_percent: u8) -> Self {
        Self {
            mode: SignedAutoupdateMode::ControlledRollout,
            trusted_signing_keys,
            max_rollout_percent,
        }
    }

    pub fn evaluate(&self, candidate: &SignedAutoupdateCandidate) -> SignedAutoupdateDecision {
        if self.mode == SignedAutoupdateMode::Disabled {
            return SignedAutoupdateDecision::reject(SignedAutoupdateRejectReason::PolicyDisabled);
        }

        if !valid_rollout_percent(self.max_rollout_percent) {
            return SignedAutoupdateDecision::reject(
                SignedAutoupdateRejectReason::InvalidPolicyRolloutLimit,
            );
        }

        if self.trusted_signing_keys.len() > MAX_TRUSTED_SIGNING_KEYS {
            return SignedAutoupdateDecision::reject(
                SignedAutoupdateRejectReason::TooManyTrustedSigningKeys,
            );
        }

        if self
            .trusted_signing_keys
            .iter()
            .all(|key| key.trim().is_empty())
        {
            return SignedAutoupdateDecision::reject(
                SignedAutoupdateRejectReason::NoTrustedSigningKeys,
            );
        }

        if candidate.release_version.trim().is_empty() {
            return SignedAutoupdateDecision::reject(
                SignedAutoupdateRejectReason::MissingReleaseVersion,
            );
        }

        if !valid_rollout_percent(candidate.rollout_percent) {
            return SignedAutoupdateDecision::reject(
                SignedAutoupdateRejectReason::InvalidRequestedRollout,
            );
        }

        if candidate.rollout_percent > self.max_rollout_percent {
            return SignedAutoupdateDecision::reject(
                SignedAutoupdateRejectReason::RolloutExceedsPolicy,
            );
        }

        if candidate.artifacts.is_empty() {
            return SignedAutoupdateDecision::reject(SignedAutoupdateRejectReason::NoArtifacts);
        }

        if candidate.artifacts.len() > MAX_RELEASE_ARTIFACTS {
            return SignedAutoupdateDecision::reject(
                SignedAutoupdateRejectReason::TooManyArtifacts,
            );
        }

        for artifact in &candidate.artifacts {
            if let Some(reason) = self.artifact_rejection_reason(artifact) {
                return SignedAutoupdateDecision::reject(reason);
            }
        }

        match &candidate.rollback_artifact {
            Some(artifact) if self.artifact_rejection_reason(artifact).is_none() => {}
            Some(_) => {
                return SignedAutoupdateDecision::reject(
                    SignedAutoupdateRejectReason::RollbackNotAuthenticated,
                );
            }
            None => {
                return SignedAutoupdateDecision::reject(
                    SignedAutoupdateRejectReason::MissingRollback,
                );
            }
        }

        SignedAutoupdateDecision::accept()
    }

    fn artifact_rejection_reason(
        &self,
        artifact: &SignedReleaseArtifact,
    ) -> Option<SignedAutoupdateRejectReason> {
        if artifact.artifact_id.trim().is_empty() {
            return Some(SignedAutoupdateRejectReason::MissingArtifactId);
        }

        if artifact.digest_sha256_hex.trim().is_empty() {
            return Some(SignedAutoupdateRejectReason::ArtifactDigestMissing);
        }

        if !valid_sha256_hex(&artifact.digest_sha256_hex) {
            return Some(SignedAutoupdateRejectReason::ArtifactDigestInvalid);
        }

        match artifact.signature.status {
            SignatureVerificationStatus::Verified => {
                if self.is_trusted_signing_key(&artifact.signature.signing_key_id) {
                    None
                } else {
                    Some(SignedAutoupdateRejectReason::UntrustedSigningKey)
                }
            }
            SignatureVerificationStatus::Missing => {
                Some(SignedAutoupdateRejectReason::UnsignedArtifact)
            }
            SignatureVerificationStatus::Invalid => {
                Some(SignedAutoupdateRejectReason::ArtifactSignatureInvalid)
            }
            SignatureVerificationStatus::UntrustedKey => {
                Some(SignedAutoupdateRejectReason::UntrustedSigningKey)
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedAutoupdateCandidate {
    pub release_version: String,
    pub rollout_percent: u8,
    pub artifacts: Vec<SignedReleaseArtifact>,
    pub rollback_artifact: Option<SignedReleaseArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedReleaseArtifact {
    pub artifact_id: String,
    pub artifact_kind: SignedReleaseArtifactKind,
    pub digest_sha256_hex: String,
    pub signature: SignatureVerification,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SignedReleaseArtifactKind {
    NodeBinary,
    PackageArchive,
    Installer,
    Manifest,
    Other(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignatureVerification {
    pub signing_key_id: String,
    pub status: SignatureVerificationStatus,
}

impl SignatureVerification {
    pub fn verified(signing_key_id: impl Into<String>) -> Self {
        Self {
            signing_key_id: signing_key_id.into(),
            status: SignatureVerificationStatus::Verified,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SignatureVerificationStatus {
    Verified,
    Missing,
    Invalid,
    UntrustedKey,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SignedAutoupdateDecision {
    pub accepted: bool,
    pub reason: SignedAutoupdateDecisionReason,
}

impl SignedAutoupdateDecision {
    pub fn accept() -> Self {
        Self {
            accepted: true,
            reason: SignedAutoupdateDecisionReason::Accepted,
        }
    }

    pub fn reject(reason: SignedAutoupdateRejectReason) -> Self {
        Self {
            accepted: false,
            reason: SignedAutoupdateDecisionReason::Rejected(reason),
        }
    }

    pub fn reason_code(&self) -> &'static str {
        self.reason.as_str()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SignedAutoupdateDecisionReason {
    Accepted,
    Rejected(SignedAutoupdateRejectReason),
}

impl SignedAutoupdateDecisionReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected(reason) => reason.as_str(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SignedAutoupdateRejectReason {
    PolicyDisabled,
    InvalidPolicyRolloutLimit,
    NoTrustedSigningKeys,
    TooManyTrustedSigningKeys,
    MissingReleaseVersion,
    InvalidRequestedRollout,
    RolloutExceedsPolicy,
    NoArtifacts,
    TooManyArtifacts,
    MissingArtifactId,
    ArtifactDigestMissing,
    ArtifactDigestInvalid,
    UnsignedArtifact,
    ArtifactSignatureInvalid,
    UntrustedSigningKey,
    MissingRollback,
    RollbackNotAuthenticated,
}

impl SignedAutoupdateRejectReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PolicyDisabled => "policy_disabled",
            Self::InvalidPolicyRolloutLimit => "invalid_policy_rollout_limit",
            Self::NoTrustedSigningKeys => "no_trusted_signing_keys",
            Self::TooManyTrustedSigningKeys => "too_many_trusted_signing_keys",
            Self::MissingReleaseVersion => "missing_release_version",
            Self::InvalidRequestedRollout => "invalid_requested_rollout",
            Self::RolloutExceedsPolicy => "rollout_exceeds_policy",
            Self::NoArtifacts => "no_artifacts",
            Self::TooManyArtifacts => "too_many_artifacts",
            Self::MissingArtifactId => "missing_artifact_id",
            Self::ArtifactDigestMissing => "artifact_digest_missing",
            Self::ArtifactDigestInvalid => "artifact_digest_invalid",
            Self::UnsignedArtifact => "unsigned_artifact",
            Self::ArtifactSignatureInvalid => "artifact_signature_invalid",
            Self::UntrustedSigningKey => "untrusted_signing_key",
            Self::MissingRollback => "missing_rollback",
            Self::RollbackNotAuthenticated => "rollback_not_authenticated",
        }
    }
}

fn valid_rollout_percent(value: u8) -> bool {
    (1..=100).contains(&value)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TRUSTED_KEY: &str = "iamine-release-key-v1";

    fn digest() -> String {
        "a".repeat(64)
    }

    fn verified_artifact(id: &str, signing_key_id: &str) -> SignedReleaseArtifact {
        SignedReleaseArtifact {
            artifact_id: id.to_string(),
            artifact_kind: SignedReleaseArtifactKind::PackageArchive,
            digest_sha256_hex: digest(),
            signature: SignatureVerification::verified(signing_key_id),
        }
    }

    fn verified_candidate() -> SignedAutoupdateCandidate {
        SignedAutoupdateCandidate {
            release_version: "0.10.0".to_string(),
            rollout_percent: 5,
            artifacts: vec![verified_artifact("iamine-node-darwin-arm64", TRUSTED_KEY)],
            rollback_artifact: Some(verified_artifact(
                "iamine-node-darwin-arm64-prev",
                TRUSTED_KEY,
            )),
        }
    }

    #[test]
    fn default_policy_rejects_updates() {
        let decision = SignedAutoupdatePolicy::default().evaluate(&verified_candidate());

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "policy_disabled");
    }

    #[test]
    fn controlled_policy_accepts_verified_candidate() {
        let policy = SignedAutoupdatePolicy::controlled(vec![TRUSTED_KEY.to_string()], 10);
        let decision = policy.evaluate(&verified_candidate());

        assert!(decision.accepted);
        assert_eq!(decision.reason_code(), "accepted");
    }

    #[test]
    fn controlled_policy_rejects_unsigned_artifact() {
        let policy = SignedAutoupdatePolicy::controlled(vec![TRUSTED_KEY.to_string()], 10);
        let mut candidate = verified_candidate();
        candidate.artifacts[0].signature.status = SignatureVerificationStatus::Missing;

        let decision = policy.evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "unsigned_artifact");
    }

    #[test]
    fn controlled_policy_rejects_untrusted_signing_key() {
        let policy = SignedAutoupdatePolicy::controlled(vec![TRUSTED_KEY.to_string()], 10);
        let mut candidate = verified_candidate();
        candidate.artifacts[0].signature = SignatureVerification::verified("other-key");

        let decision = policy.evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "untrusted_signing_key");
    }

    #[test]
    fn controlled_policy_rejects_unbounded_trusted_signing_keys() {
        let keys = (0..=MAX_TRUSTED_SIGNING_KEYS)
            .map(|idx| format!("iamine-release-key-{idx}"))
            .collect();
        let policy = SignedAutoupdatePolicy::controlled(keys, 10);

        let decision = policy.evaluate(&verified_candidate());

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "too_many_trusted_signing_keys");
    }

    #[test]
    fn controlled_policy_rejects_rollout_above_policy_limit() {
        let policy = SignedAutoupdatePolicy::controlled(vec![TRUSTED_KEY.to_string()], 5);
        let mut candidate = verified_candidate();
        candidate.rollout_percent = 10;

        let decision = policy.evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "rollout_exceeds_policy");
    }

    #[test]
    fn controlled_policy_rejects_missing_rollback_when_required() {
        let policy = SignedAutoupdatePolicy::controlled(vec![TRUSTED_KEY.to_string()], 10);
        let mut candidate = verified_candidate();
        candidate.rollback_artifact = None;

        let decision = policy.evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "missing_rollback");
    }

    #[test]
    fn controlled_policy_rejects_invalid_artifact_digest() {
        let policy = SignedAutoupdatePolicy::controlled(vec![TRUSTED_KEY.to_string()], 10);
        let mut candidate = verified_candidate();
        candidate.artifacts[0].digest_sha256_hex = "not-a-sha256".to_string();

        let decision = policy.evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "artifact_digest_invalid");
    }

    #[test]
    fn controlled_policy_rejects_too_many_artifacts() {
        let policy = SignedAutoupdatePolicy::controlled(vec![TRUSTED_KEY.to_string()], 10);
        let mut candidate = verified_candidate();
        candidate.artifacts = (0..=MAX_RELEASE_ARTIFACTS)
            .map(|idx| verified_artifact(&idx.to_string(), TRUSTED_KEY))
            .collect();

        let decision = policy.evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "too_many_artifacts");
    }
}
