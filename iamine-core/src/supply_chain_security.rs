use crate::release_validation::valid_sha256_hex;
use serde::{Deserialize, Serialize};

pub const MAX_SUPPLY_CHAIN_ARTIFACTS: usize = 24;
pub const MAX_TRUSTED_BUILDERS: usize = 8;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum SupplyChainSecurityMode {
    #[default]
    Disabled,
    ControlledRelease,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SupplyChainSecurityPolicy {
    pub mode: SupplyChainSecurityMode,
    pub trusted_builder_ids: Vec<String>,
}

impl Default for SupplyChainSecurityPolicy {
    fn default() -> Self {
        Self {
            mode: SupplyChainSecurityMode::Disabled,
            trusted_builder_ids: Vec::new(),
        }
    }
}

impl SupplyChainSecurityPolicy {
    pub fn controlled(trusted_builder_ids: Vec<String>) -> Self {
        Self {
            mode: SupplyChainSecurityMode::ControlledRelease,
            trusted_builder_ids,
        }
    }

    pub fn evaluate(&self, candidate: &SupplyChainReleaseCandidate) -> SupplyChainDecision {
        if self.mode == SupplyChainSecurityMode::Disabled {
            return SupplyChainDecision::reject(SupplyChainRejectReason::PolicyDisabled);
        }

        if self.trusted_builder_ids.len() > MAX_TRUSTED_BUILDERS {
            return SupplyChainDecision::reject(SupplyChainRejectReason::TooManyTrustedBuilders);
        }

        if self
            .trusted_builder_ids
            .iter()
            .all(|builder_id| builder_id.trim().is_empty())
        {
            return SupplyChainDecision::reject(SupplyChainRejectReason::NoTrustedBuilders);
        }

        if candidate.release_version.trim().is_empty() {
            return SupplyChainDecision::reject(SupplyChainRejectReason::MissingReleaseVersion);
        }

        if !valid_git_object_sha(&candidate.source.commit_sha) {
            return SupplyChainDecision::reject(SupplyChainRejectReason::InvalidSourceCommitSha);
        }

        if !valid_git_object_sha(&candidate.source.tree_sha) {
            return SupplyChainDecision::reject(SupplyChainRejectReason::InvalidSourceTreeSha);
        }

        if !candidate.source.tracked_clean {
            return SupplyChainDecision::reject(SupplyChainRejectReason::SourceTrackedDirty);
        }

        if !candidate.source.staging_clean {
            return SupplyChainDecision::reject(SupplyChainRejectReason::SourceStagingDirty);
        }

        if !valid_sha256_hex(&candidate.dependencies.lockfile_sha256_hex) {
            return SupplyChainDecision::reject(
                SupplyChainRejectReason::InvalidDependencyLockDigest,
            );
        }

        if let Some(reason) =
            dependency_check_rejection(&candidate.dependencies.cargo_audit, DependencyCheck::Audit)
        {
            return SupplyChainDecision::reject(reason);
        }

        if let Some(reason) =
            dependency_check_rejection(&candidate.dependencies.cargo_deny, DependencyCheck::Deny)
        {
            return SupplyChainDecision::reject(reason);
        }

        if let Some(reason) = secret_scan_rejection(&candidate.secret_scan) {
            return SupplyChainDecision::reject(reason);
        }

        if !self.is_trusted_builder(&candidate.build.builder_id) {
            return SupplyChainDecision::reject(SupplyChainRejectReason::UntrustedBuilder);
        }

        if candidate.build.source_commit_sha != candidate.source.commit_sha
            || candidate.build.source_tree_sha != candidate.source.tree_sha
        {
            return SupplyChainDecision::reject(SupplyChainRejectReason::BuildSourceMismatch);
        }

        if !candidate.build.isolated {
            return SupplyChainDecision::reject(SupplyChainRejectReason::BuildNotIsolated);
        }

        if !candidate.build.reproducible {
            return SupplyChainDecision::reject(SupplyChainRejectReason::BuildNotReproducible);
        }

        if !candidate.build.tests_passed {
            return SupplyChainDecision::reject(SupplyChainRejectReason::BuildTestsNotPassed);
        }

        if let Some(reason) = build_provenance_rejection(&candidate.build.provenance) {
            return SupplyChainDecision::reject(reason);
        }

        if candidate.artifacts.is_empty() {
            return SupplyChainDecision::reject(SupplyChainRejectReason::NoArtifacts);
        }

        if candidate.artifacts.len() > MAX_SUPPLY_CHAIN_ARTIFACTS {
            return SupplyChainDecision::reject(SupplyChainRejectReason::TooManyArtifacts);
        }

        for artifact in &candidate.artifacts {
            if let Some(reason) = self.artifact_rejection_reason(candidate, artifact) {
                return SupplyChainDecision::reject(reason);
            }
        }

        SupplyChainDecision::accept()
    }

    fn artifact_rejection_reason(
        &self,
        candidate: &SupplyChainReleaseCandidate,
        artifact: &SupplyChainArtifact,
    ) -> Option<SupplyChainRejectReason> {
        if artifact.artifact_id.trim().is_empty() {
            return Some(SupplyChainRejectReason::MissingArtifactId);
        }

        if !valid_sha256_hex(&artifact.digest_sha256_hex) {
            return Some(SupplyChainRejectReason::ArtifactDigestInvalid);
        }

        if artifact.source_commit_sha != candidate.source.commit_sha
            || artifact.source_tree_sha != candidate.source.tree_sha
        {
            return Some(SupplyChainRejectReason::ArtifactSourceMismatch);
        }

        if !self.is_trusted_builder(&artifact.builder_id) {
            return Some(SupplyChainRejectReason::ArtifactUntrustedBuilder);
        }

        artifact_provenance_rejection(&artifact.provenance)
    }

    fn is_trusted_builder(&self, builder_id: &str) -> bool {
        let builder_id = builder_id.trim();
        !builder_id.is_empty()
            && self
                .trusted_builder_ids
                .iter()
                .any(|trusted| trusted.trim() == builder_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SupplyChainReleaseCandidate {
    pub release_version: String,
    pub source: SupplyChainSourceEvidence,
    pub dependencies: SupplyChainDependencyEvidence,
    pub secret_scan: SupplyChainCheckStatus,
    pub build: SupplyChainBuildEvidence,
    pub artifacts: Vec<SupplyChainArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SupplyChainSourceEvidence {
    pub commit_sha: String,
    pub tree_sha: String,
    pub tracked_clean: bool,
    pub staging_clean: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SupplyChainDependencyEvidence {
    pub lockfile_sha256_hex: String,
    pub cargo_audit: SupplyChainCheckStatus,
    pub cargo_deny: SupplyChainCheckStatus,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SupplyChainCheckStatus {
    Pass,
    Fail,
    Skipped,
    SkippedWithAcceptedBaseline,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SupplyChainBuildEvidence {
    pub builder_id: String,
    pub source_commit_sha: String,
    pub source_tree_sha: String,
    pub isolated: bool,
    pub reproducible: bool,
    pub tests_passed: bool,
    pub provenance: SupplyChainProvenanceVerification,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SupplyChainArtifact {
    pub artifact_id: String,
    pub artifact_kind: SupplyChainArtifactKind,
    pub digest_sha256_hex: String,
    pub source_commit_sha: String,
    pub source_tree_sha: String,
    pub builder_id: String,
    pub provenance: SupplyChainProvenanceVerification,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SupplyChainArtifactKind {
    NodeBinary,
    PackageArchive,
    Installer,
    Manifest,
    Sbom,
    Attestation,
    Other(String),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SupplyChainProvenanceVerification {
    Verified,
    Missing,
    Invalid,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SupplyChainDecision {
    pub accepted: bool,
    pub reason: SupplyChainDecisionReason,
}

impl SupplyChainDecision {
    pub fn accept() -> Self {
        Self {
            accepted: true,
            reason: SupplyChainDecisionReason::Accepted,
        }
    }

    pub fn reject(reason: SupplyChainRejectReason) -> Self {
        Self {
            accepted: false,
            reason: SupplyChainDecisionReason::Rejected(reason),
        }
    }

    pub fn reason_code(&self) -> &'static str {
        self.reason.as_str()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SupplyChainDecisionReason {
    Accepted,
    Rejected(SupplyChainRejectReason),
}

impl SupplyChainDecisionReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected(reason) => reason.as_str(),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum SupplyChainRejectReason {
    PolicyDisabled,
    NoTrustedBuilders,
    TooManyTrustedBuilders,
    MissingReleaseVersion,
    InvalidSourceCommitSha,
    InvalidSourceTreeSha,
    SourceTrackedDirty,
    SourceStagingDirty,
    InvalidDependencyLockDigest,
    CargoAuditFailed,
    CargoAuditMissingAcceptedBaseline,
    CargoDenyFailed,
    CargoDenyMissingAcceptedBaseline,
    SecretScanFailed,
    SecretScanRequired,
    UntrustedBuilder,
    BuildSourceMismatch,
    BuildNotIsolated,
    BuildNotReproducible,
    BuildTestsNotPassed,
    BuildProvenanceMissing,
    BuildProvenanceInvalid,
    NoArtifacts,
    TooManyArtifacts,
    MissingArtifactId,
    ArtifactDigestInvalid,
    ArtifactSourceMismatch,
    ArtifactUntrustedBuilder,
    ArtifactProvenanceMissing,
    ArtifactProvenanceInvalid,
}

impl SupplyChainRejectReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PolicyDisabled => "policy_disabled",
            Self::NoTrustedBuilders => "no_trusted_builders",
            Self::TooManyTrustedBuilders => "too_many_trusted_builders",
            Self::MissingReleaseVersion => "missing_release_version",
            Self::InvalidSourceCommitSha => "invalid_source_commit_sha",
            Self::InvalidSourceTreeSha => "invalid_source_tree_sha",
            Self::SourceTrackedDirty => "source_tracked_dirty",
            Self::SourceStagingDirty => "source_staging_dirty",
            Self::InvalidDependencyLockDigest => "invalid_dependency_lock_digest",
            Self::CargoAuditFailed => "cargo_audit_failed",
            Self::CargoAuditMissingAcceptedBaseline => "cargo_audit_missing_accepted_baseline",
            Self::CargoDenyFailed => "cargo_deny_failed",
            Self::CargoDenyMissingAcceptedBaseline => "cargo_deny_missing_accepted_baseline",
            Self::SecretScanFailed => "secret_scan_failed",
            Self::SecretScanRequired => "secret_scan_required",
            Self::UntrustedBuilder => "untrusted_builder",
            Self::BuildSourceMismatch => "build_source_mismatch",
            Self::BuildNotIsolated => "build_not_isolated",
            Self::BuildNotReproducible => "build_not_reproducible",
            Self::BuildTestsNotPassed => "build_tests_not_passed",
            Self::BuildProvenanceMissing => "build_provenance_missing",
            Self::BuildProvenanceInvalid => "build_provenance_invalid",
            Self::NoArtifacts => "no_artifacts",
            Self::TooManyArtifacts => "too_many_artifacts",
            Self::MissingArtifactId => "missing_artifact_id",
            Self::ArtifactDigestInvalid => "artifact_digest_invalid",
            Self::ArtifactSourceMismatch => "artifact_source_mismatch",
            Self::ArtifactUntrustedBuilder => "artifact_untrusted_builder",
            Self::ArtifactProvenanceMissing => "artifact_provenance_missing",
            Self::ArtifactProvenanceInvalid => "artifact_provenance_invalid",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DependencyCheck {
    Audit,
    Deny,
}

fn dependency_check_rejection(
    status: &SupplyChainCheckStatus,
    check: DependencyCheck,
) -> Option<SupplyChainRejectReason> {
    match (status, check) {
        (SupplyChainCheckStatus::Pass, _) => None,
        (SupplyChainCheckStatus::SkippedWithAcceptedBaseline, _) => None,
        (SupplyChainCheckStatus::Fail, DependencyCheck::Audit) => {
            Some(SupplyChainRejectReason::CargoAuditFailed)
        }
        (SupplyChainCheckStatus::Fail, DependencyCheck::Deny) => {
            Some(SupplyChainRejectReason::CargoDenyFailed)
        }
        (SupplyChainCheckStatus::Skipped, DependencyCheck::Audit) => {
            Some(SupplyChainRejectReason::CargoAuditMissingAcceptedBaseline)
        }
        (SupplyChainCheckStatus::Skipped, DependencyCheck::Deny) => {
            Some(SupplyChainRejectReason::CargoDenyMissingAcceptedBaseline)
        }
    }
}

fn secret_scan_rejection(status: &SupplyChainCheckStatus) -> Option<SupplyChainRejectReason> {
    match status {
        SupplyChainCheckStatus::Pass => None,
        SupplyChainCheckStatus::Fail => Some(SupplyChainRejectReason::SecretScanFailed),
        SupplyChainCheckStatus::Skipped | SupplyChainCheckStatus::SkippedWithAcceptedBaseline => {
            Some(SupplyChainRejectReason::SecretScanRequired)
        }
    }
}

fn build_provenance_rejection(
    status: &SupplyChainProvenanceVerification,
) -> Option<SupplyChainRejectReason> {
    match status {
        SupplyChainProvenanceVerification::Verified => None,
        SupplyChainProvenanceVerification::Missing => {
            Some(SupplyChainRejectReason::BuildProvenanceMissing)
        }
        SupplyChainProvenanceVerification::Invalid => {
            Some(SupplyChainRejectReason::BuildProvenanceInvalid)
        }
    }
}

fn artifact_provenance_rejection(
    status: &SupplyChainProvenanceVerification,
) -> Option<SupplyChainRejectReason> {
    match status {
        SupplyChainProvenanceVerification::Verified => None,
        SupplyChainProvenanceVerification::Missing => {
            Some(SupplyChainRejectReason::ArtifactProvenanceMissing)
        }
        SupplyChainProvenanceVerification::Invalid => {
            Some(SupplyChainRejectReason::ArtifactProvenanceInvalid)
        }
    }
}

fn valid_git_object_sha(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
mod tests {
    use super::*;

    const BUILDER_ID: &str = "iamine-release-builder-v1";

    fn commit_sha() -> String {
        "a".repeat(40)
    }

    fn tree_sha() -> String {
        "b".repeat(40)
    }

    fn digest() -> String {
        "c".repeat(64)
    }

    fn policy() -> SupplyChainSecurityPolicy {
        SupplyChainSecurityPolicy::controlled(vec![BUILDER_ID.to_string()])
    }

    fn candidate() -> SupplyChainReleaseCandidate {
        let source = SupplyChainSourceEvidence {
            commit_sha: commit_sha(),
            tree_sha: tree_sha(),
            tracked_clean: true,
            staging_clean: true,
        };
        SupplyChainReleaseCandidate {
            release_version: "0.10.0".to_string(),
            dependencies: SupplyChainDependencyEvidence {
                lockfile_sha256_hex: digest(),
                cargo_audit: SupplyChainCheckStatus::Pass,
                cargo_deny: SupplyChainCheckStatus::Pass,
            },
            secret_scan: SupplyChainCheckStatus::Pass,
            build: SupplyChainBuildEvidence {
                builder_id: BUILDER_ID.to_string(),
                source_commit_sha: source.commit_sha.clone(),
                source_tree_sha: source.tree_sha.clone(),
                isolated: true,
                reproducible: true,
                tests_passed: true,
                provenance: SupplyChainProvenanceVerification::Verified,
            },
            artifacts: vec![SupplyChainArtifact {
                artifact_id: "iamine-node-darwin-arm64".to_string(),
                artifact_kind: SupplyChainArtifactKind::NodeBinary,
                digest_sha256_hex: digest(),
                source_commit_sha: source.commit_sha.clone(),
                source_tree_sha: source.tree_sha.clone(),
                builder_id: BUILDER_ID.to_string(),
                provenance: SupplyChainProvenanceVerification::Verified,
            }],
            source,
        }
    }

    #[test]
    fn default_policy_rejects_release_candidates() {
        let decision = SupplyChainSecurityPolicy::default().evaluate(&candidate());

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "policy_disabled");
    }

    #[test]
    fn controlled_policy_accepts_verified_release_candidate() {
        let decision = policy().evaluate(&candidate());

        assert!(decision.accepted);
        assert_eq!(decision.reason_code(), "accepted");
    }

    #[test]
    fn controlled_policy_accepts_explicit_dependency_baseline_exception() {
        let mut candidate = candidate();
        candidate.dependencies.cargo_audit = SupplyChainCheckStatus::SkippedWithAcceptedBaseline;
        candidate.dependencies.cargo_deny = SupplyChainCheckStatus::SkippedWithAcceptedBaseline;

        let decision = policy().evaluate(&candidate);

        assert!(decision.accepted);
        assert_eq!(decision.reason_code(), "accepted");
    }

    #[test]
    fn controlled_policy_rejects_skipped_audit_without_exception() {
        let mut candidate = candidate();
        candidate.dependencies.cargo_audit = SupplyChainCheckStatus::Skipped;

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(
            decision.reason_code(),
            "cargo_audit_missing_accepted_baseline"
        );
    }

    #[test]
    fn controlled_policy_rejects_secret_scan_failure() {
        let mut candidate = candidate();
        candidate.secret_scan = SupplyChainCheckStatus::Fail;

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "secret_scan_failed");
    }

    #[test]
    fn controlled_policy_rejects_skipped_secret_scan_even_with_baseline() {
        let mut candidate = candidate();
        candidate.secret_scan = SupplyChainCheckStatus::SkippedWithAcceptedBaseline;

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "secret_scan_required");
    }

    #[test]
    fn controlled_policy_rejects_dirty_source() {
        let mut candidate = candidate();
        candidate.source.tracked_clean = false;

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "source_tracked_dirty");
    }

    #[test]
    fn controlled_policy_rejects_untrusted_builders() {
        let mut candidate = candidate();
        candidate.build.builder_id = "other-builder".to_string();

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "untrusted_builder");
    }

    #[test]
    fn controlled_policy_rejects_build_source_mismatch() {
        let mut candidate = candidate();
        candidate.build.source_commit_sha = "d".repeat(40);

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "build_source_mismatch");
    }

    #[test]
    fn controlled_policy_rejects_missing_build_provenance() {
        let mut candidate = candidate();
        candidate.build.provenance = SupplyChainProvenanceVerification::Missing;

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "build_provenance_missing");
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
    fn controlled_policy_rejects_artifact_source_mismatch() {
        let mut candidate = candidate();
        candidate.artifacts[0].source_tree_sha = "d".repeat(40);

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "artifact_source_mismatch");
    }

    #[test]
    fn controlled_policy_rejects_missing_artifact_provenance() {
        let mut candidate = candidate();
        candidate.artifacts[0].provenance = SupplyChainProvenanceVerification::Missing;

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "artifact_provenance_missing");
    }

    #[test]
    fn controlled_policy_rejects_too_many_artifacts() {
        let mut candidate = candidate();
        candidate.artifacts = (0..=MAX_SUPPLY_CHAIN_ARTIFACTS)
            .map(|idx| SupplyChainArtifact {
                artifact_id: format!("artifact-{idx}"),
                artifact_kind: SupplyChainArtifactKind::PackageArchive,
                digest_sha256_hex: digest(),
                source_commit_sha: candidate.source.commit_sha.clone(),
                source_tree_sha: candidate.source.tree_sha.clone(),
                builder_id: BUILDER_ID.to_string(),
                provenance: SupplyChainProvenanceVerification::Verified,
            })
            .collect();

        let decision = policy().evaluate(&candidate);

        assert!(!decision.accepted);
        assert_eq!(decision.reason_code(), "too_many_artifacts");
    }
}
