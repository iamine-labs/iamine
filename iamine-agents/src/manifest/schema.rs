use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const MANIFEST_SCHEMA_ID: &str = "iamine.agent.package.draft-0.1";
pub const MANIFEST_FILE_NAME: &str = "agent.yaml";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentPackageManifest {
    pub schema: String,
    pub package_id: String,
    pub package_version: String,
    pub display_name: String,
    pub summary: String,
    pub official_pack: String,
    pub status: PackageStatus,
    pub execution_authorized: bool,
    pub agent: AgentMetadata,
    pub references: ManifestReferences,
    pub distribution: DistributionPolicy,
    pub security: SecurityPolicy,
    pub review: ReviewPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PackageStatus {
    Planning,
    Review,
    BetaCandidate,
    Blocked,
    Deprecated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AgentMetadata {
    pub family: String,
    pub personas: Vec<String>,
    pub earliest_mode: ExecutionMode,
    pub task_class: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    LocalReadonly,
    LocalPlanning,
    LanReadonly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ManifestReferences {
    pub scope_manifest: String,
    pub capability_metadata: String,
    pub expertise_metadata: String,
    pub resource_requirements: String,
    pub permission_model: String,
    pub audit_policy: String,
    pub boundary_tests: String,
}

impl ManifestReferences {
    pub(crate) fn values(&self) -> [(&'static str, &str); 7] {
        [
            ("references.scope_manifest", &self.scope_manifest),
            ("references.capability_metadata", &self.capability_metadata),
            ("references.expertise_metadata", &self.expertise_metadata),
            (
                "references.resource_requirements",
                &self.resource_requirements,
            ),
            ("references.permission_model", &self.permission_model),
            ("references.audit_policy", &self.audit_policy),
            ("references.boundary_tests", &self.boundary_tests),
        ]
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DistributionPolicy {
    pub allowed_channels: Vec<DistributionChannel>,
    pub allowed_install_modes: Vec<InstallMode>,
    pub public_beta: bool,
    pub marketplace: bool,
    pub third_party_publication: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DistributionChannel {
    LocalDev,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InstallMode {
    ManualReview,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SecurityPolicy {
    pub collects_credentials: bool,
    pub collects_host_identifiers: bool,
    pub requires_network: bool,
    pub allows_destructive_actions: bool,
    pub allows_arbitrary_shell: bool,
    pub allows_unrestricted_filesystem: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReviewPolicy {
    pub requires_human_review: bool,
    pub requires_scope_manifest: bool,
    pub requires_permission_review: bool,
    pub requires_audit_policy: bool,
    pub requires_boundary_tests: bool,
}
