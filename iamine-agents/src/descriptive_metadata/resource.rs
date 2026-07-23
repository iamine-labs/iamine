use std::collections::{BTreeMap, HashSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use super::error::{
    DescriptiveMetadataViolationCode, DescriptiveMetadataViolations, ViolationCollector,
};
use super::validation::{
    validate_blocked, validate_identifier, validate_package_id, validate_safe_references,
    validate_version,
};
use super::{json_schema, parse_yaml, DescriptiveMetadataError};

pub const RESOURCE_REQUIREMENTS_SCHEMA_ID: &str = "iamine.agent.resources.draft-0.1";

const MAX_LOGICAL_CORES: u16 = 256;
const MAX_BACKGROUND_THREADS: u16 = 64;
const MAX_MEMORY_MB: u64 = 1_048_576;
const MAX_STORAGE_MB: u64 = 1_048_576;
const BLOCKED_BACKEND_CLASSES: &[&str] = &[
    "arbitrary_backend",
    "mainnet_backend",
    "private_remote_backend",
    "unrestricted_remote",
];

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResourceRequirementsMetadata {
    pub schema: String,
    pub package_id: String,
    pub resource_profile_id: String,
    pub resource_profile_version: String,
    pub operating_modes: Vec<ResourceOperatingMode>,
    pub cpu: BTreeMap<String, CpuRequirements>,
    pub memory: BTreeMap<String, MemoryRequirements>,
    pub storage: BTreeMap<String, StorageRequirements>,
    pub network: BTreeMap<String, NetworkRequirements>,
    pub model_dependencies: ModelDependencies,
    pub accelerators: AcceleratorRequirements,
    pub constraints: ResourceConstraints,
    pub degradation: ResourceDegradation,
    pub privacy: ResourcePrivacy,
    pub review: ResourceReview,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResourceOperatingMode {
    LocalReadonly,
    LocalPlanning,
    LanReadonly,
}

impl ResourceOperatingMode {
    const fn as_str(self) -> &'static str {
        match self {
            Self::LocalReadonly => "local_readonly",
            Self::LocalPlanning => "local_planning",
            Self::LanReadonly => "lan_readonly",
        }
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CpuRequirements {
    pub min_logical_cores: u16,
    pub recommended_logical_cores: u16,
    pub max_background_threads: u16,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MemoryRequirements {
    pub min_ram_mb: u64,
    pub recommended_ram_mb: u64,
    pub max_working_set_mb: u64,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct StorageRequirements {
    pub package_size_mb: u64,
    pub temp_workspace_mb: u64,
    pub cache_budget_mb: u64,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NetworkRequirements {
    pub mode: NetworkMode,
    pub opens_ports: bool,
    pub downloads_artifacts: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NetworkMode {
    None,
    LocalOnly,
    LanReadonly,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ModelDependencies {
    pub requires_model_download: bool,
    pub requires_model_load: bool,
    pub backend_class: String,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AcceleratorRequirements {
    pub required: AcceleratorClass,
    pub optional: Vec<AcceleratorClass>,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AcceleratorClass {
    None,
    OptionalGpu,
    OptionalNeuralEngine,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResourceConstraints {
    pub runs_dynamic_hardware_probe: bool,
    pub allows_unrestricted_filesystem: bool,
    pub starts_background_download: bool,
    pub loads_models: bool,
    pub restarts_services: bool,
    pub mutates_vm_or_container: bool,
    pub starts_worker: bool,
    pub overrides_scheduler: bool,
    pub claims_runtime_priority: bool,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResourceDegradation {
    pub on_insufficient_resources: ResourceDegradationBehavior,
}

#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResourceDegradationBehavior {
    BlockRuntimeEligibility,
    RequireHumanReview,
    OfferPlanningOnly,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResourcePrivacy {
    pub stores_raw_hardware_inventory: bool,
    pub stores_permanent_hardware_fingerprint: bool,
    pub stores_private_paths: bool,
    pub stores_host_identifiers: bool,
    pub stores_credentials: bool,
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResourceReview {
    pub requires_human_review: bool,
    pub self_approval_allowed: bool,
    pub evidence: Vec<String>,
}

pub fn resource_requirements_json_schema() -> Result<serde_json::Value, DescriptiveMetadataError> {
    json_schema::<ResourceRequirementsMetadata>()
}

pub fn parse_resource_requirements_yaml(
    input: &str,
) -> Result<ResourceRequirementsMetadata, DescriptiveMetadataError> {
    parse_yaml(input, validate_resource_requirements)
}

fn validate_resource_requirements(
    metadata: &ResourceRequirementsMetadata,
) -> Result<(), DescriptiveMetadataViolations> {
    let mut collector = ViolationCollector::default();
    if metadata.schema != RESOURCE_REQUIREMENTS_SCHEMA_ID {
        collector.push(
            DescriptiveMetadataViolationCode::UnsupportedSchema,
            "schema",
            "resource requirements schema identifier is not supported",
        );
    }
    validate_package_id(&mut collector, &metadata.package_id);
    validate_identifier(
        &mut collector,
        "resource_profile_id",
        &metadata.resource_profile_id,
        true,
    );
    validate_version(
        &mut collector,
        "resource_profile_version",
        &metadata.resource_profile_version,
    );

    let modes = validate_operating_modes(&mut collector, &metadata.operating_modes);
    validate_mode_map(&mut collector, "cpu", &modes, &metadata.cpu);
    validate_mode_map(&mut collector, "memory", &modes, &metadata.memory);
    validate_mode_map(&mut collector, "storage", &modes, &metadata.storage);
    validate_mode_map(&mut collector, "network", &modes, &metadata.network);

    for value in metadata.cpu.values() {
        if value.min_logical_cores == 0
            || value.min_logical_cores > MAX_LOGICAL_CORES
            || value.recommended_logical_cores < value.min_logical_cores
            || value.recommended_logical_cores > MAX_LOGICAL_CORES
            || value.max_background_threads > MAX_BACKGROUND_THREADS
            || value.max_background_threads > value.recommended_logical_cores
        {
            collector.push(
                DescriptiveMetadataViolationCode::InvalidResourceBound,
                "cpu",
                "CPU requirements must be bounded and internally ordered",
            );
        }
    }
    for value in metadata.memory.values() {
        if value.min_ram_mb == 0
            || value.min_ram_mb > MAX_MEMORY_MB
            || value.recommended_ram_mb < value.min_ram_mb
            || value.recommended_ram_mb > MAX_MEMORY_MB
            || value.max_working_set_mb < value.recommended_ram_mb
            || value.max_working_set_mb > MAX_MEMORY_MB
        {
            collector.push(
                DescriptiveMetadataViolationCode::InvalidResourceBound,
                "memory",
                "memory requirements must be bounded and internally ordered",
            );
        }
    }
    for value in metadata.storage.values() {
        if value.package_size_mb == 0
            || value.package_size_mb > MAX_STORAGE_MB
            || value.temp_workspace_mb > MAX_STORAGE_MB
            || value.cache_budget_mb > MAX_STORAGE_MB
        {
            collector.push(
                DescriptiveMetadataViolationCode::InvalidResourceBound,
                "storage",
                "storage requirements must be bounded and unit-explicit",
            );
        }
    }
    if metadata
        .network
        .values()
        .any(|value| value.opens_ports || value.downloads_artifacts)
    {
        collector.push(
            DescriptiveMetadataViolationCode::UnsafeClaim,
            "network",
            "resource metadata cannot open ports or download artifacts",
        );
    }

    validate_identifier(
        &mut collector,
        "model_dependencies.backend_class",
        &metadata.model_dependencies.backend_class,
        true,
    );
    let backend = HashSet::from([metadata.model_dependencies.backend_class.as_str()]);
    validate_blocked(
        &mut collector,
        "model_dependencies.backend_class",
        &backend,
        BLOCKED_BACKEND_CLASSES,
    );
    if metadata.model_dependencies.requires_model_download
        || metadata.model_dependencies.requires_model_load
    {
        collector.push(
            DescriptiveMetadataViolationCode::UnsafeClaim,
            "model_dependencies",
            "resource metadata cannot authorize model download or loading",
        );
    }

    if metadata.accelerators.required != AcceleratorClass::None {
        collector.push(
            DescriptiveMetadataViolationCode::UnsafeClaim,
            "accelerators.required",
            "accelerators cannot be mandatory in this release phase",
        );
    }
    if metadata.accelerators.optional.len() > 2 {
        collector.push(
            DescriptiveMetadataViolationCode::InvalidCollection,
            "accelerators.optional",
            "optional accelerator declarations must remain bounded",
        );
    }
    let mut optional_accelerators = HashSet::with_capacity(metadata.accelerators.optional.len());
    for accelerator in &metadata.accelerators.optional {
        if *accelerator == AcceleratorClass::None || !optional_accelerators.insert(*accelerator) {
            collector.push(
                DescriptiveMetadataViolationCode::InvalidCollection,
                "accelerators.optional",
                "optional accelerators must be unique optional classes",
            );
        }
    }

    if metadata.constraints.runs_dynamic_hardware_probe
        || metadata.constraints.allows_unrestricted_filesystem
        || metadata.constraints.starts_background_download
        || metadata.constraints.loads_models
        || metadata.constraints.restarts_services
        || metadata.constraints.mutates_vm_or_container
        || metadata.constraints.starts_worker
        || metadata.constraints.overrides_scheduler
        || metadata.constraints.claims_runtime_priority
    {
        collector.push(
            DescriptiveMetadataViolationCode::UnsafeClaim,
            "constraints",
            "resource metadata cannot claim runtime or platform side effects",
        );
    }
    if metadata.privacy.stores_raw_hardware_inventory
        || metadata.privacy.stores_permanent_hardware_fingerprint
        || metadata.privacy.stores_private_paths
        || metadata.privacy.stores_host_identifiers
        || metadata.privacy.stores_credentials
    {
        collector.push(
            DescriptiveMetadataViolationCode::UnsafeClaim,
            "privacy",
            "resource metadata cannot retain private hardware or operator data",
        );
    }
    if !metadata.review.requires_human_review || metadata.review.self_approval_allowed {
        collector.push(
            DescriptiveMetadataViolationCode::MissingSafetyBoundary,
            "review",
            "resource metadata must require independent human review",
        );
    }
    validate_safe_references(&mut collector, "review.evidence", &metadata.review.evidence);

    collector.finish()
}

fn validate_operating_modes(
    collector: &mut ViolationCollector,
    values: &[ResourceOperatingMode],
) -> HashSet<&'static str> {
    if values.is_empty() || values.len() > 3 {
        collector.push(
            DescriptiveMetadataViolationCode::InvalidCollection,
            "operating_modes",
            "operating modes must be bounded and non-empty",
        );
    }
    let mut modes = HashSet::with_capacity(values.len());
    for value in values {
        if !modes.insert(value.as_str()) {
            collector.push(
                DescriptiveMetadataViolationCode::DuplicateValue,
                "operating_modes",
                "operating modes must be unique",
            );
        }
    }
    modes
}

fn validate_mode_map<T>(
    collector: &mut ViolationCollector,
    field: &'static str,
    modes: &HashSet<&str>,
    values: &BTreeMap<String, T>,
) {
    for key in values.keys() {
        validate_identifier(collector, field, key, false);
    }
    if values.len() != modes.len() || values.keys().any(|key| !modes.contains(key.as_str())) {
        collector.push(
            DescriptiveMetadataViolationCode::ContradictoryRequirement,
            field,
            "resource maps must exactly match declared operating modes",
        );
    }
}
