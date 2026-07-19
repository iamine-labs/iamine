use std::collections::HashSet;
use std::fmt;

use crate::identifiers::{is_package_identifier, is_snake_identifier};

const MAX_BOUNDARY_ENTRIES: usize = 64;

const BROAD_IDENTIFIERS: &[&str] = &[
    "admin",
    "all_files",
    "all_networks",
    "automation",
    "do_anything",
    "general_assistant",
    "general_help",
    "system_admin",
];

const REQUIRED_FORBIDDEN_INPUTS: &[&str] = &[
    "credentials",
    "disk_uuids",
    "full_hostnames",
    "home_directories",
    "ip_addresses",
    "mac_addresses",
    "machine_ids",
    "personal_paths",
    "private_keys",
    "private_paths",
    "raw_process_lists",
    "serial_numbers",
    "usernames",
    "wallet_keys",
];

const REQUIRED_BLOCKED_ACTIONS: &[&str] = &[
    "change_settings",
    "delete_files",
    "download_models",
    "load_models",
    "mutate_vm_or_container",
    "publish_agent",
    "restart_services",
    "run_shell",
    "scan_network",
    "write_files",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopePolicySpec {
    pub package_id: String,
    pub scope_id: String,
    pub task_types: Vec<String>,
    pub in_scope_tasks: Vec<String>,
    pub out_of_scope_tasks: Vec<String>,
    pub allowed_input_classes: Vec<String>,
    pub forbidden_input_classes: Vec<String>,
    pub allowed_operations: Vec<String>,
    pub blocked_actions: Vec<String>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct ScopePolicy {
    package_id: String,
    task_types: HashSet<String>,
    in_scope_tasks: HashSet<String>,
    out_of_scope_tasks: HashSet<String>,
    allowed_input_classes: HashSet<String>,
    forbidden_input_classes: HashSet<String>,
    allowed_operations: HashSet<String>,
    blocked_actions: HashSet<String>,
}

impl fmt::Debug for ScopePolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScopePolicy")
            .field("task_type_count", &self.task_types.len())
            .field("in_scope_task_count", &self.in_scope_tasks.len())
            .field("out_of_scope_task_count", &self.out_of_scope_tasks.len())
            .field("allowed_input_count", &self.allowed_input_classes.len())
            .field("forbidden_input_count", &self.forbidden_input_classes.len())
            .field("allowed_operation_count", &self.allowed_operations.len())
            .field("blocked_action_count", &self.blocked_actions.len())
            .finish()
    }
}

impl TryFrom<ScopePolicySpec> for ScopePolicy {
    type Error = ScopePolicyError;

    fn try_from(spec: ScopePolicySpec) -> Result<Self, Self::Error> {
        if !is_package_identifier(&spec.package_id) || !spec.package_id.starts_with("iamine.") {
            return Err(ScopePolicyError::new(
                ScopePolicyErrorCode::InvalidIdentifier,
                "package_id",
                "package identifier must be bounded and IAMINE-scoped",
            ));
        }
        validate_narrow_identifier("scope_id", &spec.scope_id)?;

        let task_types = validate_collection("task_types", spec.task_types, true)?;
        let in_scope_tasks = validate_collection("in_scope_tasks", spec.in_scope_tasks, true)?;
        let out_of_scope_tasks =
            validate_collection("out_of_scope_tasks", spec.out_of_scope_tasks, false)?;
        let allowed_input_classes =
            validate_collection("allowed_input_classes", spec.allowed_input_classes, false)?;
        let forbidden_input_classes = validate_collection(
            "forbidden_input_classes",
            spec.forbidden_input_classes,
            false,
        )?;
        let allowed_operations =
            validate_collection("allowed_operations", spec.allowed_operations, false)?;
        let blocked_actions = validate_collection("blocked_actions", spec.blocked_actions, false)?;

        ensure_disjoint("task_boundary", &in_scope_tasks, &out_of_scope_tasks)?;
        ensure_disjoint(
            "input_boundary",
            &allowed_input_classes,
            &forbidden_input_classes,
        )?;
        ensure_disjoint("operation_boundary", &allowed_operations, &blocked_actions)?;
        ensure_required(
            "forbidden_input_classes",
            &forbidden_input_classes,
            REQUIRED_FORBIDDEN_INPUTS,
        )?;
        ensure_required(
            "blocked_actions",
            &blocked_actions,
            REQUIRED_BLOCKED_ACTIONS,
        )?;
        ensure_none_present(
            "allowed_input_classes",
            &allowed_input_classes,
            REQUIRED_FORBIDDEN_INPUTS,
        )?;
        ensure_none_present(
            "allowed_operations",
            &allowed_operations,
            REQUIRED_BLOCKED_ACTIONS,
        )?;

        Ok(Self {
            package_id: spec.package_id,
            task_types,
            in_scope_tasks,
            out_of_scope_tasks,
            allowed_input_classes,
            forbidden_input_classes,
            allowed_operations,
            blocked_actions,
        })
    }
}

impl ScopePolicy {
    pub(crate) fn package_matches(&self, package_id: &str) -> bool {
        self.package_id == package_id
    }

    pub(crate) fn supports_task_type(&self, task_type: &str) -> bool {
        self.task_types.contains(task_type)
    }

    pub(crate) fn includes_task(&self, task: &str) -> bool {
        self.in_scope_tasks.contains(task)
    }

    pub(crate) fn excludes_task(&self, task: &str) -> bool {
        self.out_of_scope_tasks.contains(task)
    }

    pub(crate) fn allows_input(&self, input: &str) -> bool {
        self.allowed_input_classes.contains(input)
    }

    pub(crate) fn forbids_input(&self, input: &str) -> bool {
        self.forbidden_input_classes.contains(input)
    }

    pub(crate) fn allows_operation(&self, operation: &str) -> bool {
        self.allowed_operations.contains(operation)
    }

    pub(crate) fn blocks_action(&self, action: &str) -> bool {
        self.blocked_actions.contains(action)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScopePolicyErrorCode {
    InvalidIdentifier,
    InvalidCollection,
    DuplicateValue,
    ContradictoryBoundary,
    MissingSafetyBoundary,
    UnsafeBoundary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopePolicyError {
    code: ScopePolicyErrorCode,
    field: &'static str,
    message: &'static str,
}

impl ScopePolicyError {
    const fn new(code: ScopePolicyErrorCode, field: &'static str, message: &'static str) -> Self {
        Self {
            code,
            field,
            message,
        }
    }

    pub const fn code(&self) -> ScopePolicyErrorCode {
        self.code
    }

    pub const fn field(&self) -> &'static str {
        self.field
    }

    pub const fn message(&self) -> &'static str {
        self.message
    }
}

impl fmt::Display for ScopePolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.field, self.message)
    }
}

impl std::error::Error for ScopePolicyError {}

fn validate_narrow_identifier(field: &'static str, value: &str) -> Result<(), ScopePolicyError> {
    if !is_snake_identifier(value) || BROAD_IDENTIFIERS.contains(&value) {
        return Err(ScopePolicyError::new(
            ScopePolicyErrorCode::InvalidIdentifier,
            field,
            "identifier must be bounded, lowercase, task-scoped snake_case",
        ));
    }
    Ok(())
}

fn validate_collection(
    field: &'static str,
    values: Vec<String>,
    require_narrow: bool,
) -> Result<HashSet<String>, ScopePolicyError> {
    if values.is_empty() || values.len() > MAX_BOUNDARY_ENTRIES {
        return Err(ScopePolicyError::new(
            ScopePolicyErrorCode::InvalidCollection,
            field,
            "collection must be non-empty and bounded",
        ));
    }

    let mut unique = HashSet::with_capacity(values.len());
    for value in values {
        if !is_snake_identifier(&value)
            || (require_narrow && BROAD_IDENTIFIERS.contains(&value.as_str()))
        {
            return Err(ScopePolicyError::new(
                ScopePolicyErrorCode::InvalidIdentifier,
                field,
                "collection values must be bounded, lowercase, narrow snake_case identifiers",
            ));
        }
        if !unique.insert(value) {
            return Err(ScopePolicyError::new(
                ScopePolicyErrorCode::DuplicateValue,
                field,
                "collection values must be unique",
            ));
        }
    }
    Ok(unique)
}

fn ensure_disjoint(
    field: &'static str,
    allowed: &HashSet<String>,
    blocked: &HashSet<String>,
) -> Result<(), ScopePolicyError> {
    if !allowed.is_disjoint(blocked) {
        return Err(ScopePolicyError::new(
            ScopePolicyErrorCode::ContradictoryBoundary,
            field,
            "allowed and blocked declarations must not overlap",
        ));
    }
    Ok(())
}

fn ensure_required(
    field: &'static str,
    values: &HashSet<String>,
    required: &[&str],
) -> Result<(), ScopePolicyError> {
    if required.iter().any(|value| !values.contains(*value)) {
        return Err(ScopePolicyError::new(
            ScopePolicyErrorCode::MissingSafetyBoundary,
            field,
            "required deny-by-default safety entries are missing",
        ));
    }
    Ok(())
}

fn ensure_none_present(
    field: &'static str,
    values: &HashSet<String>,
    blocked: &[&str],
) -> Result<(), ScopePolicyError> {
    if blocked.iter().any(|value| values.contains(*value)) {
        return Err(ScopePolicyError::new(
            ScopePolicyErrorCode::UnsafeBoundary,
            field,
            "unsafe input or operation cannot be allowed",
        ));
    }
    Ok(())
}
