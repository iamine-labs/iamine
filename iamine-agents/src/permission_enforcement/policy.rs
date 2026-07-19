use std::collections::HashSet;
use std::fmt;

use crate::identifiers::{is_package_identifier, is_snake_identifier};

const MAX_POLICY_ENTRIES: usize = 64;

const BROAD_IDENTIFIERS: &[&str] = &[
    "admin",
    "all",
    "all_access",
    "all_actions",
    "anything",
    "do_anything",
    "root",
    "system_control",
];

const SUPPORTED_APPROVED_CATEGORIES: &[&str] = &[
    "lan_readonly_metadata",
    "local_readonly",
    "package_relative_review_files",
    "redacted_status_summary",
    "user_provided_text",
];

const REQUIRED_FORBIDDEN_CATEGORIES: &[&str] = &[
    "arbitrary_shell",
    "credential_access",
    "destructive_write",
    "mainnet_operation",
    "marketplace_publish",
    "model_download",
    "model_load",
    "network_mutation",
    "private_key_access",
    "service_mutation",
    "unrestricted_filesystem",
    "vm_or_container_mutation",
    "wallet_access",
];

const REQUIRED_BLOCKED_ACTIONS: &[&str] = &[
    "access_private_keys",
    "access_wallet",
    "collect_credentials",
    "delete_files",
    "download_models",
    "load_models",
    "mainnet_operation",
    "mutate_network",
    "mutate_vm_or_container",
    "publish_agent",
    "read_private_files",
    "restart_services",
    "reward_operation",
    "run_shell",
    "settlement_operation",
    "token_operation",
    "write_files",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PermissionDefaultPolicy {
    Deny,
    Allow,
}

impl PermissionDefaultPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Deny => "deny",
            Self::Allow => "allow",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct PermissionPolicySpec {
    pub package_id: String,
    pub permission_profile_id: String,
    pub default_policy: PermissionDefaultPolicy,
    pub approved_categories: Vec<String>,
    pub forbidden_categories: Vec<String>,
    pub approved_actions: Vec<String>,
    pub blocked_actions: Vec<String>,
    pub confirmation_required_categories: Vec<String>,
    pub confirmation_required_actions: Vec<String>,
}

impl fmt::Debug for PermissionPolicySpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PermissionPolicySpec")
            .field("default_policy", &self.default_policy)
            .field("approved_category_count", &self.approved_categories.len())
            .field("forbidden_category_count", &self.forbidden_categories.len())
            .field("approved_action_count", &self.approved_actions.len())
            .field("blocked_action_count", &self.blocked_actions.len())
            .field(
                "confirmation_category_count",
                &self.confirmation_required_categories.len(),
            )
            .field(
                "confirmation_action_count",
                &self.confirmation_required_actions.len(),
            )
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct PermissionPolicy {
    package_id: String,
    approved_categories: HashSet<String>,
    forbidden_categories: HashSet<String>,
    approved_actions: HashSet<String>,
    blocked_actions: HashSet<String>,
    confirmation_required_categories: HashSet<String>,
    confirmation_required_actions: HashSet<String>,
}

impl fmt::Debug for PermissionPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PermissionPolicy")
            .field("approved_category_count", &self.approved_categories.len())
            .field("forbidden_category_count", &self.forbidden_categories.len())
            .field("approved_action_count", &self.approved_actions.len())
            .field("blocked_action_count", &self.blocked_actions.len())
            .field(
                "confirmation_category_count",
                &self.confirmation_required_categories.len(),
            )
            .field(
                "confirmation_action_count",
                &self.confirmation_required_actions.len(),
            )
            .finish()
    }
}

impl TryFrom<PermissionPolicySpec> for PermissionPolicy {
    type Error = PermissionPolicyError;

    fn try_from(spec: PermissionPolicySpec) -> Result<Self, Self::Error> {
        if !is_package_identifier(&spec.package_id) || !spec.package_id.starts_with("iamine.") {
            return Err(PermissionPolicyError::new(
                PermissionPolicyErrorCode::InvalidIdentifier,
                "package_id",
                "package identifier must be bounded and IAMINE-scoped",
            ));
        }
        validate_narrow_identifier("permission_profile_id", &spec.permission_profile_id)?;
        if spec.default_policy != PermissionDefaultPolicy::Deny {
            return Err(PermissionPolicyError::new(
                PermissionPolicyErrorCode::PermissiveDefault,
                "default_policy",
                "default permission policy must deny",
            ));
        }

        let approved_categories =
            validate_collection("approved_categories", spec.approved_categories, false)?;
        let forbidden_categories =
            validate_collection("forbidden_categories", spec.forbidden_categories, false)?;
        let approved_actions =
            validate_collection("approved_actions", spec.approved_actions, false)?;
        let blocked_actions = validate_collection("blocked_actions", spec.blocked_actions, false)?;
        let confirmation_required_categories = validate_collection(
            "confirmation_required_categories",
            spec.confirmation_required_categories,
            true,
        )?;
        let confirmation_required_actions = validate_collection(
            "confirmation_required_actions",
            spec.confirmation_required_actions,
            true,
        )?;

        ensure_supported_categories(&approved_categories)?;
        ensure_disjoint(
            "category_boundary",
            &approved_categories,
            &forbidden_categories,
        )?;
        ensure_disjoint("action_boundary", &approved_actions, &blocked_actions)?;
        ensure_required(
            "forbidden_categories",
            &forbidden_categories,
            REQUIRED_FORBIDDEN_CATEGORIES,
        )?;
        ensure_required(
            "blocked_actions",
            &blocked_actions,
            REQUIRED_BLOCKED_ACTIONS,
        )?;
        ensure_subset(
            "confirmation_required_categories",
            &confirmation_required_categories,
            &approved_categories,
        )?;
        ensure_subset(
            "confirmation_required_actions",
            &confirmation_required_actions,
            &approved_actions,
        )?;

        Ok(Self {
            package_id: spec.package_id,
            approved_categories,
            forbidden_categories,
            approved_actions,
            blocked_actions,
            confirmation_required_categories,
            confirmation_required_actions,
        })
    }
}

impl PermissionPolicy {
    pub(crate) fn package_matches(&self, package_id: &str) -> bool {
        self.package_id == package_id
    }

    pub(crate) fn approves_category(&self, category: &str) -> bool {
        self.approved_categories.contains(category)
    }

    pub(crate) fn forbids_category(&self, category: &str) -> bool {
        self.forbidden_categories.contains(category)
    }

    pub(crate) fn approves_action(&self, action: &str) -> bool {
        self.approved_actions.contains(action)
    }

    pub(crate) fn blocks_action(&self, action: &str) -> bool {
        self.blocked_actions.contains(action)
    }

    pub(crate) fn requires_confirmation(&self, action: &str, categories: &[&str]) -> bool {
        self.confirmation_required_actions.contains(action)
            || categories
                .iter()
                .any(|category| self.confirmation_required_categories.contains(*category))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PermissionPolicyErrorCode {
    InvalidIdentifier,
    PermissiveDefault,
    InvalidCollection,
    DuplicateValue,
    ContradictoryBoundary,
    MissingSafetyBoundary,
    UnsupportedPermission,
    InvalidConfirmationBoundary,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionPolicyError {
    code: PermissionPolicyErrorCode,
    field: &'static str,
    message: &'static str,
}

impl PermissionPolicyError {
    const fn new(
        code: PermissionPolicyErrorCode,
        field: &'static str,
        message: &'static str,
    ) -> Self {
        Self {
            code,
            field,
            message,
        }
    }

    pub const fn code(&self) -> PermissionPolicyErrorCode {
        self.code
    }

    pub const fn field(&self) -> &'static str {
        self.field
    }

    pub const fn message(&self) -> &'static str {
        self.message
    }
}

impl fmt::Display for PermissionPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.field, self.message)
    }
}

impl std::error::Error for PermissionPolicyError {}

fn validate_narrow_identifier(
    field: &'static str,
    value: &str,
) -> Result<(), PermissionPolicyError> {
    if !is_snake_identifier(value) || BROAD_IDENTIFIERS.contains(&value) {
        return Err(PermissionPolicyError::new(
            PermissionPolicyErrorCode::InvalidIdentifier,
            field,
            "identifier must be bounded, lowercase, narrow snake_case",
        ));
    }
    Ok(())
}

fn validate_collection(
    field: &'static str,
    values: Vec<String>,
    allow_empty: bool,
) -> Result<HashSet<String>, PermissionPolicyError> {
    if (!allow_empty && values.is_empty()) || values.len() > MAX_POLICY_ENTRIES {
        return Err(PermissionPolicyError::new(
            PermissionPolicyErrorCode::InvalidCollection,
            field,
            "collection must be bounded and non-empty when required",
        ));
    }

    let mut unique = HashSet::with_capacity(values.len());
    for value in values {
        validate_narrow_identifier(field, &value)?;
        if !unique.insert(value) {
            return Err(PermissionPolicyError::new(
                PermissionPolicyErrorCode::DuplicateValue,
                field,
                "collection values must be unique",
            ));
        }
    }
    Ok(unique)
}

fn ensure_supported_categories(approved: &HashSet<String>) -> Result<(), PermissionPolicyError> {
    if approved
        .iter()
        .any(|category| !SUPPORTED_APPROVED_CATEGORIES.contains(&category.as_str()))
    {
        return Err(PermissionPolicyError::new(
            PermissionPolicyErrorCode::UnsupportedPermission,
            "approved_categories",
            "approved permission category is unavailable in this release phase",
        ));
    }
    Ok(())
}

fn ensure_disjoint(
    field: &'static str,
    approved: &HashSet<String>,
    blocked: &HashSet<String>,
) -> Result<(), PermissionPolicyError> {
    if !approved.is_disjoint(blocked) {
        return Err(PermissionPolicyError::new(
            PermissionPolicyErrorCode::ContradictoryBoundary,
            field,
            "approved and blocked declarations must not overlap",
        ));
    }
    Ok(())
}

fn ensure_required(
    field: &'static str,
    values: &HashSet<String>,
    required: &[&str],
) -> Result<(), PermissionPolicyError> {
    if required.iter().any(|value| !values.contains(*value)) {
        return Err(PermissionPolicyError::new(
            PermissionPolicyErrorCode::MissingSafetyBoundary,
            field,
            "required deny-by-default safety entries are missing",
        ));
    }
    Ok(())
}

fn ensure_subset(
    field: &'static str,
    confirmation_required: &HashSet<String>,
    approved: &HashSet<String>,
) -> Result<(), PermissionPolicyError> {
    if !confirmation_required.is_subset(approved) {
        return Err(PermissionPolicyError::new(
            PermissionPolicyErrorCode::InvalidConfirmationBoundary,
            field,
            "confirmation can only apply to an already approved permission",
        ));
    }
    Ok(())
}
