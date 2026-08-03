use iamine_agents::{
    parse_permission_policy_yaml, parse_scope_policy_yaml, PermissionDefaultPolicy,
    PermissionPolicy, PermissionPolicySpec, ScopePolicy, ScopePolicySpec,
};

use super::{
    package::{PERMISSIONS, SCOPE},
    NodeDoctorAgentError, NodeDoctorAgentErrorCode,
};

pub(super) fn runtime_policies() -> Result<(ScopePolicy, PermissionPolicy), NodeDoctorAgentError> {
    let scope_metadata = parse_scope_policy_yaml(SCOPE)
        .map_err(|_| NodeDoctorAgentError::new(NodeDoctorAgentErrorCode::PackageInvalid))?;
    let permission_metadata = parse_permission_policy_yaml(PERMISSIONS)
        .map_err(|_| NodeDoctorAgentError::new(NodeDoctorAgentErrorCode::PackageInvalid))?;

    let scope = ScopePolicy::try_from(ScopePolicySpec {
        package_id: scope_metadata.package_id.clone(),
        scope_id: scope_metadata.scope_id.clone(),
        task_types: scope_metadata.task_boundary.task_types.clone(),
        in_scope_tasks: scope_metadata.task_boundary.in_scope.clone(),
        out_of_scope_tasks: scope_metadata.task_boundary.out_of_scope.clone(),
        allowed_input_classes: scope_metadata.input_boundary.allowed_inputs.clone(),
        forbidden_input_classes: scope_metadata.input_boundary.forbidden_inputs.clone(),
        allowed_operations: scope_metadata.operation_boundary.allowed_operations.clone(),
        blocked_actions: scope_metadata.operation_boundary.blocked_actions.clone(),
    })
    .map_err(|_| NodeDoctorAgentError::new(NodeDoctorAgentErrorCode::PackageInvalid))?;

    let mut approved_actions = scope_metadata.operation_boundary.allowed_operations;
    approved_actions.extend(
        permission_metadata
            .confirmation_requirements
            .requires_confirmation_for
            .clone(),
    );
    let permission = PermissionPolicy::try_from(PermissionPolicySpec {
        package_id: permission_metadata.package_id,
        permission_profile_id: permission_metadata.permission_profile_id,
        default_policy: match permission_metadata.default_policy {
            iamine_agents::PermissionDefaultPolicyMetadata::Deny => PermissionDefaultPolicy::Deny,
            iamine_agents::PermissionDefaultPolicyMetadata::Allow => PermissionDefaultPolicy::Allow,
        },
        approved_categories: permission_metadata.requested_categories,
        forbidden_categories: permission_metadata.forbidden_categories,
        approved_actions,
        blocked_actions: permission_metadata.blocked_actions,
        confirmation_required_categories: vec!["user_provided_text".to_string()],
        confirmation_required_actions: permission_metadata
            .confirmation_requirements
            .requires_confirmation_for,
    })
    .map_err(|_| NodeDoctorAgentError::new(NodeDoctorAgentErrorCode::PackageInvalid))?;

    Ok((scope, permission))
}
