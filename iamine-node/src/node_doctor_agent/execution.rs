use std::path::Path;

use iamine_agent_runtime::InputClassification;
use serde::Serialize;

use crate::official_agent_execution::{
    execute_official_local_readonly_agent, OfficialAgentExecutionError, OfficialAgentExecutionSpec,
};

use super::{
    package::VerifiedNodeDoctorPackage, policy::runtime_policies, register_node_doctor_program,
    NodeDoctorAgentError, NodeDoctorAgentErrorCode, NODE_DOCTOR_OUTPUT_SCHEMA_VERSION,
    NODE_DOCTOR_PACKAGE_ID, NODE_DOCTOR_SCOPE_ID, NODE_DOCTOR_TASK_INPUT, NODE_DOCTOR_TASK_TYPE,
};

const INPUT_CLASSES: [&str; 1] = ["iamine_node_status_summary"];
const REQUIRED_CATEGORIES: [&str; 2] = ["local_readonly", "redacted_status_summary"];
const EXECUTION_SPEC: OfficialAgentExecutionSpec = OfficialAgentExecutionSpec {
    package_id: NODE_DOCTOR_PACKAGE_ID,
    task_type: NODE_DOCTOR_TASK_TYPE,
    scope_id: NODE_DOCTOR_SCOPE_ID,
    task_name: NODE_DOCTOR_TASK_INPUT,
    operation: "read_declared_summary",
    input_classes: &INPUT_CLASSES,
    required_categories: &REQUIRED_CATEGORIES,
    routing_candidate_id: "node-doctor-local",
    input_classification: InputClassification::TaskDescriptor,
    max_input_bytes: 256,
    execution_timeout_ms: 5_000,
    register_program: register_node_doctor_program,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct NodeDoctorAgentExecution {
    pub(crate) schema_version: &'static str,
    pub(crate) package_id: &'static str,
    pub(crate) task_type: &'static str,
    pub(crate) scope_id: &'static str,
    pub(crate) status: &'static str,
    pub(crate) classification: &'static str,
    pub(crate) content: String,
    pub(crate) package_loaded: bool,
    pub(crate) execution_authorized: bool,
    pub(crate) sandbox_adapter_was_active: bool,
    pub(crate) os_isolation_claimed: bool,
    pub(crate) cleanup_completed: bool,
    pub(crate) audit_recorded: bool,
    pub(crate) scheduler_mutated: bool,
    pub(crate) transport_started: bool,
    pub(crate) persisted: bool,
}

pub(crate) fn execute_node_doctor_agent(
    package_root: &Path,
) -> Result<NodeDoctorAgentExecution, NodeDoctorAgentError> {
    let package = VerifiedNodeDoctorPackage::load(package_root)?;
    let (scope_policy, permission_policy) = runtime_policies()?;
    let result = execute_official_local_readonly_agent(
        package.subject(),
        scope_policy,
        permission_policy,
        NODE_DOCTOR_TASK_INPUT,
        &EXECUTION_SPEC,
    )
    .map_err(map_execution_error)?;

    Ok(NodeDoctorAgentExecution {
        schema_version: NODE_DOCTOR_OUTPUT_SCHEMA_VERSION,
        package_id: NODE_DOCTOR_PACKAGE_ID,
        task_type: NODE_DOCTOR_TASK_TYPE,
        scope_id: NODE_DOCTOR_SCOPE_ID,
        status: "completed",
        classification: result.classification.as_str(),
        content: result.content,
        package_loaded: result.package_loaded,
        execution_authorized: result.execution_authorized,
        sandbox_adapter_was_active: result.sandbox_adapter_was_active,
        os_isolation_claimed: result.os_isolation_claimed,
        cleanup_completed: result.cleanup_completed,
        audit_recorded: result.audit_recorded,
        scheduler_mutated: result.scheduler_mutated,
        transport_started: result.transport_started,
        persisted: result.persisted,
    })
}

const fn map_execution_error(error: OfficialAgentExecutionError) -> NodeDoctorAgentError {
    match error {
        OfficialAgentExecutionError::RuntimeRejected => {
            NodeDoctorAgentError::new(NodeDoctorAgentErrorCode::RuntimeRejected)
        }
        OfficialAgentExecutionError::OutputVerificationFailed => {
            NodeDoctorAgentError::new(NodeDoctorAgentErrorCode::OutputVerificationFailed)
        }
    }
}
