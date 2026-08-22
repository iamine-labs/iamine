use std::path::Path;

use iamine_agent_runtime::InputClassification;
use serde::Serialize;

use crate::official_agent_execution::{
    execute_official_local_readonly_agent, OfficialAgentExecutionError, OfficialAgentExecutionSpec,
};

use super::{
    package::VerifiedReporterPackage, policy::runtime_policies, register_reporter_program,
    ReporterAgentError, ReporterAgentErrorCode, ReporterInput, ReporterReport,
    REPORTER_OUTPUT_SCHEMA_VERSION, REPORTER_PACKAGE_ID, REPORTER_SCOPE_ID, REPORTER_TASK_TYPE,
};

const INPUT_CLASSES: [&str; 1] = ["operator_approved_redacted_evidence"];
const REQUIRED_CATEGORIES: [&str; 2] = ["local_readonly", "redacted_status_summary"];
const EXECUTION_SPEC: OfficialAgentExecutionSpec = OfficialAgentExecutionSpec {
    package_id: REPORTER_PACKAGE_ID,
    task_type: REPORTER_TASK_TYPE,
    scope_id: REPORTER_SCOPE_ID,
    task_name: "format_privacy_safe_support_report",
    operation: "format_operator_visible_report",
    input_classes: &INPUT_CLASSES,
    required_categories: &REQUIRED_CATEGORIES,
    routing_candidate_id: "support-reporter-local",
    input_classification: InputClassification::OperatorIntent,
    max_input_bytes: 2_048,
    register_program: register_reporter_program,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ReporterAgentExecution {
    pub(crate) schema_version: &'static str,
    pub(crate) package_id: &'static str,
    pub(crate) task_type: &'static str,
    pub(crate) scope_id: &'static str,
    pub(crate) status: &'static str,
    pub(crate) classification: &'static str,
    pub(crate) report: ReporterReport,
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

pub(crate) fn execute_reporter_agent(
    package_root: &Path,
    input: &ReporterInput,
) -> Result<ReporterAgentExecution, ReporterAgentError> {
    input
        .validate()
        .map_err(|_| ReporterAgentError::new(ReporterAgentErrorCode::InputInvalid))?;
    let serialized = serde_json::to_string(input)
        .map_err(|_| ReporterAgentError::new(ReporterAgentErrorCode::InputInvalid))?;
    let package = VerifiedReporterPackage::load(package_root)?;
    let (scope_policy, permission_policy) = runtime_policies()?;
    let result = execute_official_local_readonly_agent(
        package.subject(),
        scope_policy,
        permission_policy,
        &serialized,
        &EXECUTION_SPEC,
    )
    .map_err(map_execution_error)?;
    let report: ReporterReport = serde_json::from_str(&result.content)
        .map_err(|_| ReporterAgentError::new(ReporterAgentErrorCode::OutputVerificationFailed))?;
    if report.schema_version != REPORTER_OUTPUT_SCHEMA_VERSION
        || report.classification.as_str() != result.classification.as_str()
        || report.evidence != input.evidence
    {
        return Err(ReporterAgentError::new(
            ReporterAgentErrorCode::OutputVerificationFailed,
        ));
    }

    Ok(ReporterAgentExecution {
        schema_version: REPORTER_OUTPUT_SCHEMA_VERSION,
        package_id: REPORTER_PACKAGE_ID,
        task_type: REPORTER_TASK_TYPE,
        scope_id: REPORTER_SCOPE_ID,
        status: "completed",
        classification: result.classification.as_str(),
        report,
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

const fn map_execution_error(error: OfficialAgentExecutionError) -> ReporterAgentError {
    match error {
        OfficialAgentExecutionError::RuntimeRejected => {
            ReporterAgentError::new(ReporterAgentErrorCode::RuntimeRejected)
        }
        OfficialAgentExecutionError::OutputVerificationFailed => {
            ReporterAgentError::new(ReporterAgentErrorCode::OutputVerificationFailed)
        }
    }
}
