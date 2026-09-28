use std::path::Path;

use iamine_agent_runtime::InputClassification;
use serde::Serialize;

use crate::official_agent_execution::{
    execute_official_local_readonly_agent, OfficialAgentExecutionError, OfficialAgentExecutionSpec,
};

use super::{
    package::VerifiedPhotoLibraryPackage, policy::runtime_policies, register_photo_library_program,
    PhotoLibraryAgentError, PhotoLibraryAgentErrorCode, PhotoLibraryInput, PhotoLibraryReport,
    PHOTO_LIBRARY_OUTPUT_SCHEMA_VERSION, PHOTO_LIBRARY_PACKAGE_ID, PHOTO_LIBRARY_SCOPE_ID,
    PHOTO_LIBRARY_TASK_INPUT, PHOTO_LIBRARY_TASK_TYPE,
};

const INPUT_CLASSES: [&str; 3] = [
    "operator_declared_photo_inventory_summary",
    "redacted_photo_inventory_summary",
    "redacted_organization_intent_summary",
];
const REQUIRED_CATEGORIES: [&str; 2] = ["local_readonly", "redacted_status_summary"];
const EXECUTION_SPEC: OfficialAgentExecutionSpec = OfficialAgentExecutionSpec {
    package_id: PHOTO_LIBRARY_PACKAGE_ID,
    task_type: PHOTO_LIBRARY_TASK_TYPE,
    scope_id: PHOTO_LIBRARY_SCOPE_ID,
    task_name: PHOTO_LIBRARY_TASK_INPUT,
    operation: "review_declared_photo_inventory",
    input_classes: &INPUT_CLASSES,
    required_categories: &REQUIRED_CATEGORIES,
    routing_candidate_id: "photo-library-organizer-local",
    input_classification: InputClassification::OperatorIntent,
    max_input_bytes: 4_096,
    execution_timeout_ms: 1_000,
    register_program: register_photo_library_program,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct PhotoLibraryAgentExecution {
    pub(crate) schema_version: &'static str,
    pub(crate) package_id: &'static str,
    pub(crate) task_type: &'static str,
    pub(crate) scope_id: &'static str,
    pub(crate) status: &'static str,
    pub(crate) classification: &'static str,
    pub(crate) report: PhotoLibraryReport,
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

pub(crate) fn execute_photo_library_organizer_agent(
    package_root: &Path,
    input: &PhotoLibraryInput,
) -> Result<PhotoLibraryAgentExecution, PhotoLibraryAgentError> {
    input
        .validate()
        .map_err(|_| PhotoLibraryAgentError::new(PhotoLibraryAgentErrorCode::InputInvalid))?;
    let serialized = serde_json::to_string(input)
        .map_err(|_| PhotoLibraryAgentError::new(PhotoLibraryAgentErrorCode::InputInvalid))?;
    let package = VerifiedPhotoLibraryPackage::load(package_root)?;
    let (scope_policy, permission_policy) = runtime_policies()?;
    let result = execute_official_local_readonly_agent(
        package.subject(),
        scope_policy,
        permission_policy,
        &serialized,
        &EXECUTION_SPEC,
    )
    .map_err(map_execution_error)?;
    let report: PhotoLibraryReport = serde_json::from_str(&result.content).map_err(|_| {
        PhotoLibraryAgentError::new(PhotoLibraryAgentErrorCode::OutputVerificationFailed)
    })?;
    if report.schema_version != PHOTO_LIBRARY_OUTPUT_SCHEMA_VERSION
        || report.classification.runtime_classification() != result.classification
        || report.items != input.items
        || report.intent != input.intent
    {
        return Err(PhotoLibraryAgentError::new(
            PhotoLibraryAgentErrorCode::OutputVerificationFailed,
        ));
    }

    Ok(PhotoLibraryAgentExecution {
        schema_version: PHOTO_LIBRARY_OUTPUT_SCHEMA_VERSION,
        package_id: PHOTO_LIBRARY_PACKAGE_ID,
        task_type: PHOTO_LIBRARY_TASK_TYPE,
        scope_id: PHOTO_LIBRARY_SCOPE_ID,
        status: "completed",
        classification: report.classification.as_str(),
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

const fn map_execution_error(error: OfficialAgentExecutionError) -> PhotoLibraryAgentError {
    match error {
        OfficialAgentExecutionError::RuntimeRejected => {
            PhotoLibraryAgentError::new(PhotoLibraryAgentErrorCode::RuntimeRejected)
        }
        OfficialAgentExecutionError::OutputVerificationFailed => {
            PhotoLibraryAgentError::new(PhotoLibraryAgentErrorCode::OutputVerificationFailed)
        }
    }
}
