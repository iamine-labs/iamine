mod execution;
mod input;
mod package;
mod policy;

use std::{fmt, path::Path};

use iamine_agent_runtime::{
    OfficialRustProgram, OfficialRustProgramFailure, OfficialRustProgramFailureCode,
    OfficialRustProgramOutput, OfficialRustProgramRegistry, OutputClassification,
    PackageReviewSubject, RuntimeExecutionContext,
};
use serde::{Deserialize, Serialize};

pub(crate) use execution::execute_photo_library_organizer_agent;
pub(crate) use input::{
    PhotoEvidenceClaim, PhotoEvidenceStatus, PhotoInventoryEvidence, PhotoLibraryCliCommand,
    PhotoLibraryInput, PhotoOrganizationIntent,
};
#[cfg(test)]
pub(crate) use input::{PhotoInventoryCategory, PhotoInventoryItemLabel};

pub(crate) const PHOTO_LIBRARY_PACKAGE_ID: &str = "iamine.beta.photo-library-organizer";
pub(crate) const PHOTO_LIBRARY_TASK_TYPE: &str = "photo_library_organizer_review";
pub(crate) const PHOTO_LIBRARY_SCOPE_ID: &str = "photo_library_organizer_review";
pub(crate) const PHOTO_LIBRARY_TASK_INPUT: &str = "review_declared_photo_inventory";
pub(crate) const PHOTO_LIBRARY_INPUT_SCHEMA_VERSION: &str =
    "iamine.agent.photo-library-organizer.input-0.1";
pub(crate) const PHOTO_LIBRARY_OUTPUT_SCHEMA_VERSION: &str =
    "iamine.agent.photo-library-organizer.output-0.1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PhotoLibraryAgentErrorCode {
    PackageUnavailable,
    PackageInvalid,
    PackageMismatch,
    InputInvalid,
    RuntimeRejected,
    OutputVerificationFailed,
}

impl PhotoLibraryAgentErrorCode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::PackageUnavailable => "package_unavailable",
            Self::PackageInvalid => "package_invalid",
            Self::PackageMismatch => "package_mismatch",
            Self::InputInvalid => "input_invalid",
            Self::RuntimeRejected => "runtime_rejected",
            Self::OutputVerificationFailed => "output_verification_failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PhotoLibraryAgentError {
    code: PhotoLibraryAgentErrorCode,
}

impl PhotoLibraryAgentError {
    pub(crate) const fn new(code: PhotoLibraryAgentErrorCode) -> Self {
        Self { code }
    }

    #[cfg(test)]
    pub(crate) const fn code(self) -> PhotoLibraryAgentErrorCode {
        self.code
    }
}

impl fmt::Display for PhotoLibraryAgentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.as_str())
    }
}

impl std::error::Error for PhotoLibraryAgentError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PhotoLibraryReportClassification {
    PhotoLibraryReview,
    BlockedActionReport,
    HandoffRequest,
}

impl PhotoLibraryReportClassification {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::PhotoLibraryReview => "photo_library_organization_review",
            Self::BlockedActionReport => "blocked_action_report",
            Self::HandoffRequest => "handoff_request",
        }
    }

    pub(crate) const fn runtime_classification(self) -> OutputClassification {
        match self {
            Self::PhotoLibraryReview => OutputClassification::ResultSummary,
            Self::BlockedActionReport => OutputClassification::BlockedActionReport,
            Self::HandoffRequest => OutputClassification::HandoffRequest,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PhotoLibraryNextStep {
    NoActionRequired,
    ReviewAttentionInventoryMetadata,
    ReviewBlockedInventoryMetadata,
    ProvideRedactedInventoryMetadata,
    HandoffForPhotoOrFilesystemAction,
}

impl PhotoLibraryNextStep {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::NoActionRequired => "no_action_required",
            Self::ReviewAttentionInventoryMetadata => "review_attention_inventory_metadata",
            Self::ReviewBlockedInventoryMetadata => "review_blocked_inventory_metadata",
            Self::ProvideRedactedInventoryMetadata => "provide_redacted_inventory_metadata",
            Self::HandoffForPhotoOrFilesystemAction => "handoff_for_photo_or_filesystem_action",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PhotoLibraryReport {
    pub(crate) schema_version: String,
    pub(crate) classification: PhotoLibraryReportClassification,
    pub(crate) items: Vec<PhotoInventoryEvidence>,
    pub(crate) intent: Option<PhotoOrganizationIntent>,
    pub(crate) next_step: PhotoLibraryNextStep,
}

pub(crate) fn run_photo_library_organizer_agent_cli(
    command: &PhotoLibraryCliCommand,
) -> Result<(), PhotoLibraryAgentError> {
    let execution =
        execute_photo_library_organizer_agent(Path::new(&command.package_root), &command.input())?;
    if command.json {
        let output = serde_json::to_string_pretty(&execution).map_err(|_| {
            PhotoLibraryAgentError::new(PhotoLibraryAgentErrorCode::OutputVerificationFailed)
        })?;
        println!("{output}");
    } else {
        println!("Photo Library Organizer");
        println!("status: {}", execution.status);
        println!("classification: {}", execution.classification);
        println!("item_count: {}", execution.report.items.len());
        println!(
            "intent: {}",
            execution
                .report
                .intent
                .map_or("none", PhotoOrganizationIntent::as_str)
        );
        println!("next_step: {}", execution.report.next_step.as_str());
    }
    Ok(())
}

pub(crate) fn register_photo_library_program<'subject>(
    registry: &OfficialRustProgramRegistry,
    subject: PackageReviewSubject<'subject>,
) -> Result<OfficialRustProgram<'subject>, OfficialRustProgramFailure> {
    if subject.package_id() != PHOTO_LIBRARY_PACKAGE_ID
        || subject.task_type() != PHOTO_LIBRARY_TASK_TYPE
    {
        return Err(OfficialRustProgramFailure::new(
            OfficialRustProgramFailureCode::RejectedInput,
        ));
    }
    Ok(registry.register(subject, photo_library_official_program))
}

fn photo_library_official_program(
    context: &RuntimeExecutionContext<'_>,
    input: &str,
) -> Result<OfficialRustProgramOutput, OfficialRustProgramFailure> {
    context.checkpoint()?;
    if context.network_allowed()
        || context.shell_allowed()
        || context.child_processes_allowed()
        || context.persistence_allowed()
    {
        return Err(rejected_input());
    }

    let input: PhotoLibraryInput = serde_json::from_str(input).map_err(|_| rejected_input())?;
    input.validate().map_err(|_| rejected_input())?;
    let report = build_report(input);
    let content = serde_json::to_string(&report).map_err(|_| rejected_input())?;
    context.checkpoint()?;
    Ok(OfficialRustProgramOutput::operator_reviewed(
        report.classification.runtime_classification(),
        content,
    ))
}

fn build_report(input: PhotoLibraryInput) -> PhotoLibraryReport {
    let has_unsupported = input
        .items
        .iter()
        .any(|item| item.claim == PhotoEvidenceClaim::UnsupportedClaim);
    let has_missing = input.items.is_empty()
        || input
            .items
            .iter()
            .any(|item| item.status == PhotoEvidenceStatus::Missing);
    let has_blocked = input
        .items
        .iter()
        .any(|item| item.status == PhotoEvidenceStatus::Blocked);
    let has_attention = input
        .items
        .iter()
        .any(|item| item.status == PhotoEvidenceStatus::Attention);

    let (classification, next_step) = if has_unsupported {
        (
            PhotoLibraryReportClassification::HandoffRequest,
            PhotoLibraryNextStep::HandoffForPhotoOrFilesystemAction,
        )
    } else if has_missing {
        (
            PhotoLibraryReportClassification::BlockedActionReport,
            PhotoLibraryNextStep::ProvideRedactedInventoryMetadata,
        )
    } else if has_blocked {
        (
            PhotoLibraryReportClassification::PhotoLibraryReview,
            PhotoLibraryNextStep::ReviewBlockedInventoryMetadata,
        )
    } else if has_attention {
        (
            PhotoLibraryReportClassification::PhotoLibraryReview,
            PhotoLibraryNextStep::ReviewAttentionInventoryMetadata,
        )
    } else {
        (
            PhotoLibraryReportClassification::PhotoLibraryReview,
            PhotoLibraryNextStep::NoActionRequired,
        )
    };

    PhotoLibraryReport {
        schema_version: PHOTO_LIBRARY_OUTPUT_SCHEMA_VERSION.to_string(),
        classification,
        items: input.items,
        intent: input.intent,
        next_step,
    }
}

const fn rejected_input() -> OfficialRustProgramFailure {
    OfficialRustProgramFailure::new(OfficialRustProgramFailureCode::RejectedInput)
}

#[cfg(test)]
mod tests;
