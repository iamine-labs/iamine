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

pub(crate) use execution::execute_lan_file_share_agent;
#[cfg(test)]
pub(crate) use input::ShareSelector;
pub(crate) use input::{
    LanFileShareCliCommand, LanFileShareInput, LanShareEvidence, ShareEvidenceClaim,
    ShareEvidenceStatus,
};

pub(crate) const LAN_FILE_SHARE_PACKAGE_ID: &str = "iamine.beta.lan-file-share-assistant";
pub(crate) const LAN_FILE_SHARE_TASK_TYPE: &str = "file_share_readonly_review";
pub(crate) const LAN_FILE_SHARE_SCOPE_ID: &str = "lan_file_share_readonly_review";
pub(crate) const LAN_FILE_SHARE_TASK_INPUT: &str = "review_declared_share_metadata";
pub(crate) const LAN_FILE_SHARE_INPUT_SCHEMA_VERSION: &str =
    "iamine.agent.lan-file-share-assistant.input-0.1";
pub(crate) const LAN_FILE_SHARE_OUTPUT_SCHEMA_VERSION: &str =
    "iamine.agent.lan-file-share-assistant.output-0.1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LanFileShareAgentErrorCode {
    PackageUnavailable,
    PackageInvalid,
    PackageMismatch,
    InputInvalid,
    RuntimeRejected,
    OutputVerificationFailed,
}

impl LanFileShareAgentErrorCode {
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
pub(crate) struct LanFileShareAgentError {
    code: LanFileShareAgentErrorCode,
}

impl LanFileShareAgentError {
    pub(crate) const fn new(code: LanFileShareAgentErrorCode) -> Self {
        Self { code }
    }

    #[cfg(test)]
    pub(crate) const fn code(self) -> LanFileShareAgentErrorCode {
        self.code
    }
}

impl fmt::Display for LanFileShareAgentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.as_str())
    }
}

impl std::error::Error for LanFileShareAgentError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LanFileShareReportClassification {
    FileShareReview,
    BlockedActionReport,
    HandoffRequest,
}

impl LanFileShareReportClassification {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::FileShareReview => "file_share_review",
            Self::BlockedActionReport => "blocked_action_report",
            Self::HandoffRequest => "handoff_request",
        }
    }

    pub(crate) const fn runtime_classification(self) -> OutputClassification {
        match self {
            Self::FileShareReview => OutputClassification::ResultSummary,
            Self::BlockedActionReport => OutputClassification::BlockedActionReport,
            Self::HandoffRequest => OutputClassification::HandoffRequest,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LanFileShareNextStep {
    NoActionRequired,
    ReviewAttentionShareMetadata,
    ReviewBlockedShareMetadata,
    ProvideRedactedShareMetadata,
    HandoffForFileOrNetworkAction,
}

impl LanFileShareNextStep {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::NoActionRequired => "no_action_required",
            Self::ReviewAttentionShareMetadata => "review_attention_share_metadata",
            Self::ReviewBlockedShareMetadata => "review_blocked_share_metadata",
            Self::ProvideRedactedShareMetadata => "provide_redacted_share_metadata",
            Self::HandoffForFileOrNetworkAction => "handoff_for_file_or_network_action",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LanFileShareReport {
    pub(crate) schema_version: String,
    pub(crate) classification: LanFileShareReportClassification,
    pub(crate) shares: Vec<LanShareEvidence>,
    pub(crate) next_step: LanFileShareNextStep,
}

pub(crate) fn run_lan_file_share_agent_cli(
    command: &LanFileShareCliCommand,
) -> Result<(), LanFileShareAgentError> {
    let execution =
        execute_lan_file_share_agent(Path::new(&command.package_root), &command.input())?;
    if command.json {
        let output = serde_json::to_string_pretty(&execution).map_err(|_| {
            LanFileShareAgentError::new(LanFileShareAgentErrorCode::OutputVerificationFailed)
        })?;
        println!("{output}");
    } else {
        println!("LAN File Share Assistant");
        println!("status: {}", execution.status);
        println!("classification: {}", execution.classification);
        println!("share_count: {}", execution.report.shares.len());
        println!("next_step: {}", execution.report.next_step.as_str());
    }
    Ok(())
}

pub(crate) fn register_lan_file_share_program<'subject>(
    registry: &OfficialRustProgramRegistry,
    subject: PackageReviewSubject<'subject>,
) -> Result<OfficialRustProgram<'subject>, OfficialRustProgramFailure> {
    if subject.package_id() != LAN_FILE_SHARE_PACKAGE_ID
        || subject.task_type() != LAN_FILE_SHARE_TASK_TYPE
    {
        return Err(OfficialRustProgramFailure::new(
            OfficialRustProgramFailureCode::RejectedInput,
        ));
    }
    Ok(registry.register(subject, lan_file_share_official_program))
}

fn lan_file_share_official_program(
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

    let input: LanFileShareInput = serde_json::from_str(input).map_err(|_| rejected_input())?;
    input.validate().map_err(|_| rejected_input())?;
    let report = build_report(input);
    let content = serde_json::to_string(&report).map_err(|_| rejected_input())?;
    context.checkpoint()?;
    Ok(OfficialRustProgramOutput::operator_reviewed(
        report.classification.runtime_classification(),
        content,
    ))
}

fn build_report(input: LanFileShareInput) -> LanFileShareReport {
    let has_unsupported = input
        .shares
        .iter()
        .any(|item| item.claim == ShareEvidenceClaim::UnsupportedClaim);
    let has_missing = input.shares.is_empty()
        || input
            .shares
            .iter()
            .any(|item| item.status == ShareEvidenceStatus::Missing);
    let has_blocked = input
        .shares
        .iter()
        .any(|item| item.status == ShareEvidenceStatus::Blocked);
    let has_attention = input
        .shares
        .iter()
        .any(|item| item.status == ShareEvidenceStatus::Attention);

    let (classification, next_step) = if has_unsupported {
        (
            LanFileShareReportClassification::HandoffRequest,
            LanFileShareNextStep::HandoffForFileOrNetworkAction,
        )
    } else if has_missing {
        (
            LanFileShareReportClassification::BlockedActionReport,
            LanFileShareNextStep::ProvideRedactedShareMetadata,
        )
    } else if has_blocked {
        (
            LanFileShareReportClassification::FileShareReview,
            LanFileShareNextStep::ReviewBlockedShareMetadata,
        )
    } else if has_attention {
        (
            LanFileShareReportClassification::FileShareReview,
            LanFileShareNextStep::ReviewAttentionShareMetadata,
        )
    } else {
        (
            LanFileShareReportClassification::FileShareReview,
            LanFileShareNextStep::NoActionRequired,
        )
    };

    LanFileShareReport {
        schema_version: LAN_FILE_SHARE_OUTPUT_SCHEMA_VERSION.to_string(),
        classification,
        shares: input.shares,
        next_step,
    }
}

const fn rejected_input() -> OfficialRustProgramFailure {
    OfficialRustProgramFailure::new(OfficialRustProgramFailureCode::RejectedInput)
}

#[cfg(test)]
mod tests;
