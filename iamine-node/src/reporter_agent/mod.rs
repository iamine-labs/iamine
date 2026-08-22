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

pub(crate) use execution::execute_reporter_agent;
#[cfg(test)]
pub(crate) use input::ReporterEvidenceSource;
pub(crate) use input::{
    ReporterClaim, ReporterCliCommand, ReporterEvidence, ReporterEvidenceStatus, ReporterInput,
};

pub(crate) const REPORTER_PACKAGE_ID: &str = "iamine.beta.support-reporter";
pub(crate) const REPORTER_TASK_TYPE: &str = "support_report";
pub(crate) const REPORTER_SCOPE_ID: &str = "privacy_safe_support_report";
pub(crate) const REPORTER_INPUT_SCHEMA_VERSION: &str = "iamine.agent.reporter.input-0.1";
pub(crate) const REPORTER_OUTPUT_SCHEMA_VERSION: &str = "iamine.agent.reporter.output-0.1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReporterAgentErrorCode {
    PackageUnavailable,
    PackageInvalid,
    PackageMismatch,
    InputInvalid,
    RuntimeRejected,
    OutputVerificationFailed,
}

impl ReporterAgentErrorCode {
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
pub(crate) struct ReporterAgentError {
    code: ReporterAgentErrorCode,
}

impl ReporterAgentError {
    pub(crate) const fn new(code: ReporterAgentErrorCode) -> Self {
        Self { code }
    }

    #[cfg(test)]
    pub(crate) const fn code(self) -> ReporterAgentErrorCode {
        self.code
    }
}

impl fmt::Display for ReporterAgentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.as_str())
    }
}

impl std::error::Error for ReporterAgentError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReporterReportClassification {
    SupportReport,
    BlockedActionReport,
    HandoffRequest,
}

impl ReporterReportClassification {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::SupportReport => "support_report",
            Self::BlockedActionReport => "blocked_action_report",
            Self::HandoffRequest => "handoff_request",
        }
    }

    const fn runtime_classification(self) -> OutputClassification {
        match self {
            Self::SupportReport => OutputClassification::SupportReport,
            Self::BlockedActionReport => OutputClassification::BlockedActionReport,
            Self::HandoffRequest => OutputClassification::HandoffRequest,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReporterNextStep {
    NoActionRequired,
    ReviewAttentionEvidence,
    ReviewBlockedEvidence,
    ProvideRedactedEvidence,
    HumanReviewRequired,
}

impl ReporterNextStep {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::NoActionRequired => "no_action_required",
            Self::ReviewAttentionEvidence => "review_attention_evidence",
            Self::ReviewBlockedEvidence => "review_blocked_evidence",
            Self::ProvideRedactedEvidence => "provide_redacted_evidence",
            Self::HumanReviewRequired => "human_review_required",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReporterReport {
    pub(crate) schema_version: String,
    pub(crate) classification: ReporterReportClassification,
    pub(crate) evidence: Vec<ReporterEvidence>,
    pub(crate) next_step: ReporterNextStep,
}

pub(crate) fn run_reporter_agent_cli(
    command: &ReporterCliCommand,
) -> Result<(), ReporterAgentError> {
    let execution = execute_reporter_agent(Path::new(&command.package_root), &command.input())?;
    if command.json {
        let output = serde_json::to_string_pretty(&execution).map_err(|_| {
            ReporterAgentError::new(ReporterAgentErrorCode::OutputVerificationFailed)
        })?;
        println!("{output}");
    } else {
        println!("Reporter");
        println!("status: {}", execution.status);
        println!("classification: {}", execution.classification);
        println!("evidence_count: {}", execution.report.evidence.len());
        println!("next_step: {}", execution.report.next_step.as_str());
    }
    Ok(())
}

pub(crate) fn register_reporter_program<'subject>(
    registry: &OfficialRustProgramRegistry,
    subject: PackageReviewSubject<'subject>,
) -> Result<OfficialRustProgram<'subject>, OfficialRustProgramFailure> {
    if subject.package_id() != REPORTER_PACKAGE_ID || subject.task_type() != REPORTER_TASK_TYPE {
        return Err(OfficialRustProgramFailure::new(
            OfficialRustProgramFailureCode::RejectedInput,
        ));
    }
    Ok(registry.register(subject, reporter_official_program))
}

fn reporter_official_program(
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

    let input: ReporterInput = serde_json::from_str(input).map_err(|_| rejected_input())?;
    input.validate().map_err(|_| rejected_input())?;
    let report = build_report(input);
    let content = serde_json::to_string(&report).map_err(|_| rejected_input())?;
    context.checkpoint()?;
    Ok(OfficialRustProgramOutput::operator_reviewed(
        report.classification.runtime_classification(),
        content,
    ))
}

fn build_report(input: ReporterInput) -> ReporterReport {
    let has_unsupported = input
        .evidence
        .iter()
        .any(|item| item.claim == ReporterClaim::UnsupportedClaim);
    let has_missing = input.evidence.is_empty()
        || input
            .evidence
            .iter()
            .any(|item| item.status == ReporterEvidenceStatus::Missing);
    let has_blocked = input
        .evidence
        .iter()
        .any(|item| item.status == ReporterEvidenceStatus::Blocked);
    let has_attention = input
        .evidence
        .iter()
        .any(|item| item.status == ReporterEvidenceStatus::Attention);

    let (classification, next_step) = if has_unsupported {
        (
            ReporterReportClassification::HandoffRequest,
            ReporterNextStep::HumanReviewRequired,
        )
    } else if has_missing {
        (
            ReporterReportClassification::BlockedActionReport,
            ReporterNextStep::ProvideRedactedEvidence,
        )
    } else if has_blocked {
        (
            ReporterReportClassification::SupportReport,
            ReporterNextStep::ReviewBlockedEvidence,
        )
    } else if has_attention {
        (
            ReporterReportClassification::SupportReport,
            ReporterNextStep::ReviewAttentionEvidence,
        )
    } else {
        (
            ReporterReportClassification::SupportReport,
            ReporterNextStep::NoActionRequired,
        )
    };

    ReporterReport {
        schema_version: REPORTER_OUTPUT_SCHEMA_VERSION.to_string(),
        classification,
        evidence: input.evidence,
        next_step,
    }
}

const fn rejected_input() -> OfficialRustProgramFailure {
    OfficialRustProgramFailure::new(OfficialRustProgramFailureCode::RejectedInput)
}

#[cfg(test)]
mod tests;
