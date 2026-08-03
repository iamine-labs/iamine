mod execution;
mod package;
mod policy;

use std::{fmt, path::Path};

use iamine_agent_runtime::{
    OfficialRustProgram, OfficialRustProgramFailure, OfficialRustProgramFailureCode,
    OfficialRustProgramOutput, OfficialRustProgramRegistry, OutputClassification,
    PackageReviewSubject, RuntimeExecutionContext,
};

use crate::node_doctor_evidence_provider::{
    collect_node_doctor_evidence, NodeDoctorEvidenceCategory, NodeDoctorEvidenceReport,
    NodeDoctorEvidenceStatus,
};

pub(crate) use execution::execute_node_doctor_agent;

pub(crate) const NODE_DOCTOR_PACKAGE_ID: &str = "iamine.beta.node-doctor";
pub(crate) const NODE_DOCTOR_TASK_TYPE: &str = "diagnostic_report";
pub(crate) const NODE_DOCTOR_SCOPE_ID: &str = "node_readiness_diagnostic_report";
pub(crate) const NODE_DOCTOR_TASK_INPUT: &str = "explain_node_readiness";
pub(crate) const NODE_DOCTOR_OUTPUT_SCHEMA_VERSION: &str = "iamine.agent.node_doctor.output-0.1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeDoctorAgentErrorCode {
    PackageUnavailable,
    PackageInvalid,
    PackageMismatch,
    RuntimeRejected,
    OutputVerificationFailed,
}

impl NodeDoctorAgentErrorCode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::PackageUnavailable => "package_unavailable",
            Self::PackageInvalid => "package_invalid",
            Self::PackageMismatch => "package_mismatch",
            Self::RuntimeRejected => "runtime_rejected",
            Self::OutputVerificationFailed => "output_verification_failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NodeDoctorAgentError {
    code: NodeDoctorAgentErrorCode,
}

impl NodeDoctorAgentError {
    pub(crate) const fn new(code: NodeDoctorAgentErrorCode) -> Self {
        Self { code }
    }

    #[cfg(test)]
    pub(crate) const fn code(self) -> NodeDoctorAgentErrorCode {
        self.code
    }
}

impl fmt::Display for NodeDoctorAgentError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.as_str())
    }
}

impl std::error::Error for NodeDoctorAgentError {}

pub(crate) fn run_node_doctor_agent_cli(
    package_root: &Path,
    json: bool,
) -> Result<(), NodeDoctorAgentError> {
    let result = execute_node_doctor_agent(package_root)?;
    if json {
        let output = serde_json::to_string_pretty(&result).map_err(|_| {
            NodeDoctorAgentError::new(NodeDoctorAgentErrorCode::OutputVerificationFailed)
        })?;
        println!("{output}");
    } else {
        println!("Node Doctor");
        println!("status: {}", result.status);
        println!("classification: {}", result.classification);
        println!("{}", result.content);
    }
    Ok(())
}

pub(crate) fn register_node_doctor_program<'subject>(
    registry: &OfficialRustProgramRegistry,
    subject: PackageReviewSubject<'subject>,
) -> Result<OfficialRustProgram<'subject>, OfficialRustProgramFailure> {
    if subject.package_id() != NODE_DOCTOR_PACKAGE_ID
        || subject.task_type() != NODE_DOCTOR_TASK_TYPE
    {
        return Err(OfficialRustProgramFailure::new(
            OfficialRustProgramFailureCode::RejectedInput,
        ));
    }

    Ok(registry.register(subject, node_doctor_official_program))
}

fn node_doctor_official_program(
    context: &RuntimeExecutionContext<'_>,
    input: &str,
) -> Result<OfficialRustProgramOutput, OfficialRustProgramFailure> {
    context.checkpoint()?;
    if input != NODE_DOCTOR_TASK_INPUT
        || context.network_allowed()
        || context.shell_allowed()
        || context.child_processes_allowed()
        || context.persistence_allowed()
    {
        return Err(OfficialRustProgramFailure::new(
            OfficialRustProgramFailureCode::RejectedInput,
        ));
    }

    let report = collect_node_doctor_evidence();
    let output = render_node_doctor_report(&report);
    context.checkpoint()?;
    Ok(OfficialRustProgramOutput::operator_reviewed(
        output.classification,
        output.content,
    ))
}

struct RenderedNodeDoctorReport {
    classification: OutputClassification,
    content: String,
}

fn render_node_doctor_report(report: &NodeDoctorEvidenceReport) -> RenderedNodeDoctorReport {
    let classification = if report
        .evidence()
        .iter()
        .any(|item| item.status == NodeDoctorEvidenceStatus::Unknown)
    {
        OutputClassification::BlockedActionReport
    } else {
        OutputClassification::DiagnosticReport
    };

    let mut content = format!(
        "schema={NODE_DOCTOR_OUTPUT_SCHEMA_VERSION};class={};",
        classification.as_str()
    );
    for item in report.evidence() {
        content.push_str(category_name(item.category));
        content.push('=');
        content.push_str(status_name(item.status));
        content.push(':');
        content.push_str(item.reason_code);
        content.push(';');
    }
    content.push_str("next_step=");
    content.push_str(next_step(report));

    RenderedNodeDoctorReport {
        classification,
        content,
    }
}

fn next_step(report: &NodeDoctorEvidenceReport) -> &'static str {
    let mut statuses = report.evidence().iter().map(|item| item.status);
    if statuses
        .clone()
        .any(|status| status == NodeDoctorEvidenceStatus::Unknown)
    {
        "request_operator_review"
    } else if statuses
        .clone()
        .any(|status| status == NodeDoctorEvidenceStatus::Blocked)
    {
        "review_blocked_readiness_gates"
    } else if statuses
        .clone()
        .any(|status| status == NodeDoctorEvidenceStatus::Attention)
    {
        "review_attention_readiness_gates"
    } else if statuses.any(|status| status == NodeDoctorEvidenceStatus::NotObserved) {
        "review_unobserved_readiness_gates"
    } else {
        "no_action_required"
    }
}

const fn category_name(category: NodeDoctorEvidenceCategory) -> &'static str {
    match category {
        NodeDoctorEvidenceCategory::NodeStatus => "node_status",
        NodeDoctorEvidenceCategory::HardwareProfile => "hardware_profile",
        NodeDoctorEvidenceCategory::ConfigurationStatus => "configuration_status",
        NodeDoctorEvidenceCategory::ModelReadiness => "model_readiness",
        NodeDoctorEvidenceCategory::PeerNetworkStatus => "peer_network_status",
        NodeDoctorEvidenceCategory::RemoteInferenceReadiness => "remote_inference_readiness",
    }
}

const fn status_name(status: NodeDoctorEvidenceStatus) -> &'static str {
    match status {
        NodeDoctorEvidenceStatus::Ready => "ready",
        NodeDoctorEvidenceStatus::Attention => "attention",
        NodeDoctorEvidenceStatus::Blocked => "blocked",
        NodeDoctorEvidenceStatus::Unknown => "unknown",
        NodeDoctorEvidenceStatus::NotObserved => "not_observed",
    }
}

#[cfg(test)]
mod tests;
