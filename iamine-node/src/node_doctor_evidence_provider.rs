use crate::lan_node_doctor::{
    build_lan_node_doctor_report, DoctorCheck, DoctorStatus, LanNodeDoctorReport,
    BACKEND_AVAILABILITY_CHECK_ID, HARDWARE_PROFILE_CHECK_ID, MODEL_CATALOG_CHECK_ID,
    NODE_CONFIG_CHECK_ID, PEER_NETWORK_CHECK_ID, WORKER_STARTUP_POLICY_CHECK_ID,
};
use serde::Serialize;

pub(crate) const NODE_DOCTOR_EVIDENCE_SCHEMA_VERSION: &str = "1.0.0";
pub(crate) const NODE_DOCTOR_EVIDENCE_FEATURE: &str = "NODE-DOCTOR-EVIDENCE-PROVIDER-001";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NodeDoctorEvidenceCategory {
    NodeStatus,
    HardwareProfile,
    ConfigurationStatus,
    ModelReadiness,
    PeerNetworkStatus,
    RemoteInferenceReadiness,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NodeDoctorEvidenceStatus {
    Ready,
    Attention,
    Blocked,
    Unknown,
    NotObserved,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct NodeDoctorEvidenceItem {
    pub(crate) category: NodeDoctorEvidenceCategory,
    pub(crate) status: NodeDoctorEvidenceStatus,
    pub(crate) reason_code: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct NodeDoctorEvidenceRuntimeEffects {
    pub(crate) workers_started: bool,
    pub(crate) p2p_started: bool,
    pub(crate) pubsub_started: bool,
    pub(crate) model_download_started: bool,
    pub(crate) model_load_started: bool,
    pub(crate) inference_started: bool,
    pub(crate) dynamic_hardware_probe_started: bool,
}

impl NodeDoctorEvidenceRuntimeEffects {
    fn none() -> Self {
        Self {
            workers_started: false,
            p2p_started: false,
            pubsub_started: false,
            model_download_started: false,
            model_load_started: false,
            inference_started: false,
            dynamic_hardware_probe_started: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct NodeDoctorEvidenceReport {
    pub(crate) schema_version: &'static str,
    pub(crate) feature: &'static str,
    pub(crate) source: &'static str,
    pub(crate) read_only: bool,
    pub(crate) redacted: bool,
    pub(crate) runtime_effects: NodeDoctorEvidenceRuntimeEffects,
    evidence: Vec<NodeDoctorEvidenceItem>,
}

impl NodeDoctorEvidenceReport {
    pub(crate) fn evidence(&self) -> &[NodeDoctorEvidenceItem] {
        &self.evidence
    }
}

pub(crate) fn collect_node_doctor_evidence() -> NodeDoctorEvidenceReport {
    let owner_report = build_lan_node_doctor_report(false);
    build_node_doctor_evidence(&owner_report)
}

pub(crate) fn build_node_doctor_evidence(
    owner_report: &LanNodeDoctorReport,
) -> NodeDoctorEvidenceReport {
    build_node_doctor_evidence_from_parts(owner_report.overall_status(), owner_report.checks())
}

pub(crate) fn build_node_doctor_evidence_from_parts(
    overall_status: DoctorStatus,
    owner_checks: &[DoctorCheck],
) -> NodeDoctorEvidenceReport {
    let evidence = vec![
        NodeDoctorEvidenceItem {
            category: NodeDoctorEvidenceCategory::NodeStatus,
            status: status_from_doctor(overall_status),
            reason_code: reason_for_node_status(overall_status),
        },
        evidence_from_checks(
            NodeDoctorEvidenceCategory::HardwareProfile,
            &[HARDWARE_PROFILE_CHECK_ID],
            owner_checks,
            EvidenceReasonSet::new(
                "hardware_profile_ready",
                "hardware_profile_attention",
                "hardware_profile_blocked",
                "hardware_profile_not_observed",
            ),
        ),
        evidence_from_checks(
            NodeDoctorEvidenceCategory::ConfigurationStatus,
            &[NODE_CONFIG_CHECK_ID],
            owner_checks,
            EvidenceReasonSet::new(
                "configuration_ready",
                "configuration_attention",
                "configuration_blocked",
                "configuration_not_observed",
            ),
        ),
        evidence_from_checks(
            NodeDoctorEvidenceCategory::ModelReadiness,
            &[
                MODEL_CATALOG_CHECK_ID,
                BACKEND_AVAILABILITY_CHECK_ID,
                WORKER_STARTUP_POLICY_CHECK_ID,
            ],
            owner_checks,
            EvidenceReasonSet::new(
                "model_readiness_ready",
                "model_readiness_attention",
                "model_readiness_blocked",
                "model_readiness_not_observed",
            ),
        ),
        evidence_from_checks(
            NodeDoctorEvidenceCategory::PeerNetworkStatus,
            &[PEER_NETWORK_CHECK_ID],
            owner_checks,
            EvidenceReasonSet::new(
                "peer_network_ready",
                "peer_network_attention",
                "peer_network_blocked",
                "peer_network_not_observed",
            ),
        ),
        evidence_from_checks(
            NodeDoctorEvidenceCategory::RemoteInferenceReadiness,
            &[
                MODEL_CATALOG_CHECK_ID,
                BACKEND_AVAILABILITY_CHECK_ID,
                WORKER_STARTUP_POLICY_CHECK_ID,
                PEER_NETWORK_CHECK_ID,
            ],
            owner_checks,
            EvidenceReasonSet::new(
                "remote_inference_ready",
                "remote_inference_attention",
                "remote_inference_blocked",
                "remote_inference_not_observed",
            ),
        ),
    ];

    NodeDoctorEvidenceReport {
        schema_version: NODE_DOCTOR_EVIDENCE_SCHEMA_VERSION,
        feature: NODE_DOCTOR_EVIDENCE_FEATURE,
        source: "owner_module_summary",
        read_only: true,
        redacted: true,
        runtime_effects: NodeDoctorEvidenceRuntimeEffects::none(),
        evidence,
    }
}

#[derive(Debug, Clone, Copy)]
struct EvidenceReasonSet {
    ready: &'static str,
    attention: &'static str,
    blocked: &'static str,
    not_observed: &'static str,
}

impl EvidenceReasonSet {
    const fn new(
        ready: &'static str,
        attention: &'static str,
        blocked: &'static str,
        not_observed: &'static str,
    ) -> Self {
        Self {
            ready,
            attention,
            blocked,
            not_observed,
        }
    }

    fn for_status(self, status: NodeDoctorEvidenceStatus) -> &'static str {
        match status {
            NodeDoctorEvidenceStatus::Ready => self.ready,
            NodeDoctorEvidenceStatus::Attention => self.attention,
            NodeDoctorEvidenceStatus::Blocked => self.blocked,
            NodeDoctorEvidenceStatus::Unknown => "owner_evidence_unavailable",
            NodeDoctorEvidenceStatus::NotObserved => self.not_observed,
        }
    }
}

fn evidence_from_checks(
    category: NodeDoctorEvidenceCategory,
    required_check_ids: &[&str],
    owner_checks: &[DoctorCheck],
    reasons: EvidenceReasonSet,
) -> NodeDoctorEvidenceItem {
    let matched = required_check_ids
        .iter()
        .map(|required_id| {
            owner_checks
                .iter()
                .find(|check| check.id == *required_id)
                .map(|check| check.status)
        })
        .collect::<Option<Vec<_>>>();

    let status = matched
        .as_deref()
        .map(reduce_statuses)
        .unwrap_or(NodeDoctorEvidenceStatus::Unknown);

    NodeDoctorEvidenceItem {
        category,
        status,
        reason_code: reasons.for_status(status),
    }
}

fn reduce_statuses(statuses: &[DoctorStatus]) -> NodeDoctorEvidenceStatus {
    if statuses.contains(&DoctorStatus::Fail) {
        NodeDoctorEvidenceStatus::Blocked
    } else if statuses.contains(&DoctorStatus::Warn) {
        NodeDoctorEvidenceStatus::Attention
    } else if statuses.contains(&DoctorStatus::NotRun) {
        NodeDoctorEvidenceStatus::NotObserved
    } else {
        NodeDoctorEvidenceStatus::Ready
    }
}

fn status_from_doctor(status: DoctorStatus) -> NodeDoctorEvidenceStatus {
    match status {
        DoctorStatus::Pass => NodeDoctorEvidenceStatus::Ready,
        DoctorStatus::Warn => NodeDoctorEvidenceStatus::Attention,
        DoctorStatus::Fail => NodeDoctorEvidenceStatus::Blocked,
        DoctorStatus::NotRun => NodeDoctorEvidenceStatus::NotObserved,
    }
}

fn reason_for_node_status(status: DoctorStatus) -> &'static str {
    match status {
        DoctorStatus::Pass => "node_status_summary_ready",
        DoctorStatus::Warn => "node_status_summary_attention",
        DoctorStatus::Fail => "node_status_summary_blocked",
        DoctorStatus::NotRun => "node_status_summary_not_observed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use std::collections::{BTreeMap, BTreeSet};

    fn owner_check(id: &'static str, status: DoctorStatus) -> DoctorCheck {
        DoctorCheck {
            id,
            status,
            message: "owner-only message".to_string(),
            details: BTreeMap::new(),
        }
    }

    fn complete_owner_checks(status: DoctorStatus) -> Vec<DoctorCheck> {
        vec![
            owner_check(HARDWARE_PROFILE_CHECK_ID, status),
            owner_check(NODE_CONFIG_CHECK_ID, status),
            owner_check(MODEL_CATALOG_CHECK_ID, status),
            owner_check(BACKEND_AVAILABILITY_CHECK_ID, status),
            owner_check(WORKER_STARTUP_POLICY_CHECK_ID, status),
            owner_check(PEER_NETWORK_CHECK_ID, status),
        ]
    }

    #[test]
    fn provider_exposes_each_bounded_category_once() {
        let report = build_node_doctor_evidence_from_parts(
            DoctorStatus::Pass,
            &complete_owner_checks(DoctorStatus::Pass),
        );
        let categories = report
            .evidence()
            .iter()
            .map(|item| item.category)
            .collect::<BTreeSet<_>>();

        assert_eq!(report.evidence().len(), 6);
        assert_eq!(categories.len(), 6);
        assert!(report.read_only);
        assert!(report.redacted);
    }

    #[test]
    fn missing_owner_evidence_fails_closed_as_unknown() {
        let mut checks = complete_owner_checks(DoctorStatus::Pass);
        checks.retain(|check| check.id != NODE_CONFIG_CHECK_ID);

        let report = build_node_doctor_evidence_from_parts(DoctorStatus::Pass, &checks);
        let configuration = report
            .evidence()
            .iter()
            .find(|item| item.category == NodeDoctorEvidenceCategory::ConfigurationStatus)
            .expect("configuration evidence must remain present");

        assert_eq!(configuration.status, NodeDoctorEvidenceStatus::Unknown);
        assert_eq!(configuration.reason_code, "owner_evidence_unavailable");
    }

    #[test]
    fn blocked_and_attention_states_preserve_precedence() {
        let mut checks = complete_owner_checks(DoctorStatus::Pass);
        checks
            .iter_mut()
            .find(|check| check.id == MODEL_CATALOG_CHECK_ID)
            .expect("model catalog check")
            .status = DoctorStatus::Warn;
        checks
            .iter_mut()
            .find(|check| check.id == BACKEND_AVAILABILITY_CHECK_ID)
            .expect("backend check")
            .status = DoctorStatus::Fail;

        let report = build_node_doctor_evidence_from_parts(DoctorStatus::Warn, &checks);
        let model = report
            .evidence()
            .iter()
            .find(|item| item.category == NodeDoctorEvidenceCategory::ModelReadiness)
            .expect("model evidence");

        assert_eq!(model.status, NodeDoctorEvidenceStatus::Blocked);
        assert_eq!(model.reason_code, "model_readiness_blocked");
    }

    #[test]
    fn unobserved_network_blocks_remote_readiness_claims() {
        let mut checks = complete_owner_checks(DoctorStatus::Pass);
        checks
            .iter_mut()
            .find(|check| check.id == PEER_NETWORK_CHECK_ID)
            .expect("network check")
            .status = DoctorStatus::NotRun;

        let report = build_node_doctor_evidence_from_parts(DoctorStatus::Pass, &checks);
        for category in [
            NodeDoctorEvidenceCategory::PeerNetworkStatus,
            NodeDoctorEvidenceCategory::RemoteInferenceReadiness,
        ] {
            let item = report
                .evidence()
                .iter()
                .find(|item| item.category == category)
                .expect("bounded evidence category");
            assert_eq!(item.status, NodeDoctorEvidenceStatus::NotObserved);
        }
    }

    #[test]
    fn serialized_evidence_omits_private_owner_messages_and_details() {
        let private_value = "/Users/alice/private/node.log";
        let mut checks = complete_owner_checks(DoctorStatus::Pass);
        checks[0].message = format!("hardware error at {private_value}");
        checks[0].details.insert(
            "raw_path".to_string(),
            Value::String(private_value.to_string()),
        );

        let report = build_node_doctor_evidence_from_parts(DoctorStatus::Pass, &checks);
        let serialized = serde_json::to_string(&report).expect("serialize evidence report");

        assert!(!serialized.contains(private_value));
        assert!(!serialized.contains("raw_path"));
        assert!(!serialized.contains("owner-only message"));
    }

    #[test]
    fn collector_declares_no_runtime_or_mutation_side_effects() {
        let report = collect_node_doctor_evidence();
        let effects = serde_json::to_value(&report.runtime_effects).expect("serialize effects");

        assert_eq!(report.schema_version, NODE_DOCTOR_EVIDENCE_SCHEMA_VERSION);
        assert_eq!(report.feature, NODE_DOCTOR_EVIDENCE_FEATURE);
        assert_eq!(report.source, "owner_module_summary");
        assert_eq!(report.evidence().len(), 6);
        assert_eq!(
            effects,
            json!({
                "workers_started": false,
                "p2p_started": false,
                "pubsub_started": false,
                "model_download_started": false,
                "model_load_started": false,
                "inference_started": false,
                "dynamic_hardware_probe_started": false
            })
        );
    }
}
