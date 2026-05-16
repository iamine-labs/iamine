use crate::cluster_health::{classify_cluster_health, ClusterHealth, ClusterHealthThresholds};
use crate::cluster_readiness::{
    derive_cluster_readiness, ClusterReadiness, ClusterReadinessInput, ClusterReadinessReason,
};
use iamine_network::NodeCapabilityHeartbeat;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub(crate) const DEFAULT_CLUSTER_ID: &str = "default-lan";
pub(crate) const CLUSTER_NODE_STATUS_TYPE: &str = "ClusterNodeStatus";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClusterRole {
    Controller,
    Worker,
    Relay,
    Unknown,
}

impl ClusterRole {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Controller => "controller",
            Self::Worker => "worker",
            Self::Relay => "relay",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClusterExecutionMode {
    Mock,
    Real,
    RelayOnly,
    Degraded,
    Unknown,
}

impl ClusterExecutionMode {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Mock => "mock",
            Self::Real => "real",
            Self::RelayOnly => "relay_only",
            Self::Degraded => "degraded",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClusterBackend {
    Mock,
    Cpu,
    Metal,
    Cuda,
    Real,
    Unknown,
}

impl ClusterBackend {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Mock => "mock",
            Self::Cpu => "cpu",
            Self::Metal => "metal",
            Self::Cuda => "cuda",
            Self::Real => "real",
            Self::Unknown => "unknown",
        }
    }

    pub(crate) fn from_backend_and_accelerator(backend: &str, accelerator: Option<&str>) -> Self {
        if backend.eq_ignore_ascii_case("mock") {
            return Self::Mock;
        }
        match accelerator.unwrap_or("").to_ascii_lowercase().as_str() {
            "metal" => Self::Metal,
            "cuda" => Self::Cuda,
            "cpu" => Self::Cpu,
            _ if backend.eq_ignore_ascii_case("real") => Self::Real,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ClusterMetricsStatus {
    Available,
    Unavailable,
    Fallback,
    Disabled,
    Unknown,
}

impl ClusterMetricsStatus {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Unavailable => "unavailable",
            Self::Fallback => "fallback",
            Self::Disabled => "disabled",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClusterCapabilitySummary {
    pub(crate) models_in_storage: Vec<String>,
    pub(crate) models_in_registry: Vec<String>,
    pub(crate) executable_models: Vec<String>,
    pub(crate) supported_task_types: Vec<String>,
    pub(crate) real_inference_available: Option<bool>,
}

impl Default for ClusterCapabilitySummary {
    fn default() -> Self {
        Self {
            models_in_storage: Vec::new(),
            models_in_registry: Vec::new(),
            executable_models: Vec::new(),
            supported_task_types: Vec::new(),
            real_inference_available: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClusterNode {
    pub(crate) peer_id: String,
    pub(crate) peer_id_short: String,
    pub(crate) hostname: String,
    pub(crate) observed_addresses: Vec<String>,
    pub(crate) role: ClusterRole,
    pub(crate) execution_mode: ClusterExecutionMode,
    pub(crate) backend: ClusterBackend,
    pub(crate) last_seen_ms: Option<u64>,
    pub(crate) latency_ms: Option<u32>,
    pub(crate) capabilities: ClusterCapabilitySummary,
    pub(crate) metrics_status: ClusterMetricsStatus,
    pub(crate) ready_for_tasks: bool,
    pub(crate) readiness_reason: ClusterReadinessReason,
    pub(crate) degraded_reason: Option<String>,
}

impl ClusterNode {
    pub(crate) fn new(peer_id: impl Into<String>) -> Self {
        let peer_id = peer_id.into();
        Self {
            peer_id_short: peer_id_short(&peer_id),
            peer_id,
            hostname: "unknown".to_string(),
            observed_addresses: Vec::new(),
            role: ClusterRole::Unknown,
            execution_mode: ClusterExecutionMode::Unknown,
            backend: ClusterBackend::Unknown,
            last_seen_ms: None,
            latency_ms: None,
            capabilities: ClusterCapabilitySummary::default(),
            metrics_status: ClusterMetricsStatus::Unknown,
            ready_for_tasks: false,
            readiness_reason: ClusterReadinessReason::Unknown,
            degraded_reason: None,
        }
    }

    pub(crate) fn health_at(
        &self,
        now_ms: u64,
        thresholds: ClusterHealthThresholds,
    ) -> ClusterHealth {
        classify_cluster_health(
            now_ms,
            self.last_seen_ms,
            matches!(self.execution_mode, ClusterExecutionMode::Degraded)
                || self.degraded_reason.is_some(),
            thresholds,
        )
    }

    pub(crate) fn readiness_at(
        &self,
        now_ms: u64,
        thresholds: ClusterHealthThresholds,
    ) -> ClusterReadiness {
        derive_cluster_readiness(&ClusterReadinessInput {
            role: self.role,
            execution_mode: self.execution_mode,
            backend: self.backend,
            health: self.health_at(now_ms, thresholds),
            metrics_status: self.metrics_status,
            real_inference_available: self.capabilities.real_inference_available,
            supported_task_types: &self.capabilities.supported_task_types,
            executable_models: &self.capabilities.executable_models,
        })
    }

    fn refresh_readiness_at(&mut self, now_ms: u64) {
        let readiness = self.readiness_at(now_ms, ClusterHealthThresholds::default());
        self.ready_for_tasks = readiness.ready_for_tasks;
        self.readiness_reason = readiness.readiness_reason;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClusterNodeStatusMessage {
    #[serde(rename = "type")]
    pub(crate) message_type: String,
    pub(crate) cluster_id: String,
    pub(crate) peer_id: String,
    pub(crate) hostname: String,
    pub(crate) role: ClusterRole,
    pub(crate) execution_mode: ClusterExecutionMode,
    pub(crate) backend: ClusterBackend,
    pub(crate) observed_addresses: Vec<String>,
    pub(crate) latency_ms: Option<u32>,
    pub(crate) real_inference_available: Option<bool>,
    pub(crate) supported_task_types: Vec<String>,
    pub(crate) models_in_storage: Vec<String>,
    pub(crate) models_in_registry: Vec<String>,
    pub(crate) executable_models: Vec<String>,
    pub(crate) metrics_status: ClusterMetricsStatus,
    #[serde(default)]
    pub(crate) ready_for_tasks: bool,
    #[serde(default)]
    pub(crate) readiness_reason: ClusterReadinessReason,
    pub(crate) degraded_reason: Option<String>,
}

impl ClusterNodeStatusMessage {
    pub(crate) fn worker(
        cluster_id: impl Into<String>,
        peer_id: impl Into<String>,
        hostname: impl Into<String>,
        execution_mode: ClusterExecutionMode,
        backend: ClusterBackend,
        real_inference_available: bool,
        supported_task_types: Vec<String>,
        models_in_storage: Vec<String>,
        models_in_registry: Vec<String>,
        executable_models: Vec<String>,
        metrics_status: ClusterMetricsStatus,
        latency_ms: Option<u32>,
    ) -> Self {
        let readiness = derive_cluster_readiness(&ClusterReadinessInput {
            role: ClusterRole::Worker,
            execution_mode,
            backend,
            health: ClusterHealth::Healthy,
            metrics_status,
            real_inference_available: Some(real_inference_available),
            supported_task_types: &supported_task_types,
            executable_models: &executable_models,
        });

        Self {
            message_type: CLUSTER_NODE_STATUS_TYPE.to_string(),
            cluster_id: cluster_id.into(),
            peer_id: peer_id.into(),
            hostname: hostname.into(),
            role: ClusterRole::Worker,
            execution_mode,
            backend,
            observed_addresses: Vec::new(),
            latency_ms,
            real_inference_available: Some(real_inference_available),
            supported_task_types,
            models_in_storage,
            models_in_registry,
            executable_models,
            metrics_status,
            ready_for_tasks: readiness.ready_for_tasks,
            readiness_reason: readiness.readiness_reason,
            degraded_reason: None,
        }
    }
}

pub(crate) struct ClusterRegistry {
    cluster_id: String,
    nodes: BTreeMap<String, ClusterNode>,
}

impl ClusterRegistry {
    pub(crate) fn new(cluster_id: impl Into<String>) -> Self {
        Self {
            cluster_id: cluster_id.into(),
            nodes: BTreeMap::new(),
        }
    }

    pub(crate) fn with_default_cluster_id() -> Self {
        Self::new(cluster_id_from_env())
    }

    pub(crate) fn cluster_id(&self) -> &str {
        &self.cluster_id
    }

    pub(crate) fn list_nodes(&self) -> Vec<ClusterNode> {
        self.nodes.values().cloned().collect()
    }

    pub(crate) fn node_by_peer_id(&self, peer_id: &str) -> Option<&ClusterNode> {
        self.nodes.get(peer_id)
    }

    pub(crate) fn add_or_update_discovered_node(
        &mut self,
        peer_id: &str,
        observed_address: Option<String>,
        now_ms: u64,
    ) -> bool {
        let is_new = !self.nodes.contains_key(peer_id);
        let node = self
            .nodes
            .entry(peer_id.to_string())
            .or_insert_with(|| ClusterNode::new(peer_id));
        node.last_seen_ms = Some(now_ms);
        if let Some(address) = observed_address {
            push_unique_sorted(&mut node.observed_addresses, address);
        }
        node.refresh_readiness_at(now_ms);
        is_new
    }

    pub(crate) fn update_latency(&mut self, peer_id: &str, latency_ms: u32, now_ms: u64) {
        let node = self
            .nodes
            .entry(peer_id.to_string())
            .or_insert_with(|| ClusterNode::new(peer_id));
        node.latency_ms = Some(latency_ms);
        node.last_seen_ms = Some(now_ms);
        node.refresh_readiness_at(now_ms);
    }

    pub(crate) fn update_from_capability_heartbeat(
        &mut self,
        heartbeat: &NodeCapabilityHeartbeat,
        now_ms: u64,
    ) {
        let node = self
            .nodes
            .entry(heartbeat.peer_id.clone())
            .or_insert_with(|| ClusterNode::new(&heartbeat.peer_id));
        node.role = ClusterRole::Worker;
        node.last_seen_ms = Some(now_ms);
        node.latency_ms = Some(heartbeat.latency_ms);
        node.capabilities.executable_models = sorted_unique(heartbeat.models.clone());
        node.capabilities.real_inference_available = Some(!heartbeat.models.is_empty());
        if node.backend == ClusterBackend::Unknown {
            node.backend =
                ClusterBackend::from_backend_and_accelerator("real", Some(&heartbeat.accelerator));
        }
        if node.execution_mode == ClusterExecutionMode::Unknown && !heartbeat.models.is_empty() {
            node.execution_mode = ClusterExecutionMode::Real;
        }
        node.refresh_readiness_at(now_ms);
    }

    pub(crate) fn update_from_worker_heartbeat_value(&mut self, value: &Value, now_ms: u64) {
        let Some(peer_id) = value.get("peer_id").and_then(Value::as_str) else {
            return;
        };
        let node = self
            .nodes
            .entry(peer_id.to_string())
            .or_insert_with(|| ClusterNode::new(peer_id));
        node.role = ClusterRole::Worker;
        node.last_seen_ms = Some(now_ms);
        if let Some(latency) = value.get("avg_latency_ms").and_then(Value::as_f64) {
            node.latency_ms = Some(latency.max(0.0).round() as u32);
        }
        if let Some(tasks) = value
            .get("capabilities")
            .and_then(|caps| caps.get("supported_tasks"))
            .and_then(Value::as_array)
        {
            node.capabilities.supported_task_types = sorted_unique(
                tasks
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect(),
            );
        }
        node.refresh_readiness_at(now_ms);
    }

    pub(crate) fn update_from_cluster_status_message(
        &mut self,
        message: ClusterNodeStatusMessage,
        now_ms: u64,
    ) {
        let node = self
            .nodes
            .entry(message.peer_id.clone())
            .or_insert_with(|| ClusterNode::new(&message.peer_id));
        node.hostname = message.hostname;
        node.role = message.role;
        node.execution_mode = message.execution_mode;
        node.backend = message.backend;
        node.last_seen_ms = Some(now_ms);
        node.latency_ms = message.latency_ms;
        node.metrics_status = message.metrics_status;
        node.ready_for_tasks = message.ready_for_tasks;
        node.readiness_reason = message.readiness_reason;
        node.degraded_reason = message.degraded_reason;
        for address in message.observed_addresses {
            push_unique_sorted(&mut node.observed_addresses, address);
        }
        node.capabilities.real_inference_available = message.real_inference_available;
        node.capabilities.supported_task_types = sorted_unique(message.supported_task_types);
        node.capabilities.models_in_storage = sorted_unique(message.models_in_storage);
        node.capabilities.models_in_registry = sorted_unique(message.models_in_registry);
        node.capabilities.executable_models = sorted_unique(message.executable_models);
    }

    #[allow(dead_code)]
    pub(crate) fn nodes_supporting_task(&self, task_type: &str) -> Vec<ClusterNode> {
        self.nodes
            .values()
            .filter(|node| {
                node.capabilities
                    .supported_task_types
                    .iter()
                    .any(|task| task == task_type)
            })
            .cloned()
            .collect()
    }

    #[allow(dead_code)]
    pub(crate) fn nodes_with_model(&self, model_id: &str) -> Vec<ClusterNode> {
        self.nodes
            .values()
            .filter(|node| {
                node.capabilities
                    .executable_models
                    .iter()
                    .any(|model| model == model_id)
            })
            .cloned()
            .collect()
    }

    pub(crate) fn healthy_nodes(&self, now_ms: u64) -> Vec<ClusterNode> {
        self.nodes_by_health(now_ms, ClusterHealth::Healthy)
    }

    pub(crate) fn stale_nodes(&self, now_ms: u64) -> Vec<ClusterNode> {
        self.nodes_by_health(now_ms, ClusterHealth::Stale)
    }

    pub(crate) fn degraded_nodes(&self, now_ms: u64) -> Vec<ClusterNode> {
        self.nodes_by_health(now_ms, ClusterHealth::Degraded)
    }

    pub(crate) fn offline_nodes(&self, now_ms: u64) -> Vec<ClusterNode> {
        self.nodes_by_health(now_ms, ClusterHealth::Offline)
    }

    fn nodes_by_health(&self, now_ms: u64, health: ClusterHealth) -> Vec<ClusterNode> {
        let thresholds = ClusterHealthThresholds::default();
        self.nodes
            .values()
            .filter(|node| node.health_at(now_ms, thresholds) == health)
            .cloned()
            .collect()
    }
}

pub(crate) fn cluster_id_from_env() -> String {
    std::env::var("IAMINE_CLUSTER_ID")
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| DEFAULT_CLUSTER_ID.to_string())
}

pub(crate) fn local_hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "local".to_string())
}

pub(crate) fn peer_id_short(peer_id: &str) -> String {
    peer_id.chars().take(12).collect()
}

pub(crate) fn sorted_unique(mut values: Vec<String>) -> Vec<String> {
    values.retain(|value| !value.trim().is_empty());
    values.sort();
    values.dedup();
    values
}

fn push_unique_sorted(values: &mut Vec<String>, value: String) {
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
        values.sort();
    }
}

pub(crate) fn unix_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn heartbeat(peer_id: &str) -> NodeCapabilityHeartbeat {
        NodeCapabilityHeartbeat {
            peer_id: peer_id.to_string(),
            cpu_score: 100,
            ram_gb: 8,
            gpu_available: false,
            storage_available_gb: 50,
            accelerator: "CPU".to_string(),
            models: vec!["tinyllama-1b".to_string()],
            worker_slots: 4,
            active_tasks: 0,
            latency_ms: 12,
        }
    }

    #[test]
    fn cluster_id_defaults_to_default_lan() {
        let previous = std::env::var("IAMINE_CLUSTER_ID").ok();
        std::env::remove_var("IAMINE_CLUSTER_ID");
        assert_eq!(cluster_id_from_env(), DEFAULT_CLUSTER_ID);
        if let Some(previous) = previous {
            std::env::set_var("IAMINE_CLUSTER_ID", previous);
        }
    }

    #[test]
    fn cluster_id_from_env_reads_env() {
        let previous = std::env::var("IAMINE_CLUSTER_ID").ok();
        std::env::set_var("IAMINE_CLUSTER_ID", "lab-lan");
        assert_eq!(cluster_id_from_env(), "lab-lan");
        if let Some(previous) = previous {
            std::env::set_var("IAMINE_CLUSTER_ID", previous);
        } else {
            std::env::remove_var("IAMINE_CLUSTER_ID");
        }
    }

    #[test]
    fn cluster_registry_adds_discovered_node() {
        let mut registry = ClusterRegistry::new("test-lan");
        let inserted = registry.add_or_update_discovered_node(
            "peer-a",
            Some("/ip4/127.0.0.1/tcp/1".to_string()),
            100,
        );

        assert!(inserted);
        assert_eq!(registry.list_nodes().len(), 1);
        assert_eq!(
            registry
                .node_by_peer_id("peer-a")
                .unwrap()
                .observed_addresses,
            vec!["/ip4/127.0.0.1/tcp/1".to_string()]
        );
    }

    #[test]
    fn cluster_registry_deduplicates_by_peer_id() {
        let mut registry = ClusterRegistry::new("test-lan");
        registry.add_or_update_discovered_node("peer-a", Some("addr-1".to_string()), 100);
        registry.add_or_update_discovered_node("peer-a", Some("addr-1".to_string()), 200);

        let node = registry.node_by_peer_id("peer-a").unwrap();
        assert_eq!(registry.list_nodes().len(), 1);
        assert_eq!(node.observed_addresses, vec!["addr-1".to_string()]);
        assert_eq!(node.last_seen_ms, Some(200));
    }

    #[test]
    fn cluster_registry_updates_last_seen() {
        let mut registry = ClusterRegistry::new("test-lan");
        registry.update_from_capability_heartbeat(&heartbeat("peer-a"), 500);

        assert_eq!(
            registry.node_by_peer_id("peer-a").unwrap().last_seen_ms,
            Some(500)
        );
    }

    #[test]
    fn cluster_registry_updates_capabilities() {
        let mut registry = ClusterRegistry::new("test-lan");
        registry.update_from_capability_heartbeat(&heartbeat("peer-a"), 500);

        let node = registry.node_by_peer_id("peer-a").unwrap();
        assert_eq!(node.role, ClusterRole::Worker);
        assert_eq!(node.backend, ClusterBackend::Cpu);
        assert_eq!(
            node.capabilities.executable_models,
            vec!["tinyllama-1b".to_string()]
        );
    }

    #[test]
    fn cluster_node_role_detection_worker_mock() {
        let message = ClusterNodeStatusMessage::worker(
            "test-lan",
            "peer-a",
            "wrk-01",
            ClusterExecutionMode::Mock,
            ClusterBackend::Mock,
            false,
            vec![
                "reverse_string".to_string(),
                "test".to_string(),
                "echo".to_string(),
            ],
            vec!["tinyllama-1b".to_string()],
            vec!["tinyllama-1b".to_string()],
            Vec::new(),
            ClusterMetricsStatus::Fallback,
            Some(10),
        );
        let mut registry = ClusterRegistry::new("test-lan");
        registry.update_from_cluster_status_message(message, 100);
        let node = registry.node_by_peer_id("peer-a").unwrap();

        assert_eq!(node.role, ClusterRole::Worker);
        assert_eq!(node.execution_mode, ClusterExecutionMode::Mock);
        assert_eq!(node.backend, ClusterBackend::Mock);
        assert_eq!(node.capabilities.real_inference_available, Some(false));
    }

    #[test]
    fn cluster_node_role_detection_relay() {
        let mut node = ClusterNode::new("peer-r");
        node.role = ClusterRole::Relay;
        node.execution_mode = ClusterExecutionMode::RelayOnly;
        assert_eq!(node.role.as_str(), "relay");
        assert_eq!(node.execution_mode.as_str(), "relay_only");
    }

    #[test]
    fn cluster_node_real_backend_cpu() {
        let mut registry = ClusterRegistry::new("test-lan");
        registry.update_from_capability_heartbeat(&heartbeat("peer-real"), 100);
        let node = registry.node_by_peer_id("peer-real").unwrap();

        assert_eq!(node.execution_mode, ClusterExecutionMode::Real);
        assert_eq!(node.backend, ClusterBackend::Cpu);
        assert_eq!(node.capabilities.real_inference_available, Some(true));
    }

    #[test]
    fn cluster_mock_worker_does_not_advertise_real_llm_executable() {
        let message = ClusterNodeStatusMessage::worker(
            "test-lan",
            "peer-a",
            "wrk-01",
            ClusterExecutionMode::Mock,
            ClusterBackend::Mock,
            false,
            vec![
                "reverse_string".to_string(),
                "test".to_string(),
                "echo".to_string(),
            ],
            vec!["tinyllama-1b".to_string()],
            vec!["tinyllama-1b".to_string()],
            Vec::new(),
            ClusterMetricsStatus::Fallback,
            None,
        );
        let mut registry = ClusterRegistry::new("test-lan");
        registry.update_from_cluster_status_message(message, 100);

        assert!(registry
            .node_by_peer_id("peer-a")
            .unwrap()
            .capabilities
            .executable_models
            .is_empty());
    }

    #[test]
    fn cluster_mock_worker_keeps_simple_tasks() {
        let mut registry = ClusterRegistry::new("test-lan");
        let heartbeat = serde_json::json!({
            "type": "Heartbeat",
            "peer_id": "peer-a",
            "capabilities": {
                "supported_tasks": ["reverse_string", "test", "echo"]
            }
        });
        registry.update_from_worker_heartbeat_value(&heartbeat, 100);

        assert_eq!(
            registry
                .node_by_peer_id("peer-a")
                .unwrap()
                .capabilities
                .supported_task_types,
            vec![
                "echo".to_string(),
                "reverse_string".to_string(),
                "test".to_string()
            ]
        );
    }

    #[test]
    fn cluster_registry_filters_nodes_supporting_task() {
        let mut registry = ClusterRegistry::new("test-lan");
        let heartbeat = serde_json::json!({
            "type": "Heartbeat",
            "peer_id": "peer-a",
            "capabilities": {
                "supported_tasks": ["reverse_string", "test", "echo"]
            }
        });
        registry.update_from_worker_heartbeat_value(&heartbeat, 100);

        let nodes = registry.nodes_supporting_task("reverse_string");

        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].peer_id, "peer-a");
    }

    #[test]
    fn cluster_registry_filters_nodes_with_model() {
        let mut registry = ClusterRegistry::new("test-lan");
        registry.update_from_capability_heartbeat(&heartbeat("peer-a"), 100);

        let nodes = registry.nodes_with_model("tinyllama-1b");

        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].peer_id, "peer-a");
    }

    #[test]
    fn cluster_registry_health_bucket_helpers() {
        let mut registry = ClusterRegistry::new("test-lan");
        registry.add_or_update_discovered_node("peer-healthy", None, 100_000);
        registry.add_or_update_discovered_node("peer-stale", None, 60_000);
        registry.add_or_update_discovered_node("peer-offline", None, 1_000);
        let degraded = ClusterNodeStatusMessage::worker(
            "test-lan",
            "peer-degraded",
            "wrk-degraded",
            ClusterExecutionMode::Degraded,
            ClusterBackend::Cpu,
            false,
            vec!["reverse_string".to_string()],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            ClusterMetricsStatus::Fallback,
            None,
        );
        registry.update_from_cluster_status_message(degraded, 100_000);

        assert_eq!(registry.healthy_nodes(100_000).len(), 1);
        assert_eq!(registry.stale_nodes(100_000).len(), 1);
        assert_eq!(registry.degraded_nodes(100_000).len(), 1);
        assert_eq!(registry.offline_nodes(100_000).len(), 1);
    }
}
