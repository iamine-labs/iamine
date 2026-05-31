use crate::cluster_stress::ClusterStressConfig;
use libp2p::Multiaddr;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) enum NodeMode {
    Help,
    Daemon,
    Worker,
    Relay,
    Client {
        peer: Option<Multiaddr>,
        task_type: String,
        data: String,
    },
    Stress {
        peer: Option<Multiaddr>,
        count: usize,
    },
    Broadcast {
        task_type: String,
        data: String,
    },
    SimulateWorkers {
        count: usize,
    },
    ModelsList,
    ModelsStats,
    ModelsDownload {
        model_id: String,
    },
    ModelsRemove {
        model_id: String,
    },
    ModelsMenu, // ← nuevo
    ModelsSearch {
        query: String,
    }, // ← nuevo
    TestInference {
        prompt: String,
    },
    Infer {
        prompt: String,
        model_id: Option<String>,
        max_tokens_override: Option<u32>,
        force_network: bool,
        no_local: bool,
        prefer_local: bool,
    },
    SemanticEval,
    RegressionRun,
    CheckCode,
    CheckSecurity,
    ValidateRelease,
    TasksStats {
        json: bool,
    },
    TasksTrace {
        task_id: String,
        json: bool,
    },
    ClusterStatus {
        json: bool,
    },
    ClusterStress {
        config: ClusterStressConfig,
    },
    Capabilities,
    Nodes,
    Topology, // ← NEW
    ModelsRecommend,
}

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DebugFlags {
    pub(crate) network: bool,
    pub(crate) scheduler: bool,
    pub(crate) tasks: bool,
}

impl DebugFlags {
    pub(crate) fn from_args(args: &[String]) -> Self {
        Self {
            network: args.iter().any(|arg| arg == "--debug-network"),
            scheduler: args.iter().any(|arg| arg == "--debug-scheduler"),
            tasks: args.iter().any(|arg| arg == "--debug-tasks"),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct InferenceControlFlags {
    pub(crate) force_network: bool,
    pub(crate) no_local: bool,
    pub(crate) prefer_local: bool,
}

impl InferenceControlFlags {
    pub(crate) fn from_args(args: &[String]) -> Self {
        Self {
            force_network: args.iter().any(|arg| arg == "--force-network"),
            no_local: args.iter().any(|arg| arg == "--no-local"),
            prefer_local: args.iter().any(|arg| arg == "--prefer-local"),
        }
    }

    pub(crate) fn should_use_local(self, has_local_model: bool) -> bool {
        if self.force_network || self.no_local {
            return false;
        }
        if self.prefer_local {
            return has_local_model;
        }
        has_local_model
    }
}

pub(crate) fn mode_label(mode: &NodeMode) -> &'static str {
    match mode {
        NodeMode::Help => "help",
        NodeMode::Daemon => "daemon",
        NodeMode::Worker => "worker",
        NodeMode::Relay => "relay",
        NodeMode::Client { .. } => "client",
        NodeMode::Stress { .. } => "stress",
        NodeMode::Broadcast { .. } => "broadcast",
        NodeMode::SimulateWorkers { .. } => "simulate-workers",
        NodeMode::ModelsList => "models-list",
        NodeMode::ModelsStats => "models-stats",
        NodeMode::ModelsDownload { .. } => "models-download",
        NodeMode::ModelsRemove { .. } => "models-remove",
        NodeMode::ModelsMenu => "models-menu",
        NodeMode::ModelsSearch { .. } => "models-search",
        NodeMode::TestInference { .. } => "test-inference",
        NodeMode::Infer { .. } => "infer",
        NodeMode::SemanticEval => "semantic-eval",
        NodeMode::RegressionRun => "regression-run",
        NodeMode::CheckCode => "check-code",
        NodeMode::CheckSecurity => "check-security",
        NodeMode::ValidateRelease => "validate-release",
        NodeMode::TasksStats { .. } => "tasks-stats",
        NodeMode::TasksTrace { .. } => "tasks-trace",
        NodeMode::ClusterStatus { .. } => "cluster-status",
        NodeMode::ClusterStress { .. } => "cluster-stress",
        NodeMode::Capabilities => "capabilities",
        NodeMode::Nodes => "nodes",
        NodeMode::Topology => "topology",
        NodeMode::ModelsRecommend => "models-recommend",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_force_network_routing() {
        let flags = InferenceControlFlags {
            force_network: true,
            no_local: false,
            prefer_local: true,
        };
        assert!(!flags.should_use_local(true));

        let prefer_local = InferenceControlFlags {
            force_network: false,
            no_local: false,
            prefer_local: true,
        };
        assert!(prefer_local.should_use_local(true));
        assert!(!prefer_local.should_use_local(false));
    }

    #[test]
    fn cli_detects_debug_flags() {
        let args = vec![
            "iamine-node".to_string(),
            "--debug-network".to_string(),
            "--debug-scheduler".to_string(),
            "--debug-tasks".to_string(),
        ];

        let flags = DebugFlags::from_args(&args);

        assert!(flags.network);
        assert!(flags.scheduler);
        assert!(flags.tasks);
    }
}
