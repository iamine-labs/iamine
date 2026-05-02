use libp2p::Multiaddr;
use std::str::FromStr;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(super) enum NodeMode {
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
    ModelsMenu,
    ModelsSearch {
        query: String,
    },
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
    TasksStats,
    TasksTrace {
        task_id: String,
    },
    Capabilities,
    Nodes,
    Topology,
    ModelsRecommend,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct DebugFlags {
    pub(super) network: bool,
    pub(super) scheduler: bool,
    pub(super) tasks: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct InferenceControlFlags {
    pub(super) force_network: bool,
    pub(super) no_local: bool,
    pub(super) prefer_local: bool,
}

impl InferenceControlFlags {
    fn from_args(args: &[String]) -> Self {
        Self {
            force_network: args.iter().any(|arg| arg == "--force-network"),
            no_local: args.iter().any(|arg| arg == "--no-local"),
            prefer_local: args.iter().any(|arg| arg == "--prefer-local"),
        }
    }

    pub(super) fn should_use_local(self, has_local_model: bool) -> bool {
        if self.force_network || self.no_local {
            return false;
        }
        if self.prefer_local {
            return has_local_model;
        }
        has_local_model
    }
}

impl DebugFlags {
    pub(super) fn from_args(args: &[String]) -> Self {
        Self {
            network: args.iter().any(|arg| arg == "--debug-network"),
            scheduler: args.iter().any(|arg| arg == "--debug-scheduler"),
            tasks: args.iter().any(|arg| arg == "--debug-tasks"),
        }
    }
}

pub(super) fn mode_label(mode: &NodeMode) -> &'static str {
    match mode {
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
        NodeMode::TasksStats => "tasks-stats",
        NodeMode::TasksTrace { .. } => "tasks-trace",
        NodeMode::Capabilities => "capabilities",
        NodeMode::Nodes => "nodes",
        NodeMode::Topology => "topology",
        NodeMode::ModelsRecommend => "models-recommend",
    }
}

pub(super) fn parse_args() -> Result<NodeMode, String> {
    parse_mode_from_args(std::env::args().collect())
}

pub(super) fn parse_mode_from_args(raw_args: Vec<String>) -> Result<NodeMode, String> {
    let args: Vec<String> = raw_args
        .into_iter()
        .filter(|arg| {
            !matches!(
                arg.as_str(),
                "--debug-network" | "--debug-scheduler" | "--debug-tasks"
            )
        })
        .collect();

    match args.get(1).map(|s| s.as_str()) {
        Some("--daemon") => Ok(NodeMode::Daemon),
        Some("--worker") | None => Ok(NodeMode::Worker),
        Some("--relay") => Ok(NodeMode::Relay),
        Some("models") => match args.get(2).map(|s| s.as_str()) {
            Some("list") => Ok(NodeMode::ModelsList),
            Some("stats") => Ok(NodeMode::ModelsStats),
            Some("recommend") => Ok(NodeMode::ModelsRecommend),
            Some("menu") => Ok(NodeMode::ModelsMenu),
            Some("search") => {
                let query = args.get(3).ok_or("Falta <query>")?.clone();
                Ok(NodeMode::ModelsSearch { query })
            }
            Some("download") => {
                let id = args.get(3).ok_or("Falta <model_id>")?.clone();
                Ok(NodeMode::ModelsDownload { model_id: id })
            }
            Some("remove") => {
                let id = args.get(3).ok_or("Falta <model_id>")?.clone();
                Ok(NodeMode::ModelsRemove { model_id: id })
            }
            _ => Err(
                "Uso: iamine models [list|stats|recommend|menu|search <q>|download <id>|remove <id>]"
                    .to_string(),
            ),
        },
        Some("--client") => {
            let (peer, offset) = if args.get(2).map(|s| s.starts_with("/ip4")).unwrap_or(false) {
                (
                    Some(Multiaddr::from_str(args.get(2).unwrap()).map_err(|e| e.to_string())?),
                    3,
                )
            } else {
                (None, 2)
            };
            let task_type = args.get(offset).ok_or("Falta <task_type>")?.clone();
            let data = args.get(offset + 1).ok_or("Falta <data>")?.clone();
            Ok(NodeMode::Client {
                peer,
                task_type,
                data,
            })
        }
        Some("--stress") => {
            let (peer, offset) = if args.get(2).map(|s| s.starts_with("/ip4")).unwrap_or(false) {
                (
                    Some(Multiaddr::from_str(args.get(2).unwrap()).map_err(|e| e.to_string())?),
                    3,
                )
            } else {
                (None, 2)
            };
            let count = args
                .get(offset)
                .unwrap_or(&"10".to_string())
                .parse::<usize>()
                .unwrap_or(10);
            Ok(NodeMode::Stress { peer, count })
        }
        Some("--broadcast") => {
            let task_type = args.get(2).ok_or("Falta <task_type>")?.clone();
            let data = args.get(3).ok_or("Falta <data>")?.clone();
            Ok(NodeMode::Broadcast { task_type, data })
        }
        Some("--simulate-workers") => {
            let count = args
                .get(2)
                .unwrap_or(&"10".to_string())
                .parse::<usize>()
                .unwrap_or(10);
            Ok(NodeMode::SimulateWorkers { count })
        }
        Some("test-inference") => {
            let prompt = args
                .get(2)
                .cloned()
                .unwrap_or_else(|| "What is 2+2?".to_string());
            Ok(NodeMode::TestInference { prompt })
        }
        Some("capabilities") => Ok(NodeMode::Capabilities),
        Some("infer") => {
            let prompt = args.get(2).ok_or("Falta <prompt>")?.clone();
            let model_id = args
                .iter()
                .position(|a| a == "--model")
                .and_then(|i| args.get(i + 1).cloned());
            let max_tokens_override = parse_optional_u32_flag(&args, "--max-tokens")?;
            let control_flags = InferenceControlFlags::from_args(&args);
            Ok(NodeMode::Infer {
                prompt,
                model_id,
                max_tokens_override,
                force_network: control_flags.force_network,
                no_local: control_flags.no_local,
                prefer_local: control_flags.prefer_local,
            })
        }
        Some("semantic-eval") => Ok(NodeMode::SemanticEval),
        Some("regression-run") => Ok(NodeMode::RegressionRun),
        Some("check-code") => Ok(NodeMode::CheckCode),
        Some("check-security") => Ok(NodeMode::CheckSecurity),
        Some("validate-release") => Ok(NodeMode::ValidateRelease),
        Some("tasks") => match args.get(2).map(|s| s.as_str()) {
            Some("stats") => Ok(NodeMode::TasksStats),
            Some("trace") => {
                let task_id = args.get(3).ok_or("Falta <task_id>")?.clone();
                Ok(NodeMode::TasksTrace { task_id })
            }
            _ => Err("Uso: iamine-node tasks [stats|trace <task_id>]".to_string()),
        },
        Some("nodes") => Ok(NodeMode::Nodes),
        Some("topology") => Ok(NodeMode::Topology),
        Some(unknown) => Err(format!("Modo desconocido: {}", unknown)),
    }
}

pub(super) fn parse_optional_u32_flag(args: &[String], flag: &str) -> Result<Option<u32>, String> {
    let Some(index) = args.iter().position(|arg| arg == flag) else {
        return Ok(None);
    };

    let Some(raw) = args.get(index + 1) else {
        return Err(format!("Falta valor para {}", flag));
    };

    raw.parse::<u32>()
        .map(Some)
        .map_err(|_| format!("Valor invalido para {}: {}", flag, raw))
}

pub(super) fn is_control_plane_mode(mode: &NodeMode) -> bool {
    matches!(
        mode,
        NodeMode::ModelsList
            | NodeMode::ModelsStats
            | NodeMode::ModelsDownload { .. }
            | NodeMode::ModelsRemove { .. }
            | NodeMode::ModelsMenu
            | NodeMode::ModelsSearch { .. }
            | NodeMode::ModelsRecommend
            | NodeMode::SemanticEval
            | NodeMode::RegressionRun
            | NodeMode::CheckCode
            | NodeMode::CheckSecurity
            | NodeMode::ValidateRelease
            | NodeMode::TasksStats
            | NodeMode::TasksTrace { .. }
            | NodeMode::Capabilities
            | NodeMode::Nodes
            | NodeMode::Topology
    )
}
