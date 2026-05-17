use crate::node_modes::{InferenceControlFlags, NodeMode};
use libp2p::Multiaddr;
use std::str::FromStr;

pub(crate) fn parse_args() -> Result<NodeMode, String> {
    parse_args_from(std::env::args().collect())
}

pub(crate) fn contains_help_flag(args: &[String]) -> bool {
    args.iter()
        .any(|arg| matches!(arg.as_str(), "--help" | "-h"))
}

pub(crate) fn parse_args_from(raw_args: Vec<String>) -> Result<NodeMode, String> {
    if contains_help_flag(&raw_args) {
        return Ok(NodeMode::Help);
    }

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
        Some("--help") | Some("-h") | Some("help") => Ok(NodeMode::Help),
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
            Some("stats") => Ok(NodeMode::TasksStats {
                json: args.iter().any(|arg| arg == "--json"),
            }),
            Some("trace") => {
                let task_id = args.get(3).ok_or("Falta <task_id>")?.clone();
                Ok(NodeMode::TasksTrace {
                    task_id,
                    json: args.iter().any(|arg| arg == "--json"),
                })
            }
            _ => Err("Uso: iamine-node tasks [stats [--json]|trace <task_id> [--json]]".to_string()),
        },

        Some("cluster") => match args.get(2).map(|s| s.as_str()) {
            Some("status") => Ok(NodeMode::ClusterStatus {
                json: args.iter().any(|arg| arg == "--json"),
            }),
            _ => Err("Uso: iamine-node cluster status [--json]".to_string()),
        },

        Some("nodes") => Ok(NodeMode::Nodes),

        Some("topology") => Ok(NodeMode::Topology), // ← NEW

        Some(unknown) => Err(format!("Modo desconocido: {}", unknown)),
    }
}

pub(crate) fn parse_optional_u32_flag(args: &[String], flag: &str) -> Result<Option<u32>, String> {
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

pub(crate) fn parse_worker_port(args: &[String]) -> u16 {
    if let Some(port_arg) = args.iter().find(|arg| arg.starts_with("--port=")) {
        port_arg.replace("--port=", "").parse().unwrap_or(9000)
    } else {
        9000
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn cli_help_prints_usage() {
        let mode = parse_args_from(args(&["iamine-node", "--help"])).expect("--help should parse");

        assert!(matches!(mode, NodeMode::Help));
        assert!(crate::usage::usage_text().contains("iamine-node --broadcast <type> <data>"));
        assert!(crate::usage::usage_text().contains("--debug-network"));
    }

    #[test]
    fn cli_short_help_prints_usage() {
        let mode = parse_args_from(args(&["iamine-node", "-h"])).expect("-h should parse");

        assert!(matches!(mode, NodeMode::Help));
        assert!(crate::usage::usage_text().contains("iamine-node --worker [--port=N]"));
    }

    #[test]
    fn cli_help_does_not_start_runtime() {
        let mode = parse_args_from(args(&["iamine-node", "--help"])).expect("help should parse");

        assert!(matches!(mode, NodeMode::Help));
    }

    #[test]
    fn cli_short_help_does_not_start_runtime() {
        let mode = parse_args_from(args(&["iamine-node", "-h"])).expect("short help should parse");

        assert!(matches!(mode, NodeMode::Help));
    }

    #[test]
    fn cli_legacy_help_word_does_not_start_runtime() {
        let mode = parse_args_from(args(&["iamine-node", "help"])).expect("help should parse");

        assert!(matches!(mode, NodeMode::Help));
    }

    #[test]
    fn cli_worker_help_does_not_start_runtime() {
        let mode = parse_args_from(args(&["iamine-node", "--worker", "--help"]))
            .expect("worker help should parse");

        assert!(matches!(mode, NodeMode::Help));
    }

    #[test]
    fn cli_worker_port_help_does_not_start_runtime() {
        let mode = parse_args_from(args(&["iamine-node", "--worker", "--port=4101", "--help"]))
            .expect("worker port help should parse");

        assert!(matches!(mode, NodeMode::Help));
    }

    #[test]
    fn cli_broadcast_help_does_not_start_runtime() {
        let mode = parse_args_from(args(&[
            "iamine-node",
            "--broadcast",
            "reverse_string",
            "test",
            "--help",
        ]))
        .expect("broadcast help should parse");

        assert!(matches!(mode, NodeMode::Help));
    }

    #[test]
    fn cli_infer_force_network_help_does_not_start_runtime() {
        let mode = parse_args_from(args(&[
            "iamine-node",
            "infer",
            "2+2",
            "--force-network",
            "--help",
        ]))
        .expect("infer help should parse");

        assert!(matches!(mode, NodeMode::Help));
    }

    #[test]
    fn cli_unknown_mode_does_not_start_runtime() {
        let error = parse_args_from(args(&["iamine-node", "unknown-mode"])).unwrap_err();

        assert_eq!(error, "Modo desconocido: unknown-mode");
    }

    #[test]
    fn cli_detects_worker_mode() {
        let mode = parse_args_from(args(&["iamine-node", "--worker", "--port=4101"]))
            .expect("worker mode should parse");

        assert!(matches!(mode, NodeMode::Worker));
    }

    #[test]
    fn cli_detects_broadcast_mode() {
        let mode = parse_args_from(args(&[
            "iamine-node",
            "--broadcast",
            "reverse_string",
            "abc",
        ]))
        .expect("broadcast mode should parse");

        match mode {
            NodeMode::Broadcast { task_type, data } => {
                assert_eq!(task_type, "reverse_string");
                assert_eq!(data, "abc");
            }
            other => panic!("unexpected mode: {other:?}"),
        }
    }

    #[test]
    fn cli_detects_force_network_infer_mode() {
        let mode = parse_args_from(args(&["iamine-node", "infer", "2+2", "--force-network"]))
            .expect("infer mode should parse");

        match mode {
            NodeMode::Infer {
                prompt,
                force_network,
                no_local,
                prefer_local,
                ..
            } => {
                assert_eq!(prompt, "2+2");
                assert!(force_network);
                assert!(!no_local);
                assert!(!prefer_local);
            }
            other => panic!("unexpected mode: {other:?}"),
        }
    }

    #[test]
    fn cli_valid_commands_do_not_show_unknown_mode() {
        assert!(matches!(
            parse_args_from(args(&["iamine-node", "--worker", "--port=4101"])).unwrap(),
            NodeMode::Worker
        ));
        assert!(matches!(
            parse_args_from(args(&[
                "iamine-node",
                "--broadcast",
                "reverse_string",
                "abc"
            ]))
            .unwrap(),
            NodeMode::Broadcast { .. }
        ));
        assert!(matches!(
            parse_args_from(args(&["iamine-node", "infer", "2+2", "--force-network"])).unwrap(),
            NodeMode::Infer {
                force_network: true,
                ..
            }
        ));
        assert!(matches!(
            parse_args_from(args(&["iamine-node", "cluster", "status"])).unwrap(),
            NodeMode::ClusterStatus { json: false }
        ));
        assert!(matches!(
            parse_args_from(args(&["iamine-node", "tasks", "trace", "task-1"])).unwrap(),
            NodeMode::TasksTrace {
                task_id,
                json: false
            } if task_id == "task-1"
        ));
        assert!(matches!(
            parse_args_from(args(&["iamine-node", "tasks", "stats", "--json"])).unwrap(),
            NodeMode::TasksStats { json: true }
        ));
    }

    #[test]
    fn cli_tasks_trace_json_mode() {
        let mode = parse_args_from(args(&[
            "iamine-node",
            "tasks",
            "trace",
            "task-json",
            "--json",
        ]))
        .expect("tasks trace json should parse");

        match mode {
            NodeMode::TasksTrace { task_id, json } => {
                assert_eq!(task_id, "task-json");
                assert!(json);
            }
            other => panic!("unexpected mode: {other:?}"),
        }
    }

    #[test]
    fn tasks_trace_cli_help_does_not_start_runtime() {
        let mode = parse_args_from(args(&["iamine-node", "tasks", "trace", "--help"]))
            .expect("tasks trace help should parse");

        assert!(matches!(mode, NodeMode::Help));
    }

    #[test]
    fn test_parse_optional_u32_flag() {
        let args = args(&["iamine-node", "infer", "explica", "--max-tokens", "1024"]);

        assert_eq!(
            parse_optional_u32_flag(&args, "--max-tokens").unwrap(),
            Some(1024)
        );
    }

    #[test]
    fn cli_parse_port_flag() {
        let args = args(&["iamine-node", "--worker", "--port=4101"]);

        assert_eq!(parse_worker_port(&args), 4101);
    }

    #[test]
    fn cli_invalid_port_preserves_existing_default() {
        let args = args(&["iamine-node", "--worker", "--port=bad"]);

        assert_eq!(parse_worker_port(&args), 9000);
    }

    #[test]
    fn cli_detects_cluster_status_mode() {
        let mode = parse_args_from(args(&["iamine-node", "cluster", "status"]))
            .expect("cluster status should parse");

        assert!(matches!(mode, NodeMode::ClusterStatus { json: false }));
    }

    #[test]
    fn cli_detects_cluster_status_json_mode() {
        let mode = parse_args_from(args(&["iamine-node", "cluster", "status", "--json"]))
            .expect("cluster status json should parse");

        assert!(matches!(mode, NodeMode::ClusterStatus { json: true }));
    }

    #[test]
    fn cli_cluster_status_help_does_not_start_runtime() {
        let mode = parse_args_from(args(&["iamine-node", "cluster", "status", "--help"]))
            .expect("cluster status help should parse");

        assert!(matches!(mode, NodeMode::Help));
    }
}
