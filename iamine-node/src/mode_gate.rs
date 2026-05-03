use super::*;
use crate::infer_mode_handlers::{handle_infer_mode, InferModeOutcome};

pub(super) enum ModeGateOutcome {
    ContinueRuntime,
    Exit,
}

pub(super) async fn handle_pre_runtime_mode(
    mode: &NodeMode,
) -> Result<ModeGateOutcome, Box<dyn Error>> {
    match mode {
        NodeMode::ModelsList => {
            handle_models_list();
            Ok(ModeGateOutcome::Exit)
        }
        NodeMode::ModelsStats => {
            handle_models_stats();
            Ok(ModeGateOutcome::Exit)
        }
        NodeMode::TasksStats => {
            handle_tasks_stats();
            Ok(ModeGateOutcome::Exit)
        }
        NodeMode::TasksTrace { task_id } => {
            handle_tasks_trace(task_id)?;
            Ok(ModeGateOutcome::Exit)
        }
        NodeMode::ModelsMenu => {
            handle_models_menu()?;
            Ok(ModeGateOutcome::Exit)
        }
        NodeMode::ModelsSearch { query } => {
            handle_models_search(query).await;
            Ok(ModeGateOutcome::Exit)
        }
        NodeMode::ModelsDownload { model_id } => {
            handle_models_download(model_id).await;
            Ok(ModeGateOutcome::Exit)
        }
        NodeMode::ModelsRemove { model_id } => {
            handle_models_remove(model_id)?;
            Ok(ModeGateOutcome::Exit)
        }
        NodeMode::SemanticEval => {
            handle_semantic_eval()?;
            Ok(ModeGateOutcome::Exit)
        }
        NodeMode::RegressionRun => {
            handle_regression_run()?;
            Ok(ModeGateOutcome::Exit)
        }
        NodeMode::CheckCode => {
            handle_check_code()?;
            Ok(ModeGateOutcome::Exit)
        }
        NodeMode::CheckSecurity => {
            handle_check_security()?;
            Ok(ModeGateOutcome::Exit)
        }
        NodeMode::ValidateRelease => {
            handle_validate_release()?;
            Ok(ModeGateOutcome::Exit)
        }
        NodeMode::Daemon => {
            println!("╔══════════════════════════════════╗");
            println!("║   IaMine — Inference Daemon      ║");
            println!("╚══════════════════════════════════╝\n");
            let node_identity = NodeIdentity::load_or_create();
            set_global_node_id(&node_identity.peer_id.to_string());
            let socket = daemon_socket_path();
            log_observability_event(
                LogLevel::Info,
                "daemon_started",
                "startup",
                None,
                None,
                None,
                {
                    let mut fields = Map::new();
                    fields.insert(
                        "peer_id".to_string(),
                        node_identity.peer_id.to_string().into(),
                    );
                    fields.insert(
                        "socket_path".to_string(),
                        socket.display().to_string().into(),
                    );
                    fields.insert("mode".to_string(), "daemon".into());
                    fields
                },
            );
            run_daemon(socket).await?;
            Ok(ModeGateOutcome::Exit)
        }
        NodeMode::TestInference { prompt } => {
            handle_test_inference(prompt).await?;
            Ok(ModeGateOutcome::Exit)
        }
        NodeMode::Infer {
            prompt,
            model_id,
            max_tokens_override,
            force_network,
            no_local,
            prefer_local,
        } => {
            let control_flags = InferenceControlFlags {
                force_network: *force_network,
                no_local: *no_local,
                prefer_local: *prefer_local,
            };
            let outcome = handle_infer_mode(
                prompt,
                model_id.as_deref(),
                *max_tokens_override,
                control_flags,
            )
            .await?;
            if outcome == InferModeOutcome::CompletedLocally {
                Ok(ModeGateOutcome::Exit)
            } else {
                Ok(ModeGateOutcome::ContinueRuntime)
            }
        }
        NodeMode::Capabilities => {
            handle_capabilities();
            Ok(ModeGateOutcome::Exit)
        }
        NodeMode::Nodes => {
            println!("╔══════════════════════════════════╗");
            println!("║      IaMine — Nodes View         ║");
            println!("╚══════════════════════════════════╝\n");
            Ok(ModeGateOutcome::ContinueRuntime)
        }
        NodeMode::Topology => {
            println!("╔══════════════════════════════════╗");
            println!("║    IaMine — Network Topology     ║");
            println!("╚══════════════════════════════════╝\n");
            Ok(ModeGateOutcome::ContinueRuntime)
        }
        NodeMode::ModelsRecommend => {
            handle_models_recommend();
            Ok(ModeGateOutcome::Exit)
        }
        _ => Ok(ModeGateOutcome::ContinueRuntime),
    }
}
