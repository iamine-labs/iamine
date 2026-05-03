use super::*;

#[test]
fn test_max_tokens_override() {
    let profile = analyze_prompt("explica la teoria de la relatividad");
    let decision =
        resolve_output_policy(&profile, "explica la teoria de la relatividad", Some(100));

    assert_eq!(decision.max_tokens, 100);
    assert_eq!(decision.reason, "cli override");
}

#[test]
fn test_max_tokens_override_is_clamped() {
    let profile = analyze_prompt("What is 2+2?");
    let decision = resolve_output_policy(&profile, "What is 2+2?", Some(10));

    assert_eq!(decision.max_tokens, 64);
}

#[test]
fn test_parse_optional_u32_flag() {
    let args = vec![
        "iamine-node".to_string(),
        "infer".to_string(),
        "explica".to_string(),
        "--max-tokens".to_string(),
        "1024".to_string(),
    ];

    assert_eq!(
        cli::parse_optional_u32_flag(&args, "--max-tokens").unwrap(),
        Some(1024)
    );
}

#[test]
fn test_exact_math_uses_natural_prompt_on_first_attempt() {
    assert_eq!(
        with_task_guard("What is 2+2?", PromptTaskType::ExactMath, false),
        "What is 2+2?"
    );
}

#[test]
fn test_exact_math_retry_guard_is_simple() {
    let guarded = with_task_guard("6x9", PromptTaskType::ExactMath, true);
    assert!(guarded.contains("Return only the final numeric answer"));
    assert!(guarded.ends_with("\n\n6x9"));
}

#[test]
fn test_decimal_sequence_retry_guard_mentions_decimal_point() {
    let guarded = with_task_guard(
        "dame los primeros 100 digitos de pi",
        PromptTaskType::ExactMath,
        true,
    );
    assert!(guarded.contains("Keep the decimal point attached"));
}

#[test]
fn test_cli_no_runtime_start() {
    assert!(is_control_plane_mode(&NodeMode::ModelsList));
    assert!(is_control_plane_mode(&NodeMode::ModelsStats));
    assert!(is_control_plane_mode(&NodeMode::ModelsDownload {
        model_id: "llama3-3b".to_string()
    }));
    assert!(is_control_plane_mode(&NodeMode::ModelsRemove {
        model_id: "llama3-3b".to_string()
    }));
    assert!(is_control_plane_mode(&NodeMode::TasksStats));
    assert!(is_control_plane_mode(&NodeMode::TasksTrace {
        task_id: "task-1".to_string()
    }));
    assert!(!is_control_plane_mode(&NodeMode::Worker));
}

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
fn test_empty_result_does_not_print_as_success_response() {
    assert!(!should_print_result_output(""));
    assert!(!should_print_result_output("   "));
    assert!(should_print_result_output("ok"));
}

#[test]
fn test_identity_conflict_path_uses_ephemeral_identity_strategy() {
    let identity_a = NodeIdentity::ephemeral("test-identity-safety");
    let identity_b = NodeIdentity::ephemeral("test-identity-safety");

    assert_ne!(identity_a.peer_id, identity_b.peer_id);
    assert_ne!(identity_a.node_id, identity_b.node_id);
}

#[test]
fn test_human_logs_remain_unaffected() {
    let human_line = "✅ Conectado a: 12D3KooWorker (/ip4/127.0.0.1/tcp/7001)";
    assert!(serde_json::from_str::<serde_json::Value>(human_line).is_err());
    assert!(human_line.contains("Conectado a"));
}

#[test]
fn test_parse_mode_from_args_infer_and_debug_flags_are_filtered() {
    let args = vec![
        "iamine-node".to_string(),
        "infer".to_string(),
        "2+2".to_string(),
        "--debug-network".to_string(),
        "--max-tokens".to_string(),
        "256".to_string(),
    ];
    let mode = cli::parse_mode_from_args(args).expect("infer mode should parse");
    match mode {
        NodeMode::Infer {
            prompt,
            max_tokens_override,
            ..
        } => {
            assert_eq!(prompt, "2+2");
            assert_eq!(max_tokens_override, Some(256));
        }
        _ => panic!("expected infer mode"),
    }
}
