// ─── Tests v0.6: Distributed Inference ────────────────────────────────────

#[test]
fn test_inference_task_serialization() {
    use iamine_models::InferenceTask;
    let task = InferenceTask::new(
        "req-001".to_string(),
        "tinyllama-1b".to_string(),
        "What is gravity?".to_string(),
        200,
        "peer_abc".to_string(),
    );
    let json = task.to_gossip_json();
    assert_eq!(json["type"], "InferenceRequest");
    assert_eq!(json["model_id"], "tinyllama-1b");
    assert_eq!(json["prompt"], "What is gravity?");
    assert_eq!(json["max_tokens"], 200);
    assert_eq!(json["requester_peer"], "peer_abc");
}

#[test]
fn test_inference_result_success() {
    use iamine_models::InferenceTaskResult;
    let result = InferenceTaskResult::success(
        "req-001".to_string(),
        "tinyllama-1b".to_string(),
        "Gravity is a force...".to_string(),
        10,
        false,
        0,
        150,
        "worker_xyz".to_string(),
        "Metal".to_string(),
    );
    assert!(result.success);
    assert_eq!(result.tokens_generated, 10);
    assert!(!result.truncated);
    assert_eq!(result.continuation_steps, 0);
    let json = result.to_gossip_json();
    assert_eq!(json["type"], "InferenceResult");
    assert_eq!(json["success"], true);
    assert_eq!(json["truncated"], false);
    assert_eq!(json["continuation_steps"], 0);
}

#[test]
fn test_inference_result_failure() {
    use iamine_models::InferenceTaskResult;
    let result = InferenceTaskResult::failure(
        "req-002".to_string(),
        "mistral-7b".to_string(),
        "worker_abc".to_string(),
        "Model not installed".to_string(),
    );
    assert!(!result.success);
    assert!(result.error.is_some());
    let json = result.to_gossip_json();
    assert_eq!(json["success"], false);
}

#[test]
fn test_streamed_token() {
    use iamine_models::StreamedToken;
    let token = StreamedToken {
        request_id: "req-001".to_string(),
        token: "hello ".to_string(),
        index: 0,
        is_final: false,
    };
    let json = token.to_gossip_json();
    assert_eq!(json["type"], "InferenceToken");
    assert_eq!(json["token"], "hello ");
    assert_eq!(json["is_final"], false);
}
