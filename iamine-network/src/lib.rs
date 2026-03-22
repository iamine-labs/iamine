pub mod expression_parser;
pub mod node_registry;
pub mod model_capability_matcher;
pub mod model_policy;
pub mod output_policy;
pub mod prompt_analyzer;
pub mod semantic_eval;
pub mod semantic_feedback;
pub mod semantic_validator;
pub mod task_analyzer;
pub mod topology;

pub use node_registry::{
    NodeCapability,
    NodeCapabilityHeartbeat,
    NodeRegistry,
    SharedNodeRegistry,
};
pub use model_policy::{ModelPolicyEngine, PolicyRule};
pub use output_policy::{compute_max_tokens, continue_inference, describe_output_policy, OutputPolicyDecision};
pub use model_capability_matcher::{
    is_node_compatible_with_model,
    ModelHardwareRequirements,
    NodeHardwareProfile,
};
pub use expression_parser::normalize_expression;
pub use prompt_analyzer::{
    analyze_prompt, Complexity, DeterministicLevel, Domain, Language, OutputStyle, PromptProfile,
    SemanticProfile,
};
pub use prompt_analyzer::{
    analyze_prompt_semantics,
    analyze_prompt_semantics_with_context,
    estimate_confidence,
    SemanticRoutingDecision,
    Signal,
    SignalKind,
    CONFIDENCE_THRESHOLD,
};
pub use semantic_eval::{
    evaluate_dataset,
    evaluate_default_dataset,
    load_default_dataset,
    should_use_strict_handling,
    SemanticDatasetEntry,
    SemanticEvalError,
    SemanticEvalReport,
};
pub use semantic_feedback::{
    default_log_path as default_semantic_log_path, ConflictMetric, SemanticFeedbackEngine,
    SemanticFeedbackMetrics, SemanticLog,
};
pub use semantic_validator::{
    model_validate, validate_semantic_decision, validate_semantic_decision_with_context,
    ValidatedSemanticDecision, ValidationResult,
};
pub use task_analyzer::{detect_exact_subtype, detect_task_type, ExactSubtype, TaskProfile, TaskType};
pub use topology::{
    NetworkTopology,
    SharedNetworkTopology,
    PeerLatency,
    Cluster,
    ClusterTier,
};
