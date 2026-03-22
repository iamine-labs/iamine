pub mod continuation_manager;
pub mod distributed_inference;
pub mod hardware_acceleration;
pub mod huggingface_search;
pub mod inference;
pub mod inference_engine;
pub mod inference_queue;
pub mod model_auto_provision;
pub mod model_cache;
pub mod model_downloader;
pub mod model_events;
pub mod model_installer;
pub mod model_registry;
pub mod model_requirements;
pub mod model_selector;
pub mod model_signature;
pub mod model_storage;
pub mod model_validator;
pub mod model_verifier;
pub mod node_capabilities;
pub mod node_models;
pub mod output_cleaner;
pub mod output_normalizer;
pub mod output_validator;
pub mod prompt_builder;
pub mod signed_metrics;
pub mod storage_config; // ← NUEVO

pub use continuation_manager::ContinuationManager;
pub use distributed_inference::{
    DirectInferenceRequest, InferenceTask, InferenceTaskResult, StreamedToken,
};
pub use hardware_acceleration::{AcceleratorType, HardwareAcceleration};
pub use huggingface_search::HuggingFaceSearch;
pub use inference::{InferenceEngine, InferenceRequest, InferenceResult};
pub use inference_engine::{
    BackendType, InferenceContext, InferenceEngine as RealInferenceEngine,
    InferenceRequest as RealInferenceRequest, InferenceResult as RealInferenceResult,
    SamplingConfig,
};
pub use inference_queue::{InferenceQueue, InferenceRequest as QueuedInferenceRequest};
pub use model_auto_provision::{AutoProvisionProfile, ModelAutoProvision};
pub use model_cache::{LoadedModel, ModelCache};
pub use model_downloader::{DownloadPhase, DownloadProgress, ModelDownloader};
pub use model_events::{CapabilitiesUpdatedEvent, ModelInstalledEvent, ModelRemovedEvent};
pub use model_installer::{InstallResult, ModelInstaller, ModelStatus};
pub use model_registry::{ModelDescriptor, ModelManifest, ModelRegistry};
pub use model_requirements::{can_node_run_model, runnable_models, ModelRequirements};
pub use model_selector::{
    classify_prompt, estimate_tokens, select_best_model, ModelInfo, PromptComplexity,
};
pub use model_signature::{
    full_model_verification, verify_model_hash, verify_model_signature, SignatureVerification,
};
pub use model_storage::ModelStorage;
pub use model_validator::ModelValidator;
pub use model_verifier::ModelVerifier;
pub use node_capabilities::NodeCapabilities as ModelNodeCapabilities;
pub use node_models::{DownloadStrategy, ModelId, NodeModels, PeerModelRegistry};
pub use output_cleaner::clean_output;
pub use output_normalizer::normalize_output;
pub use output_validator::validate_structured_output;
pub use prompt_builder::{Language, PromptBuilder, TemplateType};
pub use signed_metrics::{NodeMetricsPayload, SignedNodeMetrics};
pub use storage_config::StorageConfig;
