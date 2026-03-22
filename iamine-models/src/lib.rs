pub mod model_registry;
pub mod model_storage;
pub mod model_downloader;
pub mod model_verifier;
pub mod model_cache;
pub mod inference;
pub mod inference_engine;
pub mod inference_queue;
pub mod continuation_manager;
pub mod prompt_builder;
pub mod output_cleaner;
pub mod output_normalizer;
pub mod output_validator;
pub mod hardware_acceleration;
pub mod node_models;
pub mod node_capabilities;
pub mod signed_metrics;
pub mod model_validator;
pub mod storage_config;
pub mod model_installer;
pub mod distributed_inference;
pub mod model_requirements;
pub mod model_selector;
pub mod model_signature;
pub mod model_events;
pub mod model_auto_provision;
pub mod huggingface_search; // ← NUEVO

pub use model_registry::{ModelDescriptor, ModelRegistry, ModelManifest};
pub use model_storage::ModelStorage;
pub use model_downloader::{ModelDownloader, DownloadProgress, DownloadPhase};
pub use model_verifier::ModelVerifier;
pub use model_cache::{ModelCache, LoadedModel};
pub use inference::{InferenceEngine, InferenceRequest, InferenceResult};
pub use inference_engine::{
    BackendType,
    InferenceEngine as RealInferenceEngine,
    InferenceContext,
    SamplingConfig,
    InferenceRequest as RealInferenceRequest,
    InferenceResult as RealInferenceResult,
};
pub use inference_queue::{InferenceQueue, InferenceRequest as QueuedInferenceRequest};
pub use continuation_manager::ContinuationManager;
pub use prompt_builder::{Language, PromptBuilder, TemplateType};
pub use output_cleaner::clean_output;
pub use output_normalizer::normalize_output;
pub use output_validator::validate_structured_output;
pub use hardware_acceleration::{HardwareAcceleration, AcceleratorType};
pub use node_models::{NodeModels, ModelId, PeerModelRegistry, DownloadStrategy};
pub use node_capabilities::NodeCapabilities as ModelNodeCapabilities;
pub use signed_metrics::{SignedNodeMetrics, NodeMetricsPayload};
pub use model_validator::ModelValidator;
pub use storage_config::StorageConfig;
pub use model_installer::{ModelInstaller, InstallResult, ModelStatus};
pub use distributed_inference::{
    InferenceTask,
    InferenceTaskResult,
    StreamedToken,
    DirectInferenceRequest,
};
pub use model_requirements::{ModelRequirements, can_node_run_model, runnable_models};
pub use model_selector::{select_best_model, estimate_tokens, classify_prompt, PromptComplexity, ModelInfo};
pub use model_signature::{verify_model_hash, verify_model_signature, full_model_verification, SignatureVerification};
pub use model_events::{ModelInstalledEvent, ModelRemovedEvent, CapabilitiesUpdatedEvent};
pub use model_auto_provision::{AutoProvisionProfile, ModelAutoProvision};
pub use huggingface_search::HuggingFaceSearch;
