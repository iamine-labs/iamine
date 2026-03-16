pub mod model_registry;
pub mod model_storage;
pub mod model_downloader;
pub mod model_verifier;
pub mod inference;
pub mod inference_engine;
pub mod hardware_acceleration;
pub mod node_models;
pub mod node_capabilities;
pub mod signed_metrics;
pub mod model_validator;
pub mod storage_config;
pub mod model_installer;
pub mod distributed_inference; // ← nuevo v0.6

pub use model_registry::{ModelDescriptor, ModelRegistry};
pub use model_storage::ModelStorage;
pub use model_downloader::{ModelDownloader, DownloadProgress};
pub use model_verifier::ModelVerifier;
pub use inference::{InferenceEngine, InferenceRequest, InferenceResult};
pub use inference_engine::{
    InferenceEngine as RealInferenceEngine,
    InferenceRequest as RealInferenceRequest,
    InferenceResult as RealInferenceResult,
};
pub use hardware_acceleration::{HardwareAcceleration, AcceleratorType};
pub use node_models::{NodeModels, ModelId, PeerModelRegistry, DownloadStrategy};
pub use node_capabilities::NodeCapabilities as ModelNodeCapabilities;
pub use signed_metrics::{SignedNodeMetrics, NodeMetricsPayload};
pub use model_validator::ModelValidator;
pub use storage_config::StorageConfig;
pub use model_installer::{ModelInstaller, InstallResult, ModelStatus};
pub use distributed_inference::{InferenceTask, InferenceTaskResult, StreamedToken};
