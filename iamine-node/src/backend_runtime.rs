use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum InferenceBackendKind {
    Real,
    Mock,
}

impl InferenceBackendKind {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Real => "real",
            Self::Mock => "mock",
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct CpuFeatureSupport {
    pub(super) avx: bool,
    pub(super) avx2: bool,
    pub(super) fma: bool,
    pub(super) f16c: bool,
    pub(super) sse4_1: bool,
    pub(super) sse4_2: bool,
}

impl CpuFeatureSupport {
    pub(super) fn detect() -> Self {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            return Self {
                avx: std::is_x86_feature_detected!("avx"),
                avx2: std::is_x86_feature_detected!("avx2"),
                fma: std::is_x86_feature_detected!("fma"),
                f16c: std::is_x86_feature_detected!("f16c"),
                sse4_1: std::is_x86_feature_detected!("sse4.1"),
                sse4_2: std::is_x86_feature_detected!("sse4.2"),
            };
        }

        #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
        {
            Self::default_non_x86()
        }
    }

    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    fn default_non_x86() -> Self {
        // Legacy CPU guard applies to x86 GGML builds. Non-x86 keeps real path enabled by default.
        Self {
            avx: true,
            avx2: true,
            fma: true,
            f16c: true,
            sse4_1: true,
            sse4_2: true,
        }
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    fn missing_for_real_backend(self) -> Vec<String> {
        let mut missing = Vec::new();
        if !self.avx {
            missing.push("avx".to_string());
        }
        if !self.avx2 {
            missing.push("avx2".to_string());
        }
        if !self.fma {
            missing.push("fma".to_string());
        }
        if !self.f16c {
            missing.push("f16c".to_string());
        }
        if !self.sse4_1 {
            missing.push("sse4_1".to_string());
        }
        if !self.sse4_2 {
            missing.push("sse4_2".to_string());
        }
        missing
    }

    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    fn missing_for_real_backend(self) -> Vec<String> {
        let _ = self;
        Vec::new()
    }
}

#[derive(Debug, Clone)]
pub(super) struct InferenceBackendState {
    pub(super) configured_backend: InferenceBackendKind,
    pub(super) skip_model_validation: bool,
    pub(super) cpu_features: CpuFeatureSupport,
    pub(super) missing_cpu_features: Vec<String>,
    pub(super) real_backend_available: bool,
}

impl InferenceBackendState {
    pub(super) fn from_args(args: &[String]) -> Self {
        let configured_backend = parse_backend_from_cli(args)
            .or_else(parse_backend_from_env)
            .unwrap_or(InferenceBackendKind::Real);
        let skip_model_validation =
            parse_skip_model_validation_from_cli(args) || parse_skip_model_validation_from_env();
        let cpu_features = CpuFeatureSupport::detect();
        let missing_cpu_features = cpu_features.missing_for_real_backend();
        let real_backend_available = missing_cpu_features.is_empty();

        Self {
            configured_backend,
            skip_model_validation,
            cpu_features,
            missing_cpu_features,
            real_backend_available,
        }
    }

    pub(super) fn mock_enabled(&self) -> bool {
        self.configured_backend == InferenceBackendKind::Mock
    }

    pub(super) fn is_backend_available(&self) -> bool {
        self.mock_enabled() || self.real_backend_available
    }

    pub(super) fn should_skip_startup_model_load(&self) -> bool {
        self.skip_model_validation || self.mock_enabled() || !self.real_backend_available
    }

    pub(super) fn can_advertise_real_inference(&self) -> bool {
        self.configured_backend == InferenceBackendKind::Real && self.real_backend_available
    }

    pub(super) fn should_advertise_inference_capabilities(&self) -> bool {
        self.mock_enabled() || self.can_advertise_real_inference()
    }

    pub(super) fn emit_startup_events(&self) {
        log_observability_event(
            LogLevel::Info,
            "inference_backend_selected",
            "startup",
            None,
            None,
            None,
            {
                let mut fields = Map::new();
                fields.insert(
                    "configured_backend".to_string(),
                    self.configured_backend.as_str().into(),
                );
                fields.insert(
                    "effective_backend".to_string(),
                    if self.mock_enabled() {
                        "mock".into()
                    } else if self.real_backend_available {
                        "real".into()
                    } else {
                        "unavailable".into()
                    },
                );
                fields.insert(
                    "skip_model_validation".to_string(),
                    self.skip_model_validation.into(),
                );
                fields.insert(
                    "real_backend_available".to_string(),
                    self.real_backend_available.into(),
                );
                fields.insert(
                    "mock_backend_enabled".to_string(),
                    self.mock_enabled().into(),
                );
                fields.insert(
                    "cpu_features".to_string(),
                    serde_json::json!({
                        "avx": self.cpu_features.avx,
                        "avx2": self.cpu_features.avx2,
                        "fma": self.cpu_features.fma,
                        "f16c": self.cpu_features.f16c,
                        "sse4_1": self.cpu_features.sse4_1,
                        "sse4_2": self.cpu_features.sse4_2,
                    }),
                );
                fields
            },
        );

        if !self.real_backend_available {
            log_observability_event(
                LogLevel::Error,
                "cpu_backend_unsupported",
                "startup",
                None,
                None,
                Some(MODEL_BACKEND_UNSUPPORTED_CPU_001),
                {
                    let mut fields = Map::new();
                    fields.insert(
                        "missing_cpu_features".to_string(),
                        serde_json::json!(self.missing_cpu_features),
                    );
                    fields.insert(
                        "required_features".to_string(),
                        serde_json::json!(["avx", "avx2", "fma", "f16c", "sse4_1", "sse4_2"]),
                    );
                    fields.insert("recoverable".to_string(), true.into());
                    fields
                },
            );
        }

        if !self.is_backend_available() {
            log_observability_event(
                LogLevel::Error,
                "inference_backend_unavailable",
                "startup",
                None,
                None,
                Some(WORKER_INFERENCE_BACKEND_DISABLED_001),
                {
                    let mut fields = Map::new();
                    fields.insert("configured_backend".to_string(), "real".into());
                    fields.insert(
                        "reason".to_string(),
                        "real_backend_unsupported_cpu_and_mock_not_enabled".into(),
                    );
                    fields.insert("recoverable".to_string(), true.into());
                    fields
                },
            );
        }

        if self.should_skip_startup_model_load() {
            log_observability_event(
                LogLevel::Info,
                "model_load_skipped_startup",
                "startup",
                None,
                None,
                Some(MODEL_LOAD_SKIPPED_STARTUP_001),
                {
                    let mut fields = Map::new();
                    let reason = if self.mock_enabled() {
                        "mock_backend_enabled"
                    } else if !self.real_backend_available {
                        "unsupported_cpu_for_real_backend"
                    } else {
                        "skip_model_validation_requested"
                    };
                    fields.insert("reason".to_string(), reason.into());
                    fields.insert("recoverable".to_string(), true.into());
                    fields
                },
            );
        }
    }
}

pub(super) fn deterministic_mock_output(task_id: &str, model_id: &str, prompt: &str) -> String {
    let prompt_preview = prompt.trim().chars().take(64).collect::<String>();
    format!(
        "[MOCK:{}] task_id={} model={} prompt='{}'",
        current_release_version(),
        task_id,
        model_id,
        prompt_preview
    )
}

pub(super) fn build_mock_inference_result(
    request: &RealInferenceRequest,
    task_id: &str,
) -> RealInferenceResult {
    let output = deterministic_mock_output(task_id, &request.model_id, &request.prompt);
    RealInferenceResult {
        task_id: request.task_id.clone(),
        model_id: request.model_id.clone(),
        output,
        tokens_generated: 32,
        truncated: false,
        continuation_steps: 0,
        execution_ms: 20,
        success: true,
        error: None,
        accelerator_used: "mock".to_string(),
    }
}

pub(super) async fn choose_inference_runtime(
    backend_state: &InferenceBackendState,
    engine: Arc<RealInferenceEngine>,
) -> Result<InferenceRuntime, String> {
    if backend_state.mock_enabled() {
        return Ok(InferenceRuntime::Mock);
    }

    if !backend_state.real_backend_available {
        return Err(format!(
            "Real backend unavailable on this CPU [{}]",
            MODEL_BACKEND_UNSUPPORTED_CPU_001
        ));
    }

    let socket = daemon_socket_path();
    if daemon_is_available(&socket).await {
        println!(
            "[Daemon] Connected to persistent runtime: {}",
            socket.display()
        );
        return Ok(InferenceRuntime::Daemon(socket));
    }

    Ok(InferenceRuntime::Engine(engine))
}

fn parse_backend_from_env() -> Option<InferenceBackendKind> {
    let value = std::env::var("IAMINE_INFERENCE_BACKEND").ok()?;
    parse_backend_value(&value)
}

fn parse_backend_from_cli(args: &[String]) -> Option<InferenceBackendKind> {
    if args.iter().any(|arg| arg == "--disable-real-inference") {
        return Some(InferenceBackendKind::Mock);
    }

    if let Some(eq_value) = args
        .iter()
        .find_map(|arg| arg.strip_prefix("--inference-backend="))
    {
        return parse_backend_value(eq_value);
    }

    args.iter()
        .position(|arg| arg == "--inference-backend")
        .and_then(|index| args.get(index + 1))
        .and_then(|value| parse_backend_value(value))
}

fn parse_backend_value(value: &str) -> Option<InferenceBackendKind> {
    match value.trim().to_ascii_lowercase().as_str() {
        "mock" => Some(InferenceBackendKind::Mock),
        "real" => Some(InferenceBackendKind::Real),
        _ => None,
    }
}

fn parse_skip_model_validation_from_env() -> bool {
    std::env::var("IAMINE_SKIP_MODEL_LOAD_ON_STARTUP")
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn parse_skip_model_validation_from_cli(args: &[String]) -> bool {
    args.iter().any(|arg| arg == "--skip-model-validation")
}
