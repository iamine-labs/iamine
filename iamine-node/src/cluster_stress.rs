use crate::cluster_stress_metrics::ClusterStressMetrics;
use crate::cluster_stress_validation::{
    validate_observations, StressTaskObservation, StressValidationFailure,
};
use crate::env_config::{IAMINE_LOG_FORMAT, IAMINE_LOG_PATH, IAMINE_TASK_LIFECYCLE_PATH};
use crate::scheduler_capability_matching::is_simple_task;
use futures::stream::{FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Output;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::process::Command;
use tokio::time::timeout;

pub(crate) const IAMINE_STRESS_CHILD: &str = "IAMINE_STRESS_CHILD";
pub(crate) const IAMINE_STRESS_TASK_ID: &str = "IAMINE_STRESS_TASK_ID";
const DEFAULT_REQUEST_COUNT: usize = 10;
const DEFAULT_CONCURRENCY: usize = 2;
const DEFAULT_TIMEOUT_SECS: u64 = 75;
const MAX_REQUEST_COUNT: usize = 200;
const MAX_CONCURRENCY: usize = 16;
const MAX_TIMEOUT_SECS: u64 = 900;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClusterStressConfig {
    pub(crate) request_count: usize,
    pub(crate) concurrency: usize,
    pub(crate) task_type: String,
    pub(crate) prefix: String,
    pub(crate) timeout_secs: u64,
    pub(crate) stop_on_first_failure: bool,
    pub(crate) json: bool,
    pub(crate) output_dir: Option<PathBuf>,
}

impl Default for ClusterStressConfig {
    fn default() -> Self {
        Self {
            request_count: DEFAULT_REQUEST_COUNT,
            concurrency: DEFAULT_CONCURRENCY,
            task_type: "reverse_string".to_string(),
            prefix: format!("cluster-stress-{}", now_ms()),
            timeout_secs: DEFAULT_TIMEOUT_SECS,
            stop_on_first_failure: false,
            json: false,
            output_dir: None,
        }
    }
}

impl ClusterStressConfig {
    pub(crate) fn from_args(args: &[String]) -> Result<Self, String> {
        let mut config = Self::default();
        let mut index = 0;

        while index < args.len() {
            match args[index].as_str() {
                "--requests" => {
                    config.request_count = parse_usize_arg(args, index, "--requests")?;
                    index += 2;
                }
                "--concurrency" => {
                    config.concurrency = parse_usize_arg(args, index, "--concurrency")?;
                    index += 2;
                }
                "--task" => {
                    config.task_type = parse_string_arg(args, index, "--task")?;
                    index += 2;
                }
                "--prefix" => {
                    config.prefix = parse_string_arg(args, index, "--prefix")?;
                    index += 2;
                }
                "--timeout-secs" => {
                    config.timeout_secs = parse_u64_arg(args, index, "--timeout-secs")?;
                    index += 2;
                }
                "--output-dir" => {
                    config.output_dir = Some(PathBuf::from(parse_string_arg(
                        args,
                        index,
                        "--output-dir",
                    )?));
                    index += 2;
                }
                "--stop-on-first-failure" => {
                    config.stop_on_first_failure = true;
                    index += 1;
                }
                "--json" => {
                    config.json = true;
                    index += 1;
                }
                unknown => {
                    return Err(format!("Argumento cluster stress desconocido: {}", unknown))
                }
            }
        }

        config.validate()?;
        Ok(config)
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.request_count > MAX_REQUEST_COUNT {
            return Err(format!(
                "--requests excede el limite seguro de {}",
                MAX_REQUEST_COUNT
            ));
        }
        if self.concurrency == 0 || self.concurrency > MAX_CONCURRENCY {
            return Err(format!(
                "--concurrency debe estar entre 1 y {}",
                MAX_CONCURRENCY
            ));
        }
        if self.request_count > 0 && self.concurrency > self.request_count {
            return Err("--concurrency no puede exceder --requests".to_string());
        }
        if self.timeout_secs == 0 || self.timeout_secs > MAX_TIMEOUT_SECS {
            return Err(format!(
                "--timeout-secs debe estar entre 1 y {}",
                MAX_TIMEOUT_SECS
            ));
        }
        if !is_simple_task(&self.task_type) {
            return Err(format!(
                "cluster stress soporta tareas simples de forma segura; task_type no soportado: {}",
                self.task_type
            ));
        }
        if self.prefix.trim().is_empty() {
            return Err("--prefix no puede estar vacio".to_string());
        }

        Ok(())
    }

    fn resolved_output_dir(&self) -> PathBuf {
        self.output_dir.clone().unwrap_or_else(|| {
            std::env::temp_dir().join(format!("iamine-{}", sanitized_component(&self.prefix)))
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClusterStressSummary {
    pub(crate) config: ClusterStressConfig,
    pub(crate) output_dir: PathBuf,
    pub(crate) metrics: ClusterStressMetrics,
    pub(crate) observations: Vec<StressTaskObservation>,
    pub(crate) validation_failures: Vec<StressValidationFailure>,
    pub(crate) passed: bool,
}

impl ClusterStressSummary {
    fn from_observations(
        config: ClusterStressConfig,
        output_dir: PathBuf,
        observations: Vec<StressTaskObservation>,
    ) -> Self {
        let metrics = ClusterStressMetrics::from_observations(config.request_count, &observations);
        let validation_failures = validate_observations(&observations);
        let passed = observations.len() == config.request_count
            && metrics.failed == 0
            && metrics.timed_out == 0
            && validation_failures.is_empty();

        Self {
            config,
            output_dir,
            metrics,
            observations,
            validation_failures,
            passed,
        }
    }
}

pub(crate) async fn run_cluster_stress(
    config: ClusterStressConfig,
) -> Result<ClusterStressSummary, String> {
    config.validate()?;
    let output_dir = config.resolved_output_dir();
    fs::create_dir_all(&output_dir).map_err(|error| {
        format!(
            "No se pudo crear output dir {}: {}",
            output_dir.display(),
            error
        )
    })?;

    let executable = std::env::current_exe()
        .map_err(|error| format!("No se pudo resolver el ejecutable actual: {}", error))?;
    let mut pending = FuturesUnordered::new();
    let mut observations = Vec::with_capacity(config.request_count);
    let mut next_request_index = 0;
    let mut stop_scheduling = false;

    while next_request_index < config.request_count && pending.len() < config.concurrency {
        pending.push(run_single_stress_request(
            executable.clone(),
            output_dir.clone(),
            config.clone(),
            next_request_index,
        ));
        next_request_index += 1;
    }

    while let Some(observation) = pending.next().await {
        if config.stop_on_first_failure && !observation.success {
            stop_scheduling = true;
        }
        observations.push(observation);

        if !stop_scheduling && next_request_index < config.request_count {
            pending.push(run_single_stress_request(
                executable.clone(),
                output_dir.clone(),
                config.clone(),
                next_request_index,
            ));
            next_request_index += 1;
        }
    }

    observations.sort_by(|left, right| left.request_id.cmp(&right.request_id));
    Ok(ClusterStressSummary::from_observations(
        config,
        output_dir,
        observations,
    ))
}

async fn run_single_stress_request(
    executable: PathBuf,
    output_dir: PathBuf,
    config: ClusterStressConfig,
    request_index: usize,
) -> StressTaskObservation {
    let request_id = format!("{}-{:04}", config.prefix, request_index + 1);
    let payload = request_id.clone();
    let expected_output = payload.chars().rev().collect::<String>();
    let trace_path = output_dir.join(format!("request-{:04}.trace.json", request_index + 1));
    let ndjson_path = output_dir.join(format!("request-{:04}.ndjson", request_index + 1));
    let human_path = output_dir.join(format!("request-{:04}.human.log", request_index + 1));
    let started_at = Instant::now();

    let mut command = Command::new(executable);
    command
        .arg("--broadcast")
        .arg(&config.task_type)
        .arg(&payload)
        .env(IAMINE_STRESS_CHILD, "1")
        .env(IAMINE_STRESS_TASK_ID, &request_id)
        .env(IAMINE_TASK_LIFECYCLE_PATH, &trace_path)
        .env(IAMINE_LOG_FORMAT, "ndjson")
        .env(IAMINE_LOG_PATH, &ndjson_path)
        .kill_on_drop(true);

    match timeout(Duration::from_secs(config.timeout_secs), command.output()).await {
        Ok(Ok(output)) => {
            let human_output = combined_output(&output);
            let write_error = fs::write(&human_path, &human_output).err();
            let mut observation = observation_from_artifacts(
                request_id,
                started_at.elapsed(),
                &trace_path,
                &ndjson_path,
            );

            if !output.status.success() {
                append_error(
                    &mut observation,
                    format!("broadcast child exited with status {}", output.status),
                );
            }
            if !human_output.contains(&expected_output) {
                append_error(
                    &mut observation,
                    "broadcast child output did not contain expected reverse_string result",
                );
            }
            if let Some(error) = write_error {
                append_error(
                    &mut observation,
                    format!("human log write failed: {}", error),
                );
            }
            observation.success = observation.error.is_none()
                && observation.final_outcome.as_deref() == Some("success");
            observation
        }
        Ok(Err(error)) => StressTaskObservation {
            request_id,
            latency_ms: elapsed_ms(started_at.elapsed()),
            error: Some(format!("broadcast child spawn failed: {}", error)),
            ..StressTaskObservation::default()
        },
        Err(_) => {
            let message = format!(
                "broadcast child timed out after {} seconds",
                config.timeout_secs
            );
            let _ = fs::write(&human_path, &message);
            StressTaskObservation {
                request_id,
                timed_out: true,
                latency_ms: elapsed_ms(started_at.elapsed()),
                error: Some(message),
                ..StressTaskObservation::default()
            }
        }
    }
}

fn observation_from_artifacts(
    request_id: String,
    elapsed: Duration,
    trace_path: &Path,
    ndjson_path: &Path,
) -> StressTaskObservation {
    let mut observation = StressTaskObservation {
        request_id,
        latency_ms: elapsed_ms(elapsed),
        ..StressTaskObservation::default()
    };

    match read_json(trace_path) {
        Ok(trace) => populate_trace_observation(&mut observation, &trace),
        Err(error) => append_error(&mut observation, error),
    }
    match read_ndjson(ndjson_path) {
        Ok(entries) => populate_log_observation(&mut observation, &entries),
        Err(error) => append_error(&mut observation, error),
    }

    observation
}

fn populate_trace_observation(observation: &mut StressTaskObservation, trace: &Value) {
    let Some(records) = trace.get("records").and_then(Value::as_object) else {
        append_error(observation, "task trace does not contain records");
        return;
    };
    let Some((task_id, record)) = records.iter().next() else {
        append_error(observation, "task trace does not contain task records");
        return;
    };

    observation.task_id = Some(task_id.clone());
    observation.selected_worker = string_field(record, "selected_worker");
    observation.retry_count = u64_field(record, "retry_count") as u32;
    observation.fallback_used = bool_field(record, "fallback_used");
    observation.compatible_candidates_count =
        u64_field(record, "compatible_candidates_count") as usize;
    observation.capability_filter_applied = bool_field(record, "capability_filter_applied");
    observation.required_task_type = string_field(record, "task_type");
    observation.required_model = string_field(record, "model_id");
    observation.final_outcome = string_field(record, "final_outcome");

    let rejected_candidates = string_array_field(record, "rejected_candidates");
    observation.compatible_workers = string_array_field(record, "candidate_workers")
        .into_iter()
        .filter(|candidate| !rejected_candidates.contains(candidate))
        .collect();
    observation.lifecycle_events = trace
        .get("events")
        .and_then(|events| events.get(task_id))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|event| string_field(event, "event"))
        .collect();
}

fn populate_log_observation(observation: &mut StressTaskObservation, entries: &[Value]) {
    for entry in entries {
        let event = entry.get("event").and_then(Value::as_str).unwrap_or("");
        let fields = entry.get("fields").unwrap_or(&Value::Null);

        if event == "result_received" && bool_field(fields, "accepted") {
            observation.accepted_results += 1;
        }
        if event == "broadcast_result_rejected"
            && string_field(fields, "rejection_reason").as_deref() == Some("duplicate_result")
        {
            observation.duplicate_result_rejections += 1;
        }
        if event == "task_completed" && bool_field(fields, "success") {
            observation.execution_count += 1;
        }
    }
}

fn read_json(path: &Path) -> Result<Value, String> {
    let bytes =
        fs::read(path).map_err(|error| format!("No se pudo leer {}: {}", path.display(), error))?;
    serde_json::from_slice(&bytes)
        .map_err(|error| format!("JSON invalido en {}: {}", path.display(), error))
}

fn read_ndjson(path: &Path) -> Result<Vec<Value>, String> {
    let contents = fs::read_to_string(path)
        .map_err(|error| format!("No se pudo leer {}: {}", path.display(), error))?;
    contents
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            serde_json::from_str(line)
                .map_err(|error| format!("NDJSON invalido en {}: {}", path.display(), error))
        })
        .collect()
}

fn parse_usize_arg(args: &[String], index: usize, flag: &str) -> Result<usize, String> {
    parse_string_arg(args, index, flag)?
        .parse()
        .map_err(|_| format!("Valor invalido para {}", flag))
}

fn parse_u64_arg(args: &[String], index: usize, flag: &str) -> Result<u64, String> {
    parse_string_arg(args, index, flag)?
        .parse()
        .map_err(|_| format!("Valor invalido para {}", flag))
}

fn parse_string_arg(args: &[String], index: usize, flag: &str) -> Result<String, String> {
    args.get(index + 1)
        .cloned()
        .ok_or_else(|| format!("Falta valor para {}", flag))
}

fn combined_output(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn append_error(observation: &mut StressTaskObservation, message: impl Into<String>) {
    let message = message.into();
    observation.error = Some(match observation.error.take() {
        Some(current) => format!("{}; {}", current, message),
        None => message,
    });
}

fn elapsed_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn string_field(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(Value::as_str).map(str::to_string)
}

fn string_array_field(value: &Value, field: &str) -> Vec<String> {
    value
        .get(field)
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect()
}

fn bool_field(value: &Value, field: &str) -> bool {
    value.get(field).and_then(Value::as_bool).unwrap_or(false)
}

fn u64_field(value: &Value, field: &str) -> u64 {
    value.get(field).and_then(Value::as_u64).unwrap_or(0)
}

fn sanitized_component(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stress_config_rejects_unbounded_concurrency() {
        let config = ClusterStressConfig {
            request_count: 20,
            concurrency: MAX_CONCURRENCY + 1,
            ..ClusterStressConfig::default()
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn stress_config_rejects_concurrency_above_request_count() {
        let config = ClusterStressConfig {
            request_count: 2,
            concurrency: 3,
            ..ClusterStressConfig::default()
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn stress_config_defaults_are_safe() {
        let config = ClusterStressConfig::default();

        assert!(config.validate().is_ok());
        assert!(config.request_count <= MAX_REQUEST_COUNT);
        assert!(config.concurrency <= MAX_CONCURRENCY);
        assert!(is_simple_task(&config.task_type));
    }

    #[test]
    fn stress_config_allows_zero_request_validation_smoke() {
        let config = ClusterStressConfig {
            request_count: 0,
            ..ClusterStressConfig::default()
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn stress_config_rejects_non_simple_tasks_until_requirement_transport_exists() {
        let config = ClusterStressConfig {
            task_type: "inference".to_string(),
            ..ClusterStressConfig::default()
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn stress_summary_does_not_pass_when_requests_are_not_run() {
        let config = ClusterStressConfig {
            request_count: 2,
            stop_on_first_failure: true,
            ..ClusterStressConfig::default()
        };
        let summary = ClusterStressSummary::from_observations(
            config,
            PathBuf::from("/tmp/stress"),
            Vec::new(),
        );

        assert_eq!(summary.metrics.not_run, 2);
        assert!(!summary.passed);
    }
}
