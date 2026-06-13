use crate::cluster_stress::{run_cluster_stress, ClusterStressConfig, ClusterStressSummary};
use std::error::Error;

pub(crate) async fn run_cluster_stress_cli(
    config: &ClusterStressConfig,
) -> Result<(), Box<dyn Error>> {
    let summary = run_cluster_stress(config.clone()).await?;

    if config.json {
        println!("{}", render_cluster_stress_json(&summary)?);
    } else {
        println!("{}", render_cluster_stress_human(&summary));
    }

    if !summary.passed {
        return Err("Cluster stress detecto fallos".into());
    }

    Ok(())
}

pub(crate) fn render_cluster_stress_json(summary: &ClusterStressSummary) -> Result<String, String> {
    serde_json::to_string_pretty(summary)
        .map_err(|error| format!("No se pudo serializar cluster stress: {}", error))
}

pub(crate) fn render_cluster_stress_human(summary: &ClusterStressSummary) -> String {
    let metrics = &summary.metrics;
    format!(
        "IAMINE Cluster Stress\n\
         result: {}\n\
         output_dir: {}\n\
         total_requests: {}\n\
         observed_requests: {}\n\
         not_run: {}\n\
         completed: {}\n\
         failed: {}\n\
         timed_out: {}\n\
         retried: {}\n\
         fallback_used: {}\n\
         duplicate_results: {}\n\
         duplicate_executions: {}\n\
         duplicate_request_ids: {}\n\
         duplicate_task_ids: {}\n\
         incompatible_assignments: {}\n\
         p50_latency_ms: {}\n\
         p95_latency_ms: {}\n\
         p99_latency_ms: {}\n\
         validation_failures: {}",
        if summary.passed { "PASS" } else { "FAIL" },
        summary.output_dir.display(),
        metrics.total_requests,
        metrics.observed_requests,
        metrics.not_run,
        metrics.completed,
        metrics.failed,
        metrics.timed_out,
        metrics.retried,
        metrics.fallback_used,
        metrics.duplicate_results,
        metrics.duplicate_executions,
        metrics.duplicate_request_ids,
        metrics.duplicate_task_ids,
        metrics.incompatible_assignments,
        optional_ms(metrics.p50_latency_ms),
        optional_ms(metrics.p95_latency_ms),
        optional_ms(metrics.p99_latency_ms),
        summary.validation_failures.len(),
    )
}

fn optional_ms(value: Option<u64>) -> String {
    value
        .map(|milliseconds| milliseconds.to_string())
        .unwrap_or_else(|| "-".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster_stress_metrics::ClusterStressMetrics;
    use std::path::PathBuf;

    fn empty_summary() -> ClusterStressSummary {
        ClusterStressSummary {
            config: ClusterStressConfig {
                request_count: 0,
                ..ClusterStressConfig::default()
            },
            output_dir: PathBuf::from("/tmp/iamine-cluster-stress-test"),
            metrics: ClusterStressMetrics::default(),
            observations: Vec::new(),
            validation_failures: Vec::new(),
            passed: true,
        }
    }

    #[test]
    fn cluster_stress_json_summary_shape() -> Result<(), String> {
        let rendered = render_cluster_stress_json(&empty_summary())?;
        let parsed: serde_json::Value =
            serde_json::from_str(&rendered).map_err(|error| error.to_string())?;

        assert!(parsed.get("metrics").is_some());
        assert!(parsed["metrics"].get("p95_latency_ms").is_some());
        assert!(parsed["metrics"].get("p99_latency_ms").is_some());
        assert_eq!(parsed["passed"], true);
        Ok(())
    }

    #[test]
    fn cluster_stress_human_summary_includes_protected_metrics() {
        let rendered = render_cluster_stress_human(&empty_summary());

        assert!(rendered.contains("duplicate_results: 0"));
        assert!(rendered.contains("duplicate_executions: 0"));
        assert!(rendered.contains("duplicate_request_ids: 0"));
        assert!(rendered.contains("duplicate_task_ids: 0"));
        assert!(rendered.contains("incompatible_assignments: 0"));
        assert!(rendered.contains("p95_latency_ms: -"));
        assert!(rendered.contains("p99_latency_ms: -"));
    }
}
