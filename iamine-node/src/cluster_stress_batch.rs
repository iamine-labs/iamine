use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ClusterStressBatchFile {
    requests: Vec<ClusterStressBatchRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ClusterStressBatchRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) required_model_id: Option<String>,
}

pub(crate) fn parse_batch_file(path: &str) -> Result<Vec<ClusterStressBatchRequest>, String> {
    let contents = fs::read_to_string(path)
        .map_err(|error| format!("No se pudo leer --batch-file: {}", error))?;
    let batch: ClusterStressBatchFile = serde_json::from_str(&contents)
        .map_err(|error| format!("JSON invalido en --batch-file: {}", error))?;
    if batch.requests.is_empty() {
        return Err("--batch-file debe contener al menos una request".to_string());
    }
    Ok(batch.requests)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEMP_BATCH_COUNTER: AtomicUsize = AtomicUsize::new(0);

    #[test]
    fn batch_file_parses_required_models() -> Result<(), String> {
        let path = write_temp_batch_file(
            r#"{"requests":[{"required_model_id":"tinyllama-1b"},{"required_model_id":"llama3-3b"}]}"#,
        )?;
        let batch = parse_batch_file(&path.display().to_string())?;
        let _ = fs::remove_file(path);

        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].required_model_id.as_deref(), Some("tinyllama-1b"));
        assert_eq!(batch[1].required_model_id.as_deref(), Some("llama3-3b"));
        Ok(())
    }

    #[test]
    fn batch_file_rejects_empty_requests() -> Result<(), String> {
        let path = write_temp_batch_file(r#"{"requests":[]}"#)?;
        let result = parse_batch_file(&path.display().to_string());
        let _ = fs::remove_file(path);

        assert!(result.is_err());
        Ok(())
    }

    fn write_temp_batch_file(contents: &str) -> Result<PathBuf, String> {
        let counter = TEMP_BATCH_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("iamine-stress-batch-{}-{}.json", now_ms(), counter));
        fs::write(&path, contents)
            .map_err(|error| format!("temp batch file write failed: {error}"))?;
        Ok(path)
    }

    fn now_ms() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or(0)
    }
}
