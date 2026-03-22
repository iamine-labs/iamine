use crate::prompt_analyzer::SemanticProfile;
use crate::semantic_validator::ValidationResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SemanticLog {
    pub prompt: String,
    pub predicted_profile: SemanticProfile,
    pub validated_profile: SemanticProfile,
    pub confidence_before: f32,
    pub confidence_after: f32,
    pub correction_applied: bool,
    pub conflicts: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictMetric {
    pub conflict: String,
    pub count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SemanticFeedbackMetrics {
    pub total_logs: usize,
    pub corrected_logs: usize,
    pub top_conflicts: Vec<ConflictMetric>,
}

#[derive(Debug, Clone)]
pub struct SemanticFeedbackEngine {
    path: PathBuf,
}

impl Default for SemanticFeedbackEngine {
    fn default() -> Self {
        Self::new(default_log_path())
    }
}

impl SemanticFeedbackEngine {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn append(&self, log: &SemanticLog) -> io::Result<()> {
        if let Some(parent) = self.path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let serialized = serde_json::to_string(log).map_err(io::Error::other)?;
        let line = format!("{}\n", serialized);
        file.write_all(line.as_bytes())?;
        Ok(())
    }

    pub fn append_from_validation(&self, prompt: &str, validation: &ValidationResult) -> io::Result<()> {
        self.append(&SemanticLog {
            prompt: prompt.to_string(),
            predicted_profile: validation.predicted_profile.clone(),
            validated_profile: validation.validated_profile.clone(),
            confidence_before: validation.confidence_before,
            confidence_after: validation.confidence_after,
            correction_applied: validation.correction_applied,
            conflicts: validation.conflicts.clone(),
        })
    }

    pub fn read_all(&self) -> io::Result<Vec<SemanticLog>> {
        if !self.path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut logs = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let log: SemanticLog = serde_json::from_str(&line).map_err(io::Error::other)?;
            logs.push(log);
        }

        Ok(logs)
    }

    pub fn metrics(&self) -> io::Result<SemanticFeedbackMetrics> {
        let logs = self.read_all()?;
        let mut conflict_counts: HashMap<String, usize> = HashMap::new();
        let corrected_logs = logs.iter().filter(|log| log.correction_applied).count();

        for log in &logs {
            for conflict in &log.conflicts {
                *conflict_counts.entry(conflict.clone()).or_insert(0) += 1;
            }
        }

        let mut top_conflicts = conflict_counts
            .into_iter()
            .map(|(conflict, count)| ConflictMetric { conflict, count })
            .collect::<Vec<_>>();
        top_conflicts.sort_by(|left, right| {
            right
                .count
                .cmp(&left.count)
                .then_with(|| left.conflict.cmp(&right.conflict))
        });

        Ok(SemanticFeedbackMetrics {
            total_logs: logs.len(),
            corrected_logs,
            top_conflicts,
        })
    }
}

pub fn default_log_path() -> PathBuf {
    std::env::var_os("IAMINE_SEMANTIC_LOG_PATH")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("semantic_logs.json"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prompt_analyzer::{DeterministicLevel, Domain, OutputStyle};
    use crate::task_analyzer::TaskType;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_log_path() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("iamine-semantic-feedback-{}.json", nanos))
    }

    fn profile(task: TaskType) -> SemanticProfile {
        SemanticProfile {
            primary_task: task,
            secondary_tasks: Vec::new(),
            domain: Some(Domain::General),
            output_style: OutputStyle::Explanatory,
            requires_context: false,
            deterministic_level: DeterministicLevel::Low,
            confidence: 0.5,
        }
    }

    #[test]
    fn test_feedback_logging() {
        let path = temp_log_path();
        let engine = SemanticFeedbackEngine::new(&path);
        let log = SemanticLog {
            prompt: "Que es una derivada?".to_string(),
            predicted_profile: profile(TaskType::General),
            validated_profile: profile(TaskType::Conceptual),
            confidence_before: 0.4,
            confidence_after: 0.7,
            correction_applied: true,
            conflicts: vec!["conceptual-math-vs-symbolic".to_string()],
        };

        engine.append(&log).unwrap();
        let logs = engine.read_all().unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0], log);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn test_privacy_no_user_data() {
        let log = SemanticLog {
            prompt: "Filosoficamente hablando que es dios?".to_string(),
            predicted_profile: profile(TaskType::General),
            validated_profile: profile(TaskType::Conceptual),
            confidence_before: 0.48,
            confidence_after: 0.8,
            correction_applied: true,
            conflicts: vec![],
        };

        let serialized = serde_json::to_string(&log).unwrap();
        assert!(!serialized.contains("user"));
        assert!(!serialized.contains("ip"));
        assert!(!serialized.contains("metadata"));
    }

    #[test]
    fn test_correction_tracking() {
        let path = temp_log_path();
        let engine = SemanticFeedbackEngine::new(&path);
        let log = SemanticLog {
            prompt: "dame 3 ideas de negocio de IA".to_string(),
            predicted_profile: profile(TaskType::Deterministic),
            validated_profile: profile(TaskType::Generative),
            confidence_before: 0.52,
            confidence_after: 0.8,
            correction_applied: true,
            conflicts: vec!["ideation-vs-deterministic".to_string()],
        };

        engine.append(&log).unwrap();
        let metrics = engine.metrics().unwrap();
        assert_eq!(metrics.total_logs, 1);
        assert_eq!(metrics.corrected_logs, 1);
        assert_eq!(metrics.top_conflicts[0].conflict, "ideation-vs-deterministic");
        let _ = std::fs::remove_file(path);
    }
}
