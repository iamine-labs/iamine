use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

pub const REQUIRED_SUCCESS_LIFECYCLE_EVENTS: &[&str] = &[
    "task_lifecycle_created",
    "task_lifecycle_assigned",
    "task_lifecycle_scheduler_decision",
    "task_lifecycle_result_received",
    "task_lifecycle_completed",
    "task_lifecycle_finalized",
];

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StressTaskObservation {
    pub request_id: String,
    pub task_id: Option<String>,
    pub success: bool,
    pub timed_out: bool,
    pub retry_count: u32,
    pub fallback_used: bool,
    pub accepted_results: usize,
    pub duplicate_result_rejections: usize,
    pub execution_count: usize,
    pub selected_worker: Option<String>,
    pub compatible_workers: Vec<String>,
    pub capability_filter_applied: bool,
    pub compatible_candidates_count: usize,
    pub required_task_type: Option<String>,
    pub required_model: Option<String>,
    pub selected_worker_executable_models: Vec<String>,
    pub lifecycle_events: Vec<String>,
    pub final_outcome: Option<String>,
    pub latency_ms: u64,
    pub error: Option<String>,
}

impl StressTaskObservation {
    pub fn duplicate_result_count(&self) -> usize {
        self.accepted_results.saturating_sub(1) + self.duplicate_result_rejections
    }

    pub fn duplicate_execution_count(&self) -> usize {
        self.execution_count.saturating_sub(1)
    }

    pub fn has_incompatible_assignment(&self) -> bool {
        let worker_incompatible = self.selected_worker.as_ref().is_some_and(|worker| {
            !self.compatible_workers.is_empty() && !self.compatible_workers.contains(worker)
        });
        let model_incompatible = self.required_model.as_ref().is_some_and(|model| {
            !self.selected_worker_executable_models.is_empty()
                && !self.selected_worker_executable_models.contains(model)
        });

        worker_incompatible || model_incompatible
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "detail", rename_all = "snake_case")]
pub enum StressValidationIssue {
    DuplicateResults,
    DuplicateExecutions,
    DuplicateRequestId(String),
    DuplicateTaskId(String),
    TaskIdCorrelationCollision {
        task_id: String,
        request_ids: Vec<String>,
    },
    IncompatibleAssignment,
    CapabilityFilterMissing,
    MissingFinalOutcome,
    MissingLifecycleEvent(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StressValidationFailure {
    pub request_id: String,
    pub issue: StressValidationIssue,
}

pub fn validate_observation(observation: &StressTaskObservation) -> Vec<StressValidationIssue> {
    let mut issues = Vec::new();

    if observation.duplicate_result_count() > 0 {
        issues.push(StressValidationIssue::DuplicateResults);
    }
    if observation.duplicate_execution_count() > 0 {
        issues.push(StressValidationIssue::DuplicateExecutions);
    }
    if observation.has_incompatible_assignment() {
        issues.push(StressValidationIssue::IncompatibleAssignment);
    }

    if observation.success {
        if !observation.capability_filter_applied {
            issues.push(StressValidationIssue::CapabilityFilterMissing);
        }
        if observation.final_outcome.as_deref() != Some("success") {
            issues.push(StressValidationIssue::MissingFinalOutcome);
        }
        for required_event in REQUIRED_SUCCESS_LIFECYCLE_EVENTS {
            if !observation
                .lifecycle_events
                .iter()
                .any(|event| event == required_event)
            {
                issues.push(StressValidationIssue::MissingLifecycleEvent(
                    (*required_event).to_string(),
                ));
            }
        }
    }

    issues
}

pub fn validate_observations(
    observations: &[StressTaskObservation],
) -> Vec<StressValidationFailure> {
    let mut failures = observations
        .iter()
        .flat_map(|observation| {
            validate_observation(observation)
                .into_iter()
                .map(|issue| StressValidationFailure {
                    request_id: observation.request_id.clone(),
                    issue,
                })
        })
        .collect::<Vec<_>>();
    let mut request_id_counts = HashMap::new();
    let mut task_id_requests = HashMap::<String, Vec<String>>::new();

    for observation in observations {
        *request_id_counts
            .entry(observation.request_id.clone())
            .or_insert(0usize) += 1;
        if let Some(task_id) = &observation.task_id {
            task_id_requests
                .entry(task_id.clone())
                .or_default()
                .push(observation.request_id.clone());
        }
    }

    for (request_id, count) in request_id_counts {
        if count > 1 {
            failures.push(StressValidationFailure {
                request_id: request_id.clone(),
                issue: StressValidationIssue::DuplicateRequestId(request_id),
            });
        }
    }
    for (task_id, request_ids) in task_id_requests {
        if request_ids.len() > 1 {
            for request_id in &request_ids {
                failures.push(StressValidationFailure {
                    request_id: request_id.clone(),
                    issue: StressValidationIssue::DuplicateTaskId(task_id.clone()),
                });
            }
            failures.push(StressValidationFailure {
                request_id: request_ids.join(","),
                issue: StressValidationIssue::TaskIdCorrelationCollision {
                    task_id,
                    request_ids,
                },
            });
        }
    }

    failures
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StressIdentityDuplicateCounts {
    pub duplicate_request_ids: usize,
    pub duplicate_task_ids: usize,
}

pub fn identity_duplicate_counts(
    observations: &[StressTaskObservation],
) -> StressIdentityDuplicateCounts {
    StressIdentityDuplicateCounts {
        duplicate_request_ids: duplicate_count(
            observations
                .iter()
                .map(|observation| observation.request_id.as_str()),
        ),
        duplicate_task_ids: duplicate_count(
            observations
                .iter()
                .filter_map(|observation| observation.task_id.as_deref()),
        ),
    }
}

fn duplicate_count<'a>(values: impl Iterator<Item = &'a str>) -> usize {
    let mut seen = HashSet::new();
    values.filter(|value| !seen.insert(*value)).count()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn complete_observation() -> StressTaskObservation {
        StressTaskObservation {
            request_id: "request-001".to_string(),
            success: true,
            accepted_results: 1,
            execution_count: 1,
            selected_worker: Some("worker-a".to_string()),
            compatible_workers: vec!["worker-a".to_string()],
            capability_filter_applied: true,
            lifecycle_events: REQUIRED_SUCCESS_LIFECYCLE_EVENTS
                .iter()
                .map(|event| (*event).to_string())
                .collect(),
            final_outcome: Some("success".to_string()),
            ..StressTaskObservation::default()
        }
    }

    #[test]
    fn stress_validation_detects_duplicate_results() {
        let mut observation = complete_observation();
        observation.accepted_results = 2;
        assert!(
            validate_observation(&observation).contains(&StressValidationIssue::DuplicateResults)
        );
    }

    #[test]
    fn stress_validation_detects_duplicate_execution() {
        let mut observation = complete_observation();
        observation.execution_count = 2;
        assert!(validate_observation(&observation)
            .contains(&StressValidationIssue::DuplicateExecutions));
    }

    #[test]
    fn stress_validation_detects_missing_final_outcome() {
        let mut observation = complete_observation();
        observation.final_outcome = None;
        assert!(validate_observation(&observation)
            .contains(&StressValidationIssue::MissingFinalOutcome));
    }

    #[test]
    fn stress_validation_accepts_complete_lifecycle() {
        assert!(validate_observation(&complete_observation()).is_empty());
    }

    #[test]
    fn stress_validation_detects_incompatible_assignment() {
        let mut observation = complete_observation();
        observation.compatible_workers = vec!["worker-b".to_string()];
        assert!(validate_observation(&observation)
            .contains(&StressValidationIssue::IncompatibleAssignment));
    }

    #[test]
    fn stress_validation_detects_missing_lifecycle_event() {
        let mut observation = complete_observation();
        observation.lifecycle_events.pop();
        assert!(validate_observation(&observation)
            .iter()
            .any(|issue| { matches!(issue, StressValidationIssue::MissingLifecycleEvent(_)) }));
    }

    #[test]
    fn stress_validation_detects_duplicate_task_ids_across_requests() {
        let mut first = complete_observation();
        first.task_id = Some("task-collision".to_string());
        let mut second = complete_observation();
        second.request_id = "request-002".to_string();
        second.task_id = Some("task-collision".to_string());

        let failures = validate_observations(&[first, second]);

        assert!(failures.iter().any(|failure| {
            matches!(
                &failure.issue,
                StressValidationIssue::TaskIdCorrelationCollision { task_id, request_ids }
                    if task_id == "task-collision" && request_ids.len() == 2
            )
        }));
        assert_eq!(
            identity_duplicate_counts(&[
                StressTaskObservation {
                    request_id: "request-001".to_string(),
                    task_id: Some("task-collision".to_string()),
                    ..StressTaskObservation::default()
                },
                StressTaskObservation {
                    request_id: "request-002".to_string(),
                    task_id: Some("task-collision".to_string()),
                    ..StressTaskObservation::default()
                },
            ])
            .duplicate_task_ids,
            1
        );
    }

    #[test]
    fn stress_validation_detects_duplicate_request_ids() {
        let first = complete_observation();
        let second = complete_observation();

        let failures = validate_observations(&[first, second]);

        assert!(failures.iter().any(|failure| {
            matches!(
                &failure.issue,
                StressValidationIssue::DuplicateRequestId(request_id)
                    if request_id == "request-001"
            )
        }));
    }
}
