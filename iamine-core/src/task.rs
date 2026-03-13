use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Tipos de tareas soportadas
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum TaskType {
    ReverseString,
    ComputeHash,
    ValidateChallenge,
    InferenceRequest, // ML futuro
}

impl TaskType {
    pub fn from_str(s: &str) -> Self {
        match s {
            "reverse_string" => Self::ReverseString,
            "compute_hash" => Self::ComputeHash,
            "validate_challenge" => Self::ValidateChallenge,
            "inference" => Self::InferenceRequest,
            _ => Self::ComputeHash,
        }
    }

    pub fn as_str(&self) -> &str {
        match self {
            Self::ReverseString => "reverse_string",
            Self::ComputeHash => "compute_hash",
            Self::ValidateChallenge => "validate_challenge",
            Self::InferenceRequest => "inference",
        }
    }

    /// Tiempo máximo permitido para esta tarea (ms)
    pub fn timeout_ms(&self) -> u64 {
        match self {
            Self::ReverseString => 1_000,
            Self::ComputeHash => 2_000,
            Self::ValidateChallenge => 3_000,
            Self::InferenceRequest => 30_000,
        }
    }

    /// Cuántos cores necesita esta tarea
    pub fn required_cores(&self) -> usize {
        match self {
            Self::ReverseString | Self::ComputeHash | Self::ValidateChallenge => 1,
            Self::InferenceRequest => 4,
        }
    }
}

/// Estado de una tarea
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TaskStatus {
    Pending,
    Assigned { worker_id: String },
    Executing,
    Completed,
    Failed { reason: String },
    TimedOut,
    Retrying { attempt: u32 },
}

/// Tarea P2P
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub task_type: TaskType,
    pub payload: String,
    pub reward_lamports: u64,
    pub requester_pubkey: Option<String>,
    pub created_at: u64,
    pub timeout_ms: u64,
    pub max_retries: u32,
    pub attempt: u32,
    pub status: TaskStatus,
}

impl Task {
    pub fn new(task_type: TaskType, payload: String) -> Self {
        let timeout_ms = task_type.timeout_ms();
        Self {
            id: Self::generate_id(),
            task_type,
            payload,
            reward_lamports: 0,
            requester_pubkey: None,
            created_at: Self::now(),
            timeout_ms,
            max_retries: 3,
            attempt: 0,
            status: TaskStatus::Pending,
        }
    }

    pub fn with_reward(mut self, lamports: u64) -> Self {
        self.reward_lamports = lamports;
        self
    }

    pub fn with_requester(mut self, pubkey: String) -> Self {
        self.requester_pubkey = Some(pubkey);
        self
    }

    pub fn is_timed_out(&self) -> bool {
        Self::now() - self.created_at > self.timeout_ms
    }

    pub fn can_retry(&self) -> bool {
        self.attempt < self.max_retries
    }

    fn generate_id() -> String {
        let t = Self::now();
        format!("task_{}", t)
    }

    fn now() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}
