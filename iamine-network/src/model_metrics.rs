#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ModelMetrics {
    pub success: bool,
    pub latency_ms: u64,
    pub semantic_success: bool,
    pub retry_count: u32,
}

impl ModelMetrics {
    pub fn new(success: bool, latency_ms: u64, semantic_success: bool, retry_count: u32) -> Self {
        Self {
            success,
            latency_ms,
            semantic_success,
            retry_count,
        }
    }

    pub fn accuracy_signal(&self) -> f32 {
        if self.success {
            1.0
        } else {
            0.0
        }
    }

    pub fn latency_signal(&self) -> f32 {
        if self.latency_ms == 0 {
            1.0
        } else {
            (1.0 / (1.0 + (self.latency_ms as f32 / 1_000.0))).max(0.0)
        }
    }

    pub fn reliability_signal(&self) -> f32 {
        if !self.success {
            0.0
        } else {
            1.0 / (1.0 + self.retry_count as f32)
        }
    }

    pub fn semantic_signal(&self) -> f32 {
        if self.semantic_success {
            1.0
        } else {
            0.0
        }
    }
}
