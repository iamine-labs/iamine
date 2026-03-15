use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Rate limiter simple por tipo de mensaje
pub struct RateLimiter {
    counts: HashMap<String, (u64, Instant)>,
    max_per_sec: u64,
}

impl RateLimiter {
    pub fn new(max_per_sec: u64) -> Self {
        Self {
            counts: HashMap::new(),
            max_per_sec,
        }
    }

    /// Retorna true si se permite el mensaje, false si está rate-limited
    pub fn allow(&mut self, msg_type: &str) -> bool {
        let now = Instant::now();
        let entry = self.counts.entry(msg_type.to_string())
            .or_insert((0, now));

        // Reset cada segundo
        if entry.1.elapsed() >= Duration::from_secs(1) {
            *entry = (0, now);
        }

        if entry.0 >= self.max_per_sec {
            return false;
        }

        entry.0 += 1;
        true
    }
}
