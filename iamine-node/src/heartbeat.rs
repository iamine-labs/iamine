use std::time::{Duration, Instant};
use tokio::sync::mpsc;

pub struct HeartbeatService {
    pub start_time: Instant,
}

impl HeartbeatService {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
        }
    }

    pub fn uptime_secs(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    /// Inicia el loop de heartbeat — envía señal cada 5s al canal
    pub fn start(interval_secs: u64) -> mpsc::Receiver<()> {
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
            loop {
                interval.tick().await;
                if tx.send(()).await.is_err() {
                    break;
                }
            }
        });
        rx
    }
}
