use crate::node_identity::iamine_dir;
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Wallet {
    pub address: String,
    pub balance: u64,    // en $MIND (lamports)
    pub reputation: f32, // 0.0 - 100.0
    pub tasks_completed: u64,
    pub tasks_failed: u64,
    pub total_uptime_secs: u64,
}

impl Wallet {
    pub fn load_or_create(address: &str) -> Self {
        let path = iamine_dir().join("wallet.json");

        if path.exists() {
            let data = fs::read_to_string(&path).unwrap_or_default();
            if let Ok(w) = serde_json::from_str::<Wallet>(&data) {
                println!("💰 Wallet cargada:");
                println!("   Balance:   {} $MIND", w.balance);
                println!("   Reputación: {:.1}/100", w.reputation);
                println!("   Tareas:    {}", w.tasks_completed);
                return w;
            }
        }

        let w = Wallet {
            address: address.to_string(),
            balance: 0,
            reputation: 100.0,
            tasks_completed: 0,
            tasks_failed: 0,
            total_uptime_secs: 0,
        };
        w.save();
        println!("💰 Wallet nueva creada: {}", address);
        w
    }

    pub fn save(&self) {
        let path = iamine_dir().join("wallet.json");
        let data = serde_json::to_string_pretty(self).unwrap();
        let _ = fs::write(path, data);
    }

    pub fn update_balance(&mut self, amount: u64) {
        self.balance += amount;
        self.save();
    }

    pub fn update_reputation(&mut self, delta: f32) {
        self.reputation = (self.reputation + delta).clamp(0.0, 100.0);
        self.save();
    }

    pub fn increment_tasks_completed(&mut self) {
        self.tasks_completed += 1;
        self.save();
    }

    pub fn increment_tasks_failed(&mut self) {
        self.tasks_failed += 1;
        self.reputation = (self.reputation - 2.0).clamp(0.0, 100.0);
        self.save();
    }

    pub fn success_rate(&self) -> f64 {
        let total = self.tasks_completed + self.tasks_failed;
        if total == 0 {
            return 1.0;
        }
        self.tasks_completed as f64 / total as f64
    }
}
