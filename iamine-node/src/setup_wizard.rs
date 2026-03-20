use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct DetectedHardware {
    pub cpu_cores: u32,
    pub ram_gb: u32,
    pub gpu_available: bool,
    pub disk_available_gb: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContributionLevel {
    Light,
    Medium,
    High,
    Full,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ModelDownloadMode {
    Automatic,
    ManualGuided,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSetupConfig {
    pub first_run_completed: bool,
    pub cpu_cores: u32,
    pub ram_gb: u32,
    pub gpu_available: bool,
    pub disk_available_gb: u32,
    pub contribution_level: ContributionLevel,
    pub cpu_limit_percent: u8,
    pub ram_limit_gb: u32,
    pub storage_limit_gb: u32,
    pub model_download_mode: ModelDownloadMode,
}

impl NodeSetupConfig {
    pub fn load_or_run(detected: &DetectedHardware) -> Result<Self, String> {
        let path = config_path();
        if path.exists() {
            // replace the inline fs::read/serde_json with helper (keeps behavior identical)
            let cfg = Self::load_from_path(&path)?;
            println!("⚙️  Worker setup cargado desde {}", path.display());
            return Ok(cfg);
        }
        Self::run_interactive(detected)
    }

    pub fn auto_download_enabled(&self) -> bool {
        self.model_download_mode == ModelDownloadMode::Automatic
    }

    pub fn display(&self) {
        println!("🧭 Worker Setup:");
        println!("   Contribution: {:?}", self.contribution_level);
        println!("   CPU limit:    {}%", self.cpu_limit_percent);
        println!("   RAM limit:    {} GB", self.ram_limit_gb);
        println!("   Storage:      {} GB", self.storage_limit_gb);
        println!("   Download:     {:?}", self.model_download_mode);
    }

    fn save(&self) -> Result<(), String> {
        self.save_to_path(&config_path())
    }

    // ✅ Needed by unit tests (and useful for reuse)
    fn save_to_path(&self, path: &Path) -> Result<(), String> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        let data = serde_json::to_vec_pretty(self).map_err(|e| e.to_string())?;
        fs::write(path, data).map_err(|e| e.to_string())
    }

    // ✅ Needed by unit tests (and useful for reuse)
    fn load_from_path(path: &Path) -> Result<Self, String> {
        let data = fs::read(path).map_err(|e| e.to_string())?;
        serde_json::from_slice(&data).map_err(|e| e.to_string())
    }

    fn run_interactive(d: &DetectedHardware) -> Result<Self, String> {
        println!("╔══════════════════════════════════╗");
        println!("║    IaMine — Worker Setup Wizard  ║");
        println!("╚══════════════════════════════════╝\n");
        println!("Detected hardware:");
        println!("  CPU cores:      {}", d.cpu_cores);
        println!("  RAM:            {} GB", d.ram_gb);
        println!("  GPU available:  {}", if d.gpu_available { "yes" } else { "no" });
        println!("  Disk available: {} GB", d.disk_available_gb);

        let level = prompt_level()?;
        let (cpu_limit_percent, ram_limit_gb, suggested_storage_gb) = calculate_limits(d, &level);
        let mode = prompt_mode()?;
        let storage_limit_gb = prompt_storage(suggested_storage_gb, d.disk_available_gb)?;

        let cfg = Self {
            first_run_completed: true,
            cpu_cores: d.cpu_cores,
            ram_gb: d.ram_gb,
            gpu_available: d.gpu_available,
            disk_available_gb: d.disk_available_gb,
            contribution_level: level,
            cpu_limit_percent,
            ram_limit_gb,
            storage_limit_gb,
            model_download_mode: mode,
        };

        cfg.save()?; // keep persistence centralized

        println!("\n✅ Configuración guardada en {}", config_path().display());

        Ok(cfg)
    }
}

fn config_path() -> PathBuf {
    dirs::home_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join(".iamine")
        .join("config")
        .join("node_config.json")
}

fn calculate_limits(d: &DetectedHardware, level: &ContributionLevel) -> (u8, u32, u32) {
    let (cpu, ram_pct, storage_pct) = match level {
        ContributionLevel::Light => (25u8, 25u32, 10u32),
        ContributionLevel::Medium => (50u8, 50u32, 25u32),
        ContributionLevel::High => (75u8, 75u32, 50u32),
        ContributionLevel::Full => (90u8, 90u32, 80u32),
    };
    let ram = ((d.ram_gb as u64 * ram_pct as u64) / 100).max(1) as u32;
    let storage = ((d.disk_available_gb as u64 * storage_pct as u64) / 100).max(1) as u32;
    (cpu, ram.min(d.ram_gb), storage.min(d.disk_available_gb))
}

fn prompt_level() -> Result<ContributionLevel, String> {
    println!("\nContribution level:");
    println!("  1) Light");
    println!("  2) Medium");
    println!("  3) High");
    println!("  4) Full");
    print!("Choose [1-4] (default 2): ");
    let _ = io::stdout().flush();
    let v = read_line()?;
    match v.trim() {
        "" | "2" => Ok(ContributionLevel::Medium),
        "1" => Ok(ContributionLevel::Light),
        "3" => Ok(ContributionLevel::High),
        "4" => Ok(ContributionLevel::Full),
        _ => Err("Contribution level inválido".to_string()),
    }
}

fn prompt_mode() -> Result<ModelDownloadMode, String> {
    println!("\nModel download mode:");
    println!("  1) automatic");
    println!("  2) manual guided");
    print!("Choose [1-2] (default 1): ");
    let _ = io::stdout().flush();
    let v = read_line()?;
    match v.trim() {
        "" | "1" => Ok(ModelDownloadMode::Automatic),
        "2" => Ok(ModelDownloadMode::ManualGuided),
        _ => Err("Download mode inválido".to_string()),
    }
}

fn prompt_storage(default_gb: u32, max_gb: u32) -> Result<u32, String> {
    print!("\nStorage contribution in GB (default {} / max {}): ", default_gb, max_gb);
    let _ = io::stdout().flush();
    let v = read_line()?;
    if v.trim().is_empty() {
        return Ok(default_gb);
    }
    let n = v.trim().parse::<u32>().map_err(|_| "Storage inválido".to_string())?;
    Ok(n.clamp(1, max_gb.max(1)))
}

fn read_line() -> Result<String, String> {
    let mut s = String::new();
    io::stdin().read_line(&mut s).map_err(|e| e.to_string())?;
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs; // ensure fs is in scope for cleanup

    #[test]
    fn test_calculate_limits_medium() {
        let detected = DetectedHardware {
            cpu_cores: 8,
            ram_gb: 16,
            gpu_available: true,
            disk_available_gb: 40,
        };

        let (cpu, ram, storage) = calculate_limits(&detected, &ContributionLevel::Medium);
        assert_eq!(cpu, 50);
        assert_eq!(ram, 8);
        assert_eq!(storage, 10);
    }

    #[test]
    fn test_calculate_limits_full() {
        let detected = DetectedHardware {
            cpu_cores: 8,
            ram_gb: 8,
            gpu_available: false,
            disk_available_gb: 20,
        };

        let (cpu, ram, storage) = calculate_limits(&detected, &ContributionLevel::Full);
        assert_eq!(cpu, 90);
        assert_eq!(ram, 7);
        assert_eq!(storage, 16);
    }

    #[test]
    fn test_setup_config_roundtrip() {
        let base = std::env::temp_dir().join(format!(
            "iamine_setup_wizard_test_{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = base.join("node_config.json");

        let cfg = NodeSetupConfig {
            first_run_completed: true,
            cpu_cores: 8,
            ram_gb: 16,
            gpu_available: true,
            disk_available_gb: 50,
            contribution_level: ContributionLevel::High,
            cpu_limit_percent: 75,
            ram_limit_gb: 12,
            storage_limit_gb: 25,
            model_download_mode: ModelDownloadMode::Automatic,
        };

        cfg.save_to_path(&path).unwrap();
        let loaded = NodeSetupConfig::load_from_path(&path).unwrap();

        assert_eq!(loaded.cpu_limit_percent, 75);
        assert_eq!(loaded.storage_limit_gb, 25);
        assert!(loaded.auto_download_enabled());

        let _ = fs::remove_file(&path);
        let _ = fs::remove_dir_all(base);
    }
}
