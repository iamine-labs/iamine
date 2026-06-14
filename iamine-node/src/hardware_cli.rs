use iamine_hardware::{
    default_profile_path, inspect_hardware, HardwareCollectionMode, HardwareProfileStore,
    HardwareProfilerConfig, NodeHardwareProfile,
};
use std::error::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HardwareCliCommand {
    Inspect {
        json: bool,
        dynamic: bool,
    },
    Show {
        json: bool,
    },
    Refresh {
        json: bool,
        dynamic: bool,
        yes: bool,
    },
}

impl HardwareCliCommand {
    pub(crate) fn from_args(args: &[String]) -> Result<Self, String> {
        match args.first().map(String::as_str) {
            Some("inspect") => Ok(Self::Inspect {
                json: args.iter().any(|arg| arg == "--json"),
                dynamic: args.iter().any(|arg| arg == "--dynamic"),
            }),
            Some("show") => Ok(Self::Show {
                json: args.iter().any(|arg| arg == "--json"),
            }),
            Some("refresh") => Ok(Self::Refresh {
                json: args.iter().any(|arg| arg == "--json"),
                dynamic: args.iter().any(|arg| arg == "--dynamic"),
                yes: args.iter().any(|arg| arg == "--yes"),
            }),
            Some(other) => Err(format!(
                "Uso: iamine-node hardware [inspect [--json] [--dynamic]|show [--json]|refresh [--yes] [--json] [--dynamic]]; comando desconocido: {}",
                other
            )),
            None => Err(
                "Uso: iamine-node hardware [inspect [--json] [--dynamic]|show [--json]|refresh [--yes] [--json] [--dynamic]]"
                    .to_string(),
            ),
        }
    }
}

pub(crate) fn run_hardware_cli(command: &HardwareCliCommand) -> Result<(), Box<dyn Error>> {
    match command {
        HardwareCliCommand::Inspect { json, dynamic } => {
            let profile = inspect_hardware(config_for_dynamic(*dynamic))?;
            render_profile(&profile, *json)?;
        }
        HardwareCliCommand::Show { json } => {
            let store = HardwareProfileStore::default();
            let profile = store.load()?;
            render_profile(&profile, *json)?;
        }
        HardwareCliCommand::Refresh { json, dynamic, yes } => {
            let store = HardwareProfileStore::default();
            let profile = store.refresh(config_for_dynamic(*dynamic))?;
            if !*json {
                println!(
                    "hardware_profile_saved: {}",
                    display_profile_path_without_home()
                );
                if !*yes {
                    println!("refresh_confirmation: --yes not provided; noninteractive refresh completed");
                }
            }
            render_profile(&profile, *json)?;
        }
    }
    Ok(())
}

pub(crate) fn render_profile_json(profile: &NodeHardwareProfile) -> Result<String, String> {
    serde_json::to_string_pretty(profile)
        .map_err(|error| format!("No se pudo serializar hardware profile: {}", error))
}

pub(crate) fn render_profile_human(profile: &NodeHardwareProfile) -> String {
    let static_profile = &profile.static_profile;
    let cpu = &static_profile.cpu;
    let memory = &static_profile.memory;
    let accelerator_names = static_profile
        .accelerators
        .iter()
        .map(|accelerator| format!("{:?}:{}", accelerator.kind, accelerator.name))
        .collect::<Vec<_>>()
        .join(", ");

    let dynamic = profile
        .dynamic_profile
        .as_ref()
        .map(|dynamic| {
            format!(
                "\ndynamic:\n  duration_ms: {}\n  cpu_score_ops_per_sec: {}\n  storage_write_mb_per_sec: {}",
                dynamic.duration_ms,
                dynamic.cpu.score_ops_per_sec,
                dynamic
                    .storage
                    .write_mb_per_sec
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string())
            )
        })
        .unwrap_or_default();

    format!(
        "IAMINE Hardware Profile\n\
         schema_version: {}\n\
         profile_id: {}\n\
         collection_mode: {:?}\n\
         generated_at_unix_ms: {}\n\
         os: {}/{}\n\
         cpu_logical_cores: {}\n\
         cpu_physical_cores: {}\n\
         cpu_recommended_threads: {}\n\
         cpu_features: {}\n\
         memory_total_gb: {}\n\
         memory_unified: {}\n\
         accelerators: {}\n\
         effective_worker_slots: {}\n\
         effective_accelerator: {:?}\n\
         warnings: {}{}",
        profile.schema_version,
        profile.profile_id,
        profile.collection_mode,
        profile.generated_at_unix_ms,
        static_profile.os_family,
        static_profile.os_arch,
        cpu.logical_cores,
        cpu.physical_cores
            .map(|value| value.to_string())
            .unwrap_or_else(|| "-".to_string()),
        cpu.recommended_threads,
        if cpu.features.features.is_empty() {
            "-".to_string()
        } else {
            cpu.features.features.join(",")
        },
        memory.total_gb,
        memory.unified_memory,
        if accelerator_names.is_empty() {
            "-".to_string()
        } else {
            accelerator_names
        },
        static_profile.effective.effective_worker_slots,
        static_profile.effective.effective_accelerator,
        profile.warnings.len(),
        dynamic
    )
}

fn render_profile(profile: &NodeHardwareProfile, json: bool) -> Result<(), Box<dyn Error>> {
    if json {
        println!("{}", render_profile_json(profile)?);
    } else {
        println!("{}", render_profile_human(profile));
    }
    Ok(())
}

fn config_for_dynamic(dynamic: bool) -> HardwareProfilerConfig {
    HardwareProfilerConfig {
        mode: if dynamic {
            HardwareCollectionMode::QuickDynamic
        } else {
            HardwareCollectionMode::StaticOnly
        },
        ..HardwareProfilerConfig::default()
    }
}

fn display_profile_path_without_home() -> String {
    let path = default_profile_path();
    path.strip_prefix(dirs::home_dir().unwrap_or_default())
        .map(|relative| format!("~{}", relative.display()))
        .unwrap_or_else(|_| path.display().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use iamine_hardware::{
        build_node_hardware_profile, AcceleratorKind, AcceleratorStaticProfile, CpuFeatureProfile,
        CpuStaticProfile, DetectionConfidence, HardwareProfileParts, MemoryStaticProfile,
        StorageStaticProfile,
    };

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    fn test_profile() -> NodeHardwareProfile {
        build_node_hardware_profile(HardwareProfileParts {
            mode: HardwareCollectionMode::StaticOnly,
            cpu: CpuStaticProfile {
                architecture: "x86_64".to_string(),
                vendor: None,
                brand: Some("Test CPU".to_string()),
                physical_cores: Some(4),
                logical_cores: 8,
                recommended_threads: 4,
                features: CpuFeatureProfile {
                    avx2: true,
                    avx512f: false,
                    fma: true,
                    neon: false,
                    features: vec!["AVX2".to_string(), "FMA".to_string()],
                },
                confidence: DetectionConfidence::High,
            },
            memory: MemoryStaticProfile {
                total_bytes: 16 * 1_073_741_824,
                available_bytes: Some(8 * 1_073_741_824),
                total_gb: 16,
                unified_memory: false,
                confidence: DetectionConfidence::High,
            },
            accelerators: vec![AcceleratorStaticProfile {
                kind: AcceleratorKind::Cpu,
                name: "CPU".to_string(),
                vendor: None,
                memory_bytes: None,
                unified_memory: false,
                confidence: DetectionConfidence::High,
            }],
            storage: StorageStaticProfile {
                available_bytes: Some(1),
                confidence: DetectionConfidence::Medium,
            },
            dynamic_profile: None,
            warnings: Vec::new(),
            generated_at_unix_ms: 42,
        })
    }

    #[test]
    fn hardware_cli_parses_inspect_json_dynamic() -> Result<(), String> {
        let command = HardwareCliCommand::from_args(&args(&["inspect", "--json", "--dynamic"]))?;

        assert!(matches!(
            command,
            HardwareCliCommand::Inspect {
                json: true,
                dynamic: true
            }
        ));
        Ok(())
    }

    #[test]
    fn hardware_cli_parses_refresh_yes() -> Result<(), String> {
        let command = HardwareCliCommand::from_args(&args(&["refresh", "--yes"]))?;

        assert!(matches!(
            command,
            HardwareCliCommand::Refresh {
                json: false,
                dynamic: false,
                yes: true
            }
        ));
        Ok(())
    }

    #[test]
    fn hardware_json_render_contains_schema_version() -> Result<(), String> {
        let json = render_profile_json(&test_profile())?;
        let parsed: serde_json::Value =
            serde_json::from_str(&json).map_err(|error| error.to_string())?;

        assert_eq!(parsed["schema_version"], "1.0.0");
        assert!(parsed.get("static_profile").is_some());
        Ok(())
    }

    #[test]
    fn hardware_human_render_distinguishes_effective_fields() {
        let rendered = render_profile_human(&test_profile());

        assert!(rendered.contains("effective_worker_slots: 8"));
        assert!(rendered.contains("cpu_features: AVX2,FMA"));
    }
}
