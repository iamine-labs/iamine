use std::io::{self, Write};
use iamine_models::{
    ModelRegistry, ModelStorage, ModelNodeCapabilities,
    ModelRequirements, can_node_run_model,
};

pub struct ModelSelectorCLI;

impl ModelSelectorCLI {
    /// Mostrar modelos instalados y disponibles según capacidades del nodo
    pub fn show_model_menu(peer_id: &str) -> Result<(), String> {
        let registry = ModelRegistry::new();
        let storage = ModelStorage::new();
        let caps = ModelNodeCapabilities::detect(peer_id);

        let all_models = registry.list();
        let installed: Vec<_> = all_models.iter().filter(|m| storage.has_model(&m.id)).collect();
        let available: Vec<_> = all_models.iter()
            .filter(|m| !storage.has_model(&m.id) && Self::is_runnable(&m.id, &caps))
            .collect();

        println!("╔══════════════════════════════════════╗");
        println!("║   IaMine — Gestión de Modelos        ║");
        println!("╚══════════════════════════════════════╝\n");
        println!("🖥️  Tu Nodo:");
        println!("   CPU Cores:    {}", caps.cpu_cores);
        println!("   RAM:          {} GB", caps.ram_gb);
        println!("   GPU:          {}", caps.gpu_type.as_deref().unwrap_or("❌"));
        println!("   Storage:      {} GB disponibles\n", caps.storage_available_gb);

        println!("✅ Modelos Instalados ({}):", installed.len());
        if installed.is_empty() {
            println!("   (ninguno)");
        } else {
            for (i, m) in installed.iter().enumerate() {
                println!("   {}. {} ({:.1} GB, {} GB RAM)",
                    i + 1, m.id, m.size_bytes as f64 / 1_073_741_824.0, m.required_ram_gb);
            }
        }

        println!("\n⬜ Modelos Disponibles ({}):", available.len());
        if available.is_empty() {
            println!("   (sin recursos suficientes para otros modelos)");
        } else {
            for (i, m) in available.iter().enumerate() {
                let badge = Self::badge(&m.id);
                println!("   {}. {} {} ({:.1} GB, {} GB RAM)",
                    i + 1, m.id, badge, m.size_bytes as f64 / 1_073_741_824.0, m.required_ram_gb);
            }
        }
        Ok(())
    }

    /// Selector interactivo: descargar modelo
    pub fn select_model_to_download(peer_id: &str) -> Result<String, String> {
        let registry = ModelRegistry::new();
        let storage = ModelStorage::new();
        let caps = ModelNodeCapabilities::detect(peer_id);

        let all_models = registry.list();
        let available: Vec<_> = all_models.iter()
            .filter(|m| !storage.has_model(&m.id) && Self::is_runnable(&m.id, &caps))
            .collect();

        if available.is_empty() {
            return Err("No hay modelos disponibles para descargar".into());
        }

        println!("\n📥 Selecciona modelo a descargar:");
        for (i, m) in available.iter().enumerate() {
            let badge = Self::badge(&m.id);
            println!("   [{}] {} {} ({:.1} GB)",
                i + 1, m.id, badge, m.size_bytes as f64 / 1_073_741_824.0);
        }
        print!("Elige [1-{}]: ", available.len());
        let _ = io::stdout().flush();

        let n = Self::read_num(1, available.len() as u32)?;
        let chosen: String = available[(n - 1) as usize].id.clone();
        Ok(chosen)
    }

    /// Badge de confiabilidad según descarga/popularidad
    fn badge(id: &str) -> &'static str {
        match id {
            "tinyllama-1b" | "llama3-3b" | "mistral-7b" => "⭐⭐⭐⭐⭐",
            "neural-chat-7b" | "orca-mini-7b" | "zephyr-7b" => "⭐⭐⭐⭐",
            _ => "⭐⭐⭐",
        }
    }

    /// Verificar si el modelo puede ejecutarse en el nodo
    fn is_runnable(id: &str, caps: &ModelNodeCapabilities) -> bool {
        ModelRequirements::for_model(id)
            .map(|r| can_node_run_model(caps, &r))
            .unwrap_or(false)
    }

    /// Leer número del usuario con validación
    fn read_num(min: u32, max: u32) -> Result<u32, String> {
        let mut buf = String::new();
        io::stdin().read_line(&mut buf).map_err(|e| e.to_string())?;
        let n: u32 = buf.trim().parse().map_err(|_| "Entrada inválida".to_string())?;
        if n >= min && n <= max { Ok(n) } else { Err(format!("Elige entre {} y {}", min, max)) }
    }
}
