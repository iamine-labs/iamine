use std::io::{self, Write};
use iamine_models::{
    ModelRegistry, ModelStorage, ModelNodeCapabilities, 
    ModelRequirements, can_node_run_model, AutoProvisionProfile
};

pub struct ModelSelectorCLI;

impl ModelSelectorCLI {
    /// Mostrar modelos instalados y disponibles según capacidades del nodo
    pub fn show_model_menu(peer_id: &str) -> Result<(), String> {
        let registry = ModelRegistry::new();
        let storage = ModelStorage::new();
        let caps = ModelNodeCapabilities::detect(peer_id);

        let all_models = registry.list();
        let installed: Vec<_> = all_models
            .iter()
            .filter(|m| storage.has_model(&m.id))
            .collect();
        
        let available: Vec<_> = all_models
            .iter()
            .filter(|m| {
                !storage.has_model(&m.id)
                    && Self::is_runnable_on_node(&m.id, &caps)
            })
            .collect();

        println!("╔══════════════════════════════════════╗");
        println!("║   IaMine — Gestión de Modelos        ║");
        println!("╚══════════════════════════════════════╝\n");

        // Mostrar capacidades
        println!("🖥️  Tu Nodo:");
        println!("   CPU Cores:    {}", caps.cpu_cores);
        println!("   RAM:          {} GB", caps.ram_gb);
        println!("   GPU:          {}", caps.gpu_type.as_deref().unwrap_or("❌"));
        println!("   Storage:      {} GB disponibles", caps.storage_available_gb);
        println!();

        // Modelos instalados
        println!("✅ Modelos Instalados ({}):", installed.len());
        if installed.is_empty() {
            println!("   (ninguno)");
        } else {
            for (idx, model) in installed.iter().enumerate() {
                let size_gb = model.size_bytes as f64 / 1_073_741_824.0;
                println!("   {}. {} ({:.1} GB, {} GB RAM)",
                    idx + 1, model.id, size_gb, model.required_ram_gb);
            }
        }
        println!();

        // Modelos disponibles para descargar
        println!("⬜ Modelos Disponibles ({}):", available.len());
        if available.is_empty() {
            println!("   (tu nodo no tiene suficientes recursos para otros modelos)");
        } else {
            for (idx, model) in available.iter().enumerate() {
                let size_gb = model.size_bytes as f64 / 1_073_741_824.0;
                let reliability = Self::reliability_badge(&model.id);
                println!("   {}. {} {} ({:.1} GB, {} GB RAM)",
                    idx + 1, model.id, reliability, size_gb, model.required_ram_gb);
            }
        }
        println!();

        Ok(())
    }

    /// Selector interactivo: cambiar modelo actual
    pub fn select_model_interactive() -> Result<String, String> {
        let storage = ModelStorage::new();
        let all_models = ModelRegistry::new().list();
        let installed: Vec<_> = all_models
            .iter()
            .filter(|m| storage.has_model(&m.id))
            .collect();

        if installed.is_empty() {
            return Err("No hay modelos instalados".to_string());
        }

        println!("\n📋 Selecciona modelo actual:");
        for (idx, model) in installed.iter().enumerate() {
            println!("   [{}] {}", idx + 1, model.id);
        }
        print!("Elige [1-{}]: ", installed.len());
        let _ = io::stdout().flush();

        let choice = Self::read_number(1, installed.len() as u32)?;
        Ok(installed[(choice - 1) as usize].id.clone())
    }

    /// Selector interactivo: descargar modelo
    pub fn select_model_to_download(peer_id: &str) -> Result<String, String> {
        let registry = ModelRegistry::new();
        let storage = ModelStorage::new();
        let caps = ModelNodeCapabilities::detect(peer_id);

        let all_models = registry.list();
        let available: Vec<_> = all_models
            .iter()
            .filter(|m| {
                !storage.has_model(&m.id)
                    && Self::is_runnable_on_node(&m.id, &caps)
            })
            .collect();

        if available.is_empty() {
            return Err("No hay modelos disponibles para descargar".to_string());
        }

        println!("\n📥 Selecciona modelo a descargar:");
        for (idx, model) in available.iter().enumerate() {
            let size_gb = model.size_bytes as f64 / 1_073_741_824.0;
            let reliability = Self::reliability_badge(&model.id);
            println!("   [{}] {} {} ({:.1} GB)", idx + 1, model.id, reliability, size_gb);
        }
        print!("Elige [1-{}]: ", available.len());
        let _ = io::stdout().flush();

        let choice = Self::read_number(1, available.len() as u32)?;
        Ok(available[(choice - 1) as usize].id.clone())
    }

    /// Badge de confiabilidad según descarga/popularidad
    fn reliability_badge(model_id: &str) -> &'static str {
        match model_id {
            "tinyllama-1b" => "⭐⭐⭐⭐⭐",    // Muy confiable, rápido
            "llama3-3b" => "⭐⭐⭐⭐⭐",       // Muy confiable
            "mistral-7b" => "⭐⭐⭐⭐⭐",      // Muy confiable
            "neural-chat-7b" => "⭐⭐⭐⭐",   // Confiable
            "orca-mini-7b" => "⭐⭐⭐⭐",     // Confiable
            "zephyr-7b" => "⭐⭐⭐⭐",        // Confiable
            _ => "⭐⭐⭐",
        }
    }

    /// Verificar si el modelo puede ejecutarse en el nodo
    fn is_runnable_on_node(model_id: &str, caps: &ModelNodeCapabilities) -> bool {
        if let Some(req) = ModelRequirements::for_model(model_id) {
            can_node_run_model(caps, &req)
        } else {
            false
        }
    }

    /// Leer número del usuario con validación
    fn read_number(min: u32, max: u32) -> Result<u32, String> {
        let mut input = String::new();
        io::stdin().read_line(&mut input).map_err(|e| e.to_string())?;
        let num: u32 = input.trim().parse()
            .map_err(|_| "Entrada inválida".to_string())?;
        
        if num >= min && num <= max {
            Ok(num)
        } else {
            Err(format!("Elige entre {} y {}", min, max))
        }
    }
}
