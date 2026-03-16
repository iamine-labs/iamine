use std::fs;
use std::path::PathBuf;

pub struct ModelStorage {
    base_path: PathBuf,
}

impl ModelStorage {
    pub fn new() -> Self {
        let base_path = Self::models_dir();
        fs::create_dir_all(&base_path).expect("No se pudo crear ~/.iamine/models");
        Self { base_path }
    }

    pub fn models_dir() -> PathBuf {
        let home = dirs::home_dir().expect("No se encontró home dir");
        home.join(".iamine").join("models")
    }

    pub fn model_path(&self, model_id: &str) -> PathBuf {
        self.base_path.join(model_id)
    }

    pub fn shard_path(&self, model_id: &str, shard_idx: u32) -> PathBuf {
        self.model_path(model_id).join(format!("shard_{:02}", shard_idx))
    }

    pub fn gguf_path(&self, model_id: &str) -> PathBuf {
        self.model_path(model_id).join(format!("{}.gguf", model_id))
    }

    /// Verifica si el modelo completo está disponible localmente
    pub fn has_model(&self, model_id: &str) -> bool {
        self.gguf_path(model_id).exists()
    }

    /// Verifica si un shard específico existe
    pub fn has_shard(&self, model_id: &str, shard_idx: u32) -> bool {
        self.shard_path(model_id, shard_idx).exists()
    }

    /// Guarda un shard en disco
    pub fn store_shard(&self, model_id: &str, shard_idx: u32, data: &[u8]) -> Result<(), String> {
        let dir = self.model_path(model_id);
        fs::create_dir_all(&dir).map_err(|e| e.to_string())?;

        let path = self.shard_path(model_id, shard_idx);
        fs::write(&path, data).map_err(|e| e.to_string())?;

        println!("💾 Shard {}/{:02} guardado ({} MB)",
            model_id, shard_idx, data.len() / 1_048_576);
        Ok(())
    }

    /// Ensambla shards en archivo GGUF final
    pub fn assemble_model(&self, model_id: &str, total_shards: u32) -> Result<PathBuf, String> {
        println!("🔧 Ensamblando {} shards para {}...", total_shards, model_id);

        let output = self.gguf_path(model_id);
        let mut assembled = Vec::new();

        for i in 0..total_shards {
            let shard_path = self.shard_path(model_id, i);
            if !shard_path.exists() {
                return Err(format!("Falta shard {:02} de {}", i, model_id));
            }
            let data = fs::read(&shard_path).map_err(|e| e.to_string())?;
            assembled.extend_from_slice(&data);
        }

        fs::write(&output, &assembled).map_err(|e| e.to_string())?;
        println!("✅ Modelo {} ensamblado: {:.1} MB", model_id, assembled.len() as f64 / 1_048_576.0);

        Ok(output)
    }

    /// Elimina modelo del disco
    pub fn delete_model(&self, model_id: &str) -> Result<(), String> {
        let path = self.model_path(model_id);
        if path.exists() {
            fs::remove_dir_all(&path).map_err(|e| e.to_string())?;
            println!("🗑️  Modelo {} eliminado", model_id);
        }
        Ok(())
    }

    /// Lista modelos disponibles localmente
    pub fn list_local_models(&self) -> Vec<String> {
        let Ok(entries) = fs::read_dir(&self.base_path) else { return vec![] };
        entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .filter_map(|e| e.file_name().into_string().ok())
            .filter(|name| self.has_model(name))
            .collect()
    }

    /// Tamaño total en disco
    pub fn total_size_bytes(&self) -> u64 {
        Self::dir_size(&self.base_path)
    }

    fn dir_size(path: &PathBuf) -> u64 {
        let Ok(entries) = fs::read_dir(path) else { return 0 };
        entries.filter_map(|e| e.ok()).map(|e| {
            let p = e.path();
            if p.is_dir() { Self::dir_size(&p) }
            else { fs::metadata(&p).map(|m| m.len()).unwrap_or(0) }
        }).sum()
    }
}
