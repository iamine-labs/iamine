use crate::model_registry::ModelDescriptor;
use crate::model_storage::ModelStorage;
use crate::model_verifier::ModelVerifier;
use std::fs;

pub struct ModelDownloader {
    storage: ModelStorage,
}

impl ModelDownloader {
    pub fn new(storage: ModelStorage) -> Self {
        Self { storage }
    }

    /// Descarga modelo completo (un GGUF = un shard para modelos pequeños)
    pub async fn download_model(&self, model: &ModelDescriptor) -> Result<(), String> {
        if self.storage.has_model(&model.id) {
            println!("✅ Modelo {} ya existe localmente", model.id);
            return Ok(());
        }

        println!("⬇️  Descargando {} ({:.1} GB)...", model.id, model.size_gb());
        println!("   URL: {}", model.download_url);

        let model_dir = self.storage.model_path(&model.id);
        fs::create_dir_all(&model_dir).map_err(|e| e.to_string())?;

        let output_path = self.storage.gguf_path(&model.id);

        // Descarga con reqwest streaming
        let client = reqwest::Client::new();
        let response = client.get(&model.download_url)
            .send()
            .await
            .map_err(|e| format!("Error de red: {}", e))?;

        if !response.status().is_success() {
            return Err(format!("HTTP {}: {}", response.status(), model.download_url));
        }

        let total = response.content_length().unwrap_or(0);
        let bytes = response.bytes().await
            .map_err(|e| format!("Error descargando: {}", e))?;

        println!("   Descargado: {:.1} MB", bytes.len() as f64 / 1_048_576.0);

        fs::write(&output_path, &bytes).map_err(|e| e.to_string())?;

        // Verificar hash
        ModelVerifier::verify_bytes(&bytes, &model.hash)?;

        println!("✅ Modelo {} descargado y verificado", model.id);
        Ok(())
    }

    /// Descarga simulada para desarrollo/testing (sin red)
    pub async fn download_model_mock(&self, model: &ModelDescriptor) -> Result<(), String> {
        if self.storage.has_model(&model.id) {
            println!("✅ Modelo {} ya existe (mock)", model.id);
            return Ok(());
        }

        println!("🧪 [Mock] Simulando descarga de {}...", model.id);
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let model_dir = self.storage.model_path(&model.id);
        fs::create_dir_all(&model_dir).map_err(|e| e.to_string())?;

        // Crear archivo GGUF mock (64 bytes de identificación)
        let mock_data = format!(
            "GGUF_MOCK:{}:{}:{}\n",
            model.id, model.version, model.architecture
        );
        let output_path = self.storage.gguf_path(&model.id);
        fs::write(&output_path, mock_data.as_bytes()).map_err(|e| e.to_string())?;

        println!("✅ [Mock] Modelo {} disponible", model.id);
        Ok(())
    }
}
