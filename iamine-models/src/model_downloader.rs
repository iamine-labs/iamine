use crate::model_registry::ModelDescriptor;
use crate::model_storage::ModelStorage;
use crate::model_verifier::ModelVerifier;
use futures::StreamExt;
use std::fs;
use std::io::Write;

pub struct ModelDownloader {
    pub storage: ModelStorage,
}

#[derive(Debug, Clone)]
pub struct DownloadProgress {
    pub model_id: String,
    pub bytes_downloaded: u64,
    pub total_bytes: u64,
    pub percent: f64,
}

impl ModelDownloader {
    pub fn new(storage: ModelStorage) -> Self {
        Self { storage }
    }

    /// Descarga real con streaming + progress + resume
    pub async fn download_model(
        &self,
        model: &ModelDescriptor,
        progress_tx: Option<tokio::sync::mpsc::Sender<DownloadProgress>>,
    ) -> Result<(), String> {
        if self.storage.has_model(&model.id) {
            println!("✅ Modelo {} ya existe localmente", model.id);
            return Ok(());
        }

        // Prioridad: IAMINE server → peers → HuggingFace
        let urls = self.build_url_priority(model);

        for (source, url) in &urls {
            println!("⬇️  [{}] Descargando {} ({:.1} GB)...", source, model.id, model.size_gb());
            match self.download_from_url(model, url, &progress_tx).await {
                Ok(_) => {
                    println!("✅ Descarga completada desde {}", source);
                    return Ok(());
                }
                Err(e) => {
                    eprintln!("⚠️  Falló {}: {} — intentando siguiente fuente...", source, e);
                }
            }
        }

        Err(format!("No se pudo descargar {} desde ninguna fuente", model.id))
    }

    fn build_url_priority<'a>(&self, model: &'a ModelDescriptor) -> Vec<(&'static str, String)> {
        vec![
            ("IAMINE-server", format!("https://models.iamine.network/{}/{}.gguf", model.id, model.id)),
            ("HuggingFace", model.download_url.clone()),
        ]
    }

    async fn download_from_url(
        &self,
        model: &ModelDescriptor,
        url: &str,
        progress_tx: &Option<tokio::sync::mpsc::Sender<DownloadProgress>>,
    ) -> Result<(), String> {
        let model_dir = self.storage.model_path(&model.id);
        fs::create_dir_all(&model_dir).map_err(|e| e.to_string())?;

        let output_path = self.storage.gguf_path(&model.id);
        let tmp_path = model_dir.join(format!("{}.tmp", model.id));

        let resume_from = if tmp_path.exists() {
            let size = fs::metadata(&tmp_path).map(|m| m.len()).unwrap_or(0);
            if size > 0 {
                println!("   ↩️  Resumiendo desde {} MB...", size / 1_048_576);
            }
            size
        } else {
            0
        };

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(300))
            .build()
            .map_err(|e| e.to_string())?;

        let mut req = client.get(url);
        if resume_from > 0 {
            req = req.header("Range", format!("bytes={}-", resume_from));
        }

        let response = req.send().await
            .map_err(|e| format!("Error de red: {}", e))?;

        if !response.status().is_success() && response.status().as_u16() != 206 {
            return Err(format!("HTTP {}", response.status()));
        }

        let total = response.content_length()
            .map(|l| l + resume_from)
            .unwrap_or(model.size_bytes);

        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(resume_from > 0)
            .write(true)
            .open(&tmp_path)
            .map_err(|e| e.to_string())?;

        let mut downloaded = resume_from;
        let mut stream = response.bytes_stream();
        let mut last_report = std::time::Instant::now();

        while let Some(chunk_result) = stream.next().await {
            let chunk: bytes::Bytes = chunk_result.map_err(|e| format!("Error stream: {}", e))?;
            file.write_all(&chunk).map_err(|e| e.to_string())?;
            downloaded += chunk.len() as u64;

            // Reportar progreso cada 500ms
            if last_report.elapsed().as_millis() > 500 {
                let percent = downloaded as f64 / total as f64 * 100.0;
                let mb = downloaded / 1_048_576;
                let total_mb = total / 1_048_576;
                print!("\r   📥 {:.1}% ({}/{} MB)", percent, mb, total_mb);
                let _ = std::io::stdout().flush();

                if let Some(tx) = progress_tx {
                    let _ = tx.try_send(DownloadProgress {
                        model_id: model.id.clone(),
                        bytes_downloaded: downloaded,
                        total_bytes: total,
                        percent,
                    });
                }
                last_report = std::time::Instant::now();
            }
        }

        println!(); // nueva línea tras progress

        // Mover .tmp → .gguf
        fs::rename(&tmp_path, &output_path).map_err(|e| e.to_string())?;
        println!("💾 Guardado en: {}", output_path.display());

        // Verificar hash
        let bytes = fs::read(&output_path).map_err(|e| e.to_string())?;
        ModelVerifier::verify_bytes(&bytes, &model.hash)?;

        println!("✅ Modelo {} descargado y verificado", model.id);
        Ok(())
    }

    /// Mock para tests (no hace red real)
    pub async fn download_model_mock(&self, model: &ModelDescriptor) -> Result<(), String> {
        if self.storage.has_model(&model.id) {
            return Ok(());
        }
        let model_dir = self.storage.model_path(&model.id);
        fs::create_dir_all(&model_dir).map_err(|e| e.to_string())?;
        let mock_data = format!("GGUF_MOCK:{}:{}:{}\n", model.id, model.version, model.architecture);
        let output_path = self.storage.gguf_path(&model.id);
        fs::write(&output_path, mock_data.as_bytes()).map_err(|e| e.to_string())?;
        println!("🧪 [Mock] Modelo {} instalado", model.id);
        Ok(())
    }
}
