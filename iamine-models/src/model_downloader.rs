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
    pub phase: DownloadPhase,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DownloadPhase {
    Downloading,
    Verifying,
    Complete,
    Failed,
}

impl ModelDownloader {
    pub fn new(storage: ModelStorage) -> Self {
        Self { storage }
    }

    fn build_http_client(&self) -> Result<reqwest::Client, String> {
        // On some macOS environments, reqwest's system proxy discovery can panic
        // inside system-configuration before any request is made.
        std::panic::catch_unwind(|| {
            reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(600))
                .no_proxy()
                .build()
        })
        .map_err(|_| {
            "Error construyendo cliente HTTP: fallo al desactivar el proxy del sistema".to_string()
        })?
        .map_err(|e| format!("Error construyendo cliente HTTP: {}", e))
    }

    /// Real download with streaming + progress + resume + SHA256 verification
    pub async fn download_model(
        &self,
        model: &ModelDescriptor,
        progress_tx: Option<tokio::sync::mpsc::Sender<DownloadProgress>>,
    ) -> Result<(), String> {
        if self.storage.has_model(&model.id) {
            println!("✅ Modelo {} ya existe localmente", model.id);
            return Ok(());
        }

        let urls = self.build_url_priority(model);

        for (source, url) in &urls {
            println!(
                "⬇️  [{}] Descargando {} ({:.1} GB)...",
                source,
                model.id,
                model.size_gb()
            );
            match self.download_from_url(model, url, &progress_tx).await {
                Ok(_) => {
                    println!("✅ Descarga completada desde {}", source);
                    return Ok(());
                }
                Err(e) => {
                    eprintln!(
                        "⚠️  Falló {}: {} — intentando siguiente fuente...",
                        source, e
                    );
                }
            }
        }

        Err(format!(
            "No se pudo descargar {} desde ninguna fuente",
            model.id
        ))
    }

    fn build_url_priority(&self, model: &ModelDescriptor) -> Vec<(&'static str, String)> {
        let mut urls = Vec::new();
        if !model.download_url.is_empty() {
            urls.push(("HuggingFace", model.download_url.clone()));
        }
        urls
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

        // Resume support
        let resume_from = if tmp_path.exists() {
            let size = fs::metadata(&tmp_path).map(|m| m.len()).unwrap_or(0);
            if size > 0 {
                println!("   ↩️  Resumiendo desde {} MB...", size / 1_048_576);
            }
            size
        } else {
            0
        };

        let client = self.build_http_client()?;

        let mut req = client.get(url).header("User-Agent", "iamine-node/0.6.7");
        if resume_from > 0 {
            req = req.header("Range", format!("bytes={}-", resume_from));
        }

        let response = req
            .send()
            .await
            .map_err(|e| format!("Error de red: {}", e))?;

        let status = response.status();
        if !status.is_success() && status.as_u16() != 206 {
            return Err(format!("HTTP {}", status));
        }

        let total = response
            .content_length()
            .map(|l| l + resume_from)
            .unwrap_or(model.size_bytes);

        println!("   📦 Tamaño total: {:.1} MB", total as f64 / 1_048_576.0);

        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(resume_from > 0)
            .write(true)
            .truncate(resume_from == 0)
            .open(&tmp_path)
            .map_err(|e| e.to_string())?;

        let mut downloaded = resume_from;
        let mut stream = response.bytes_stream();
        let mut last_report = std::time::Instant::now();
        let start_time = std::time::Instant::now();

        while let Some(chunk_result) = stream.next().await {
            let chunk: bytes::Bytes = chunk_result.map_err(|e| format!("Error stream: {}", e))?;
            file.write_all(&chunk).map_err(|e| e.to_string())?;
            downloaded += chunk.len() as u64;

            if last_report.elapsed().as_millis() > 500 {
                let percent = downloaded as f64 / total as f64 * 100.0;
                let mb = downloaded / 1_048_576;
                let total_mb = total / 1_048_576;
                let elapsed = start_time.elapsed().as_secs_f64();
                let speed_mbps = if elapsed > 0.0 {
                    (downloaded - resume_from) as f64 / 1_048_576.0 / elapsed
                } else {
                    0.0
                };

                print!(
                    "\r   📥 {:.1}% ({}/{} MB) {:.1} MB/s",
                    percent, mb, total_mb, speed_mbps
                );
                let _ = std::io::stdout().flush();

                if let Some(tx) = progress_tx {
                    let _ = tx.try_send(DownloadProgress {
                        model_id: model.id.clone(),
                        bytes_downloaded: downloaded,
                        total_bytes: total,
                        percent,
                        phase: DownloadPhase::Downloading,
                    });
                }
                last_report = std::time::Instant::now();
            }
        }

        file.flush().map_err(|e| e.to_string())?;
        drop(file);
        println!();

        let elapsed = start_time.elapsed();
        println!(
            "   ⏱️  Descargado en {:.1}s ({:.1} MB/s)",
            elapsed.as_secs_f64(),
            (downloaded - resume_from) as f64 / 1_048_576.0 / elapsed.as_secs_f64().max(0.001)
        );

        // Notify verification phase
        if let Some(tx) = progress_tx {
            let _ = tx.try_send(DownloadProgress {
                model_id: model.id.clone(),
                bytes_downloaded: downloaded,
                total_bytes: total,
                percent: 100.0,
                phase: DownloadPhase::Verifying,
            });
        }

        // Verify SHA256 if known hash exists
        if model.has_known_hash() {
            ModelVerifier::verify_file(&tmp_path, &model.hash)?;
        } else {
            // Compute and display hash for future reference
            println!("   🔐 Calculando SHA256 (sin hash conocido para verificar)...");
            let computed = ModelVerifier::compute_sha256_file(&tmp_path)?;
            println!("   📋 SHA256: {}", computed);

            // Save hash to a sidecar file for future verification
            let hash_path = model_dir.join(format!("{}.sha256", model.id));
            let _ = fs::write(&hash_path, &computed);
        }

        // Verify file size sanity (at least 1MB for any real model)
        let final_size = fs::metadata(&tmp_path).map(|m| m.len()).unwrap_or(0);
        if final_size < 1_048_576 {
            let _ = fs::remove_file(&tmp_path);
            return Err(format!(
                "Downloaded file too small ({} bytes) — likely not a valid GGUF",
                final_size
            ));
        }

        // Move .tmp → .gguf
        fs::rename(&tmp_path, &output_path).map_err(|e| e.to_string())?;
        println!("   💾 Saved: {}", output_path.display());

        if let Some(tx) = progress_tx {
            let _ = tx.try_send(DownloadProgress {
                model_id: model.id.clone(),
                bytes_downloaded: downloaded,
                total_bytes: total,
                percent: 100.0,
                phase: DownloadPhase::Complete,
            });
        }

        println!(
            "✅ Model {} downloaded and verified ({:.1} MB)",
            model.id,
            final_size as f64 / 1_048_576.0
        );
        Ok(())
    }

    /// Mock for tests (no real network)
    pub async fn download_model_mock(&self, model: &ModelDescriptor) -> Result<(), String> {
        if self.storage.has_model(&model.id) {
            return Ok(());
        }
        let model_dir = self.storage.model_path(&model.id);
        fs::create_dir_all(&model_dir).map_err(|e| e.to_string())?;
        let mock_data = format!(
            "GGUF_MOCK:{}:{}:{}\n",
            model.id, model.version, model.architecture
        )
        .into_bytes();
        // Pad to at least 2048 bytes and start with GGUF magic for test/dev compatibility
        let mut padded = Vec::with_capacity(2048);
        padded.extend_from_slice(b"GGUF"); // GGUF magic
        padded.extend_from_slice(&mock_data);
        if padded.len() < 2048 {
            padded.resize(2048, b'X');
        }
        let output_path = self.storage.gguf_path(&model.id);
        fs::write(&output_path, &padded).map_err(|e| e.to_string())?;
        println!("🧪 [Mock] Modelo {} instalado", model.id);
        Ok(())
    }
}
