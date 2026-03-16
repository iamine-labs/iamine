use sha2::{Sha256, Digest};
use std::fs;
use std::path::Path;

pub struct ModelVerifier;

impl ModelVerifier {
    /// Verifica SHA256 de un archivo en disco
    pub fn verify_file(path: &Path, expected_hash: &str) -> Result<(), String> {
        if expected_hash.ends_with("_placeholder") {
            // Skip en desarrollo
            return Ok(());
        }

        let data = fs::read(path)
            .map_err(|e| format!("Error leyendo {}: {}", path.display(), e))?;

        let hash = Self::sha256_hex(&data);

        if hash != expected_hash {
            return Err(format!(
                "Hash inválido para {}\n  esperado: {}\n  obtenido: {}",
                path.display(), expected_hash, hash
            ));
        }

        println!("✅ Hash verificado: {}", &hash[..16]);
        Ok(())
    }

    /// Verifica SHA256 de bytes en memoria
    pub fn verify_bytes(data: &[u8], expected_hash: &str) -> Result<(), String> {
        if expected_hash.ends_with("_placeholder") {
            return Ok(());
        }

        let hash = Self::sha256_hex(data);
        if hash != expected_hash {
            return Err(format!("Hash inválido: esperado {}, obtenido {}", expected_hash, hash));
        }
        Ok(())
    }

    pub fn sha256_hex(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        hex::encode(hasher.finalize())
    }

    pub fn sha256_file(path: &Path) -> Result<String, String> {
        let data = fs::read(path).map_err(|e| e.to_string())?;
        Ok(Self::sha256_hex(&data))
    }
}
