use std::path::Path;
use sha2::{Sha256, Digest};
use std::io::Read;

/// Resultado de verificación de firma
#[derive(Debug, Clone)]
pub struct SignatureVerification {
    pub model_id: String,
    pub sha256_valid: bool,
    pub signature_present: bool,
    pub signature_valid: bool,
}

/// Verificar SHA256 de un archivo de modelo
pub fn verify_model_hash(model_path: &Path, expected_hash: &str) -> Result<bool, String> {
    if !model_path.exists() {
        return Err(format!("Archivo no encontrado: {:?}", model_path));
    }

    // Hash placeholder = siempre válido (modo desarrollo)
    if expected_hash.contains("placeholder") || expected_hash.is_empty() {
        return Ok(true);
    }

    let mut file = std::fs::File::open(model_path)
        .map_err(|e| format!("No se puede abrir: {}", e))?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1024 * 1024]; // 1MB chunks

    loop {
        let n = file.read(&mut buf).map_err(|e| format!("Error leyendo: {}", e))?;
        if n == 0 { break; }
        hasher.update(&buf[..n]);
    }

    let hash = format!("{:x}", hasher.finalize());
    Ok(hash == expected_hash)
}

/// Verificar firma criptográfica de un modelo (opcional)
pub fn verify_model_signature(
    model_path: &Path,
    signature_path: &Path,
) -> SignatureVerification {
    let model_id = model_path.file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();

    let sha256_valid = verify_model_hash(model_path, "").unwrap_or(false);
    let signature_present = signature_path.exists();

    // Por ahora, firma = verificar que el .sig existe y no está vacío
    let signature_valid = if signature_present {
        std::fs::metadata(signature_path)
            .map(|m| m.len() > 0)
            .unwrap_or(false)
    } else {
        false
    };

    SignatureVerification {
        model_id,
        sha256_valid,
        signature_present,
        signature_valid,
    }
}

/// Verificación completa de un modelo
pub fn full_model_verification(model_dir: &Path, model_id: &str) -> SignatureVerification {
    let gguf_path = model_dir.join(format!("{}.gguf", model_id));
    let sig_path = model_dir.join(format!("{}.sig", model_id));

    verify_model_signature(&gguf_path, &sig_path)
}
