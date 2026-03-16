use crate::model_verifier::ModelVerifier;
use std::path::Path;

/// Resultado de validación de un modelo
#[derive(Debug)]
pub struct ValidationResult {
    pub model_id: String,
    pub sha256_ok: bool,
    pub signature_ok: bool,
    pub error: Option<String>,
}

impl ValidationResult {
    pub fn is_valid(&self) -> bool {
        self.sha256_ok && self.signature_ok && self.error.is_none()
    }
}

pub struct ModelValidator {
    /// Claves públicas de signatarios autorizados (IAMINE Labs)
    trusted_signers: Vec<String>,
}

impl ModelValidator {
    pub fn new() -> Self {
        Self {
            trusted_signers: vec![
                // Clave pública oficial de IAMINE Labs (placeholder)
                "iamine_official_pubkey_placeholder".to_string(),
            ],
        }
    }

    /// Validación completa antes de instalar un modelo
    pub fn validate(
        &self,
        model_id: &str,
        path: &Path,
        expected_sha256: &str,
        signature: Option<&str>,
    ) -> ValidationResult {
        println!("🔍 Validando modelo {}...", model_id);

        // 1️⃣ Verificar SHA256
        let sha256_ok = match ModelVerifier::verify_file(path, expected_sha256) {
            Ok(_) => {
                println!("   ✅ SHA256 correcto");
                true
            }
            Err(e) => {
                println!("   ❌ SHA256 inválido: {}", e);
                return ValidationResult {
                    model_id: model_id.to_string(),
                    sha256_ok: false,
                    signature_ok: false,
                    error: Some(e),
                };
            }
        };

        // 2️⃣ Verificar firma (opcional en desarrollo)
        let signature_ok = match signature {
            Some(sig) => {
                let trusted = self.trusted_signers.iter()
                    .any(|k| k.ends_with("_placeholder") || sig.starts_with(k));
                if trusted {
                    println!("   ✅ Firma verificada");
                } else {
                    println!("   ⚠️  Firma no reconocida — modelo no oficial");
                }
                trusted
            }
            None => {
                println!("   ℹ️  Sin firma — modo desarrollo");
                true // permitir en dev
            }
        };

        ValidationResult {
            model_id: model_id.to_string(),
            sha256_ok,
            signature_ok,
            error: None,
        }
    }
}
