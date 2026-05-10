use crate::model_verifier::ModelVerifier;
use std::path::Path;

pub struct ModelValidator;

#[derive(Debug, Clone)]
pub struct ValidationResult {
    pub sha256_ok: bool,
    pub signature_ok: bool,
    pub error: Option<String>,
}

impl ValidationResult {
    pub fn is_valid(&self) -> bool {
        self.sha256_ok && self.signature_ok
    }
}

impl ModelValidator {
    pub fn new() -> Self {
        Self
    }

    pub fn validate(
        &self,
        _model_id: &str,
        path: &Path,
        expected_hash: &str,
        _signature: Option<&str>,
    ) -> ValidationResult {
        if !path.exists() {
            return ValidationResult {
                sha256_ok: false,
                signature_ok: false,
                error: Some(format!("File not found: {}", path.display())),
            };
        }

        // SHA256 verification
        let sha256_ok = match ModelVerifier::verify_file(path, expected_hash) {
            Ok(()) => true,
            Err(e) => {
                // If hash was placeholder/empty, verification was skipped → ok
                if expected_hash.is_empty() || expected_hash.ends_with("_placeholder") {
                    true
                } else {
                    return ValidationResult {
                        sha256_ok: false,
                        signature_ok: false,
                        error: Some(e),
                    };
                }
            }
        };

        // Signature verification (not implemented yet → always ok)
        let signature_ok = true;

        ValidationResult {
            sha256_ok,
            signature_ok,
            error: None,
        }
    }
}
