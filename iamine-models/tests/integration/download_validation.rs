// ─── Tests v0.6.7: Real Model Downloads ──────────────────────────────────

#[test]
fn test_sha256_verification_real() {
    use iamine_models::ModelVerifier;
    use std::io::Write;

    let tmp = tempfile::NamedTempFile::new().unwrap();
    tmp.as_file().write_all(b"hello world").unwrap();

    // Known SHA256 of "hello world"
    let expected = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9";
    let result = ModelVerifier::verify_file(tmp.path(), expected);
    assert!(
        result.is_ok(),
        "Real SHA256 verification should pass: {:?}",
        result
    );
}

#[test]
fn test_sha256_verification_mismatch() {
    use iamine_models::ModelVerifier;
    use std::io::Write;

    let tmp = tempfile::NamedTempFile::new().unwrap();
    tmp.as_file().write_all(b"hello world").unwrap();

    let wrong_hash = "0000000000000000000000000000000000000000000000000000000000000000";
    let result = ModelVerifier::verify_file(tmp.path(), wrong_hash);
    assert!(result.is_err(), "Should fail on hash mismatch");
}

#[test]
fn test_sha256_skip_empty_hash() {
    use iamine_models::ModelVerifier;
    use std::io::Write;

    let tmp = tempfile::NamedTempFile::new().unwrap();
    tmp.as_file().write_all(b"data").unwrap();

    // Empty hash → skip verification
    assert!(ModelVerifier::verify_file(tmp.path(), "").is_ok());
    // Placeholder hash → skip
    assert!(ModelVerifier::verify_file(tmp.path(), "tinyllama_hash_placeholder").is_ok());
}

#[test]
fn test_compute_sha256_file() {
    use iamine_models::ModelVerifier;
    use std::io::Write;

    let tmp = tempfile::NamedTempFile::new().unwrap();
    tmp.as_file().write_all(b"test data for hashing").unwrap();

    let hash = ModelVerifier::compute_sha256_file(tmp.path()).unwrap();
    assert_eq!(hash.len(), 64); // SHA256 hex = 64 chars
    assert!(hash.chars().all(|c| c.is_ascii_hexdigit()));

    // Same file should produce same hash
    let hash2 = ModelVerifier::compute_sha256_file(tmp.path()).unwrap();
    assert_eq!(hash, hash2);
}

#[test]
fn test_model_manifest() {
    let registry = iamine_models::ModelRegistry::new();
    let model = registry.get("tinyllama-1b").unwrap();
    let manifest = model.to_manifest();

    assert_eq!(manifest.model_id, "tinyllama-1b");
    assert!(manifest.size_bytes > 0);
    assert!(!manifest.download_url.is_empty());
    assert!(manifest.download_url.contains("huggingface.co"));
}

#[test]
fn test_model_has_known_hash() {
    let registry = iamine_models::ModelRegistry::new();
    let model = registry.get("tinyllama-1b").unwrap();
    // Empty hash = no known hash
    assert!(!model.has_known_hash());

    let manifest = model.to_manifest();
    assert!(!manifest.requires_hash_verification());
}

#[test]
fn test_download_phase_enum() {
    use iamine_models::DownloadPhase;
    let phase = DownloadPhase::Downloading;
    assert_eq!(phase, DownloadPhase::Downloading);
    assert_ne!(phase, DownloadPhase::Verifying);
}
