use sha2::{Sha256, Digest};
use std::path::Path;
use std::io::Read;

pub struct ModelVerifier;

impl ModelVerifier {
    /// Verify file against expected SHA256 hash.
    /// Returns Ok(()) if hash matches or if hash is empty/placeholder (skip).
    pub fn verify_file(path: &Path, expected_hash: &str) -> Result<(), String> {
        if Self::should_skip_verification(expected_hash) {
            println!("   ⏭️  Hash verification skipped (no known hash)");
            return Ok(());
        }

        if !path.exists() {
            return Err(format!("File not found: {}", path.display()));
        }

        println!("   🔐 Verifying SHA256...");
        let computed = Self::compute_sha256_file(path)?;

        if computed == expected_hash {
            println!("   ✅ SHA256 verified: {}...{}", &computed[..8], &computed[computed.len()-8..]);
            Ok(())
        } else {
            // Delete corrupt file
            let _ = std::fs::remove_file(path);
            Err(format!(
                "SHA256 mismatch!\n   Expected: {}\n   Got:      {}\n   File deleted.",
                expected_hash, computed
            ))
        }
    }

    /// Verify raw bytes against expected SHA256.
    pub fn verify_bytes(data: &[u8], expected_hash: &str) -> Result<(), String> {
        if Self::should_skip_verification(expected_hash) {
            return Ok(());
        }

        let computed = Self::compute_sha256_bytes(data);
        if computed == expected_hash {
            Ok(())
        } else {
            Err(format!("SHA256 mismatch: expected {} got {}", expected_hash, computed))
        }
    }

    /// Compute SHA256 of a file (streaming, works with large files)
    pub fn compute_sha256_file(path: &Path) -> Result<String, String> {
        let mut file = std::fs::File::open(path)
            .map_err(|e| format!("Cannot open {}: {}", path.display(), e))?;

        let file_size = file.metadata().map(|m| m.len()).unwrap_or(0);
        let mut hasher = Sha256::new();
        let mut buffer = vec![0u8; 8 * 1024 * 1024]; // 8MB chunks
        let mut processed: u64 = 0;
        let mut last_report = std::time::Instant::now();

        loop {
            let n = file.read(&mut buffer)
                .map_err(|e| format!("Read error: {}", e))?;
            if n == 0 { break; }
            hasher.update(&buffer[..n]);
            processed += n as u64;

            // Progress for large files (>100MB)
            if file_size > 100_000_000 && last_report.elapsed().as_millis() > 1000 {
                let pct = processed as f64 / file_size as f64 * 100.0;
                print!("\r   🔐 Hashing: {:.0}%", pct);
                let _ = std::io::Write::flush(&mut std::io::stdout());
                last_report = std::time::Instant::now();
            }
        }

        if file_size > 100_000_000 {
            println!(); // newline after progress
        }

        Ok(format!("{:x}", hasher.finalize()))
    }

    /// Compute SHA256 of in-memory bytes
    pub fn compute_sha256_bytes(data: &[u8]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(data);
        format!("{:x}", hasher.finalize())
    }

    /// Check if hash verification should be skipped
    fn should_skip_verification(hash: &str) -> bool {
        hash.is_empty() || hash.ends_with("_placeholder") || hash == "skip"
    }
}
