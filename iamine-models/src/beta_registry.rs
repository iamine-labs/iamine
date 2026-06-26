use crate::license_policy::{LicenseClass, LicenseMetadata};
use crate::model_registry::ModelDescriptor;
use crate::network_policy::NetworkPolicyMetadata;

const BETA_REGISTRY_REVISION: &str = "2026-06-25";

pub(crate) fn beta_model_descriptors() -> Vec<ModelDescriptor> {
    vec![tinyllama_1b(), llama_3_2_3b(), mistral_7b_instruct_v0_2()]
}

fn tinyllama_1b() -> ModelDescriptor {
    ModelDescriptor {
        id: "tinyllama-1b".to_string(),
        version: "1.0".to_string(),
        architecture: "llama".to_string(),
        size_bytes: 668_788_096,
        required_ram_gb: 2,
        required_vram_gb: 0,
        shards: 1,
        hash: "9fecc3b3cd76bba89d504f29b616eedf7da85b96540e490ca5824d3f7d2776a0"
            .to_string(),
        download_url: "https://huggingface.co/TheBloke/TinyLlama-1.1B-Chat-v1.0-GGUF/resolve/52e7645ba7c309695bec7ac98f4f005b139cf465/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf".to_string(),
        quantization: "q4_k_m".to_string(),
        license: apache_2_0_license(),
        network_policy: beta_network_policy(),
    }
}

fn llama_3_2_3b() -> ModelDescriptor {
    ModelDescriptor {
        id: "llama3-3b".to_string(),
        version: "3.2".to_string(),
        architecture: "llama".to_string(),
        size_bytes: 2_019_377_696,
        required_ram_gb: 4,
        required_vram_gb: 0,
        shards: 1,
        hash: "6c1a2b41161032677be168d354123594c0e6e67d2b9227c84f296ad037c728ff"
            .to_string(),
        download_url: "https://huggingface.co/bartowski/Llama-3.2-3B-Instruct-GGUF/resolve/5ab33fa94d1d04e903623ae72c95d1696f09f9e8/Llama-3.2-3B-Instruct-Q4_K_M.gguf".to_string(),
        quantization: "q4_k_m".to_string(),
        license: LicenseMetadata {
            license_id: Some("llama3.2".to_string()),
            license_url: Some(
                "https://developer.meta.com/ai/llama3_2/license/".to_string(),
            ),
            policy_class: Some(LicenseClass::RequiresAcceptance),
            requires_acceptance: true,
            revision: Some("2024-09-25".to_string()),
        },
        network_policy: beta_network_policy(),
    }
}

fn mistral_7b_instruct_v0_2() -> ModelDescriptor {
    ModelDescriptor {
        id: "mistral-7b".to_string(),
        version: "0.2".to_string(),
        architecture: "mistral".to_string(),
        size_bytes: 4_368_439_584,
        required_ram_gb: 8,
        required_vram_gb: 0,
        shards: 1,
        hash: "3e0039fd0273fcbebb49228943b17831aadd55cbcbf56f0af00499be2040ccf9"
            .to_string(),
        download_url: "https://huggingface.co/TheBloke/Mistral-7B-Instruct-v0.2-GGUF/resolve/3a6fbf4a41a1d52e415a4958cde6856d34b2db93/mistral-7b-instruct-v0.2.Q4_K_M.gguf".to_string(),
        quantization: "q4_k_m".to_string(),
        license: apache_2_0_license(),
        network_policy: beta_network_policy(),
    }
}

fn apache_2_0_license() -> LicenseMetadata {
    LicenseMetadata {
        license_id: Some("apache-2.0".to_string()),
        license_url: Some("https://www.apache.org/licenses/LICENSE-2.0".to_string()),
        policy_class: Some(LicenseClass::Allowed),
        requires_acceptance: false,
        revision: Some("2.0".to_string()),
    }
}

fn beta_network_policy() -> NetworkPolicyMetadata {
    NetworkPolicyMetadata::distributed_allowed(BETA_REGISTRY_REVISION)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        evaluate_model_registry_admission,
        evaluate_model_registry_admission_with_license_acceptance_store, LicenseAcceptanceStore,
        LicenseOperation, LicensePolicyStatus, ModelDownloadPolicyStatus, RegistryIntegrityStatus,
    };
    use tempfile::TempDir;

    #[test]
    fn beta_registry_metadata_is_complete_and_pinned() {
        let models = beta_model_descriptors();

        assert_eq!(models.len(), 3);
        for model in models {
            assert!(model.has_known_hash());
            assert_eq!(model.hash.len(), 64);
            assert!(model.size_bytes > 0);
            assert!(model.download_url.starts_with("https://huggingface.co/"));
            assert!(!model.download_url.contains("/resolve/main/"));
            assert_eq!(model.quantization, "q4_k_m");
            assert!(model.license.license_id.is_some());
            assert!(model.license.license_url.is_some());
            assert!(model.license.revision.is_some());
            assert!(model.network_policy.revision.is_some());
        }
    }

    #[test]
    fn beta_registry_artifact_values_match_reconciled_upstream_metadata(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let models = beta_model_descriptors();

        let tiny = models
            .iter()
            .find(|model| model.id == "tinyllama-1b")
            .ok_or("tinyllama metadata missing")?;
        assert_eq!(tiny.version, "1.0");
        assert_eq!(tiny.size_bytes, 668_788_096);
        assert_eq!(
            tiny.hash,
            "9fecc3b3cd76bba89d504f29b616eedf7da85b96540e490ca5824d3f7d2776a0"
        );
        assert!(tiny
            .download_url
            .contains("52e7645ba7c309695bec7ac98f4f005b139cf465"));

        let llama = models
            .iter()
            .find(|model| model.id == "llama3-3b")
            .ok_or("llama metadata missing")?;
        assert_eq!(llama.version, "3.2");
        assert_eq!(llama.size_bytes, 2_019_377_696);
        assert_eq!(
            llama.hash,
            "6c1a2b41161032677be168d354123594c0e6e67d2b9227c84f296ad037c728ff"
        );
        assert!(llama
            .download_url
            .contains("5ab33fa94d1d04e903623ae72c95d1696f09f9e8"));

        let mistral = models
            .iter()
            .find(|model| model.id == "mistral-7b")
            .ok_or("mistral metadata missing")?;
        assert_eq!(mistral.version, "0.2");
        assert_eq!(mistral.size_bytes, 4_368_439_584);
        assert_eq!(
            mistral.hash,
            "3e0039fd0273fcbebb49228943b17831aadd55cbcbf56f0af00499be2040ccf9"
        );
        assert!(mistral
            .download_url
            .contains("3a6fbf4a41a1d52e415a4958cde6856d34b2db93"));
        Ok(())
    }

    #[test]
    fn apache_beta_models_are_admitted_for_download() {
        for model in [tinyllama_1b(), mistral_7b_instruct_v0_2()] {
            let decision =
                evaluate_model_registry_admission(&model, LicenseOperation::Download, false);

            assert_eq!(decision.download.status, ModelDownloadPolicyStatus::Allowed);
            assert_eq!(
                decision.registry_integrity.status,
                RegistryIntegrityStatus::Trusted
            );
            assert_eq!(decision.license.status, LicensePolicyStatus::Allowed);
            assert!(decision.permits_operation);
        }
    }

    #[test]
    fn llama_beta_model_requires_explicit_license_acceptance(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let model = llama_3_2_3b();
        let decision = evaluate_model_registry_admission(&model, LicenseOperation::Download, false);

        assert_eq!(decision.download.status, ModelDownloadPolicyStatus::Allowed);
        assert_eq!(
            decision.registry_integrity.status,
            RegistryIntegrityStatus::Trusted
        );
        assert_eq!(
            decision.license.status,
            LicensePolicyStatus::RequiresAcceptance
        );
        assert!(!decision.permits_operation);
        assert!(decision
            .first_blocking_error()
            .is_some_and(|error| error.contains("license_acceptance_required")));

        let dir = TempDir::new()?;
        let store = LicenseAcceptanceStore::new_in(dir.path().join("acceptance.json"));
        store.accept_descriptor(&model)?;
        let accepted = evaluate_model_registry_admission_with_license_acceptance_store(
            &model,
            LicenseOperation::Download,
            false,
            &store,
        );

        assert!(accepted.permits_operation);
        assert!(accepted.first_blocking_error().is_none());
        Ok(())
    }
}
