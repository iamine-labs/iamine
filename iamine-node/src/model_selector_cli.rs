use iamine_models::{
    build_model_catalog_entries, select_model_catalog_download_candidate, LicenseAcceptanceStore,
    ModelCatalogDownloadAction, ModelCatalogEntry, ModelNodeCapabilities, ModelRegistry,
    ModelStorage,
};

pub struct ModelSelectorCLI;

impl ModelSelectorCLI {
    /// Mostrar modelos instalados y disponibles según capacidades del nodo.
    pub fn show_model_menu(peer_id: &str) -> Result<(), String> {
        Self::show_catalog(peer_id)
    }

    pub fn show_catalog(peer_id: &str) -> Result<(), String> {
        let entries = Self::catalog_entries(peer_id);
        Self::print_catalog(&entries);
        Ok(())
    }

    pub fn show_selection(peer_id: &str) -> Result<(), String> {
        let entries = Self::catalog_entries(peer_id);

        println!("Catalog selection:");
        match select_model_catalog_download_candidate(&entries, None)? {
            Some(selection) => {
                println!(
                    "selected={} reason={}",
                    selection.model_id, selection.reason
                );
                Self::print_entry(&selection.entry);
            }
            None => {
                println!("selected=(none)");
                println!("reason=no_ready_compatible_model");
            }
        }
        Ok(())
    }

    pub fn download_preflight(peer_id: &str, model_id: &str) -> Result<ModelCatalogEntry, String> {
        let entries = Self::catalog_entries(peer_id);
        entries
            .into_iter()
            .find(|entry| entry.id == model_id)
            .ok_or_else(|| format!("model '{model_id}' not found in catalog"))
    }

    pub fn print_download_block(entry: &ModelCatalogEntry) {
        println!(
            "Download blocked: model={} action={} reason={}",
            entry.id,
            entry.download_action.as_str(),
            entry.download_reason
        );
        Self::print_gates(entry);
        if entry.download_action == ModelCatalogDownloadAction::LicenseAcceptanceRequired {
            println!(
                "Next step: iamine-node models license accept {} --yes",
                entry.id
            );
        }
    }

    fn catalog_entries(peer_id: &str) -> Vec<ModelCatalogEntry> {
        let registry = ModelRegistry::new();
        let storage = ModelStorage::new();
        let caps = ModelNodeCapabilities::detect(peer_id);
        let license_acceptance_store = LicenseAcceptanceStore::new();

        build_model_catalog_entries(&registry, &storage, &license_acceptance_store, &caps)
    }

    fn print_catalog(entries: &[ModelCatalogEntry]) {
        println!("IaMine model catalog:");
        if entries.is_empty() {
            println!("(empty)");
            return;
        }

        for entry in entries {
            Self::print_entry(entry);
        }
    }

    fn print_entry(entry: &ModelCatalogEntry) {
        println!(
            "- {} v{} | size={:.1}GB | ram={}GB | storage={}GB | installed={} | compatibility={} | action={} | reason={}",
            entry.id,
            entry.version,
            entry.size_gb,
            entry.required_ram_gb,
            entry.required_storage_gb,
            entry.installed,
            entry.compatibility_status,
            entry.download_action.as_str(),
            entry.download_reason
        );
        Self::print_gates(entry);
    }

    fn print_gates(entry: &ModelCatalogEntry) {
        for gate in &entry.gates {
            println!("  gate {}={} ({})", gate.gate, gate.status, gate.reason);
        }
    }
}
