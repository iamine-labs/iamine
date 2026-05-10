pub(crate) fn cpu_features_are_compatible_for_real_backend(
    cpu_features: &[String],
    accelerator: &str,
    target_arch: &str,
) -> bool {
    if !target_arch.eq_ignore_ascii_case("x86_64") {
        return true;
    }

    if !accelerator.eq_ignore_ascii_case("cpu") {
        return true;
    }

    cpu_features
        .iter()
        .any(|feature| feature.eq_ignore_ascii_case("AVX2"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_feature_guard_blocks_real_backend_on_legacy_cpu() {
        assert!(!cpu_features_are_compatible_for_real_backend(
            &[],
            "CPU",
            "x86_64"
        ));
    }

    #[test]
    fn cpu_feature_guard_allows_avx2_cpu() {
        assert!(cpu_features_are_compatible_for_real_backend(
            &["AVX2".to_string()],
            "CPU",
            "x86_64"
        ));
    }

    #[test]
    fn cpu_feature_guard_allows_non_cpu_accelerators() {
        assert!(cpu_features_are_compatible_for_real_backend(
            &[],
            "Metal",
            "x86_64"
        ));
    }
}
