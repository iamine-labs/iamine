use crate::model_registry::ModelDescriptor;

const DEFAULT_MAX_MODEL_SIZE_BYTES: u64 = 16 * 1_073_741_824;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelDownloadPolicyStatus {
    Allowed,
    Blocked,
    Staged,
    Quarantined,
    MetadataOnly,
    PendingChecksum,
    PendingLicense,
    PendingHardwareValidation,
}

impl ModelDownloadPolicyStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::Blocked => "blocked",
            Self::Staged => "staged",
            Self::Quarantined => "quarantined",
            Self::MetadataOnly => "metadata_only",
            Self::PendingChecksum => "pending_checksum",
            Self::PendingLicense => "pending_license",
            Self::PendingHardwareValidation => "pending_hardware_validation",
        }
    }

    pub fn permits_download(self) -> bool {
        matches!(
            self,
            Self::Allowed
                | Self::Staged
                | Self::PendingChecksum
                | Self::PendingLicense
                | Self::PendingHardwareValidation
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelDownloadRejectReason {
    UnknownModel,
    InvalidModelId,
    InvalidVersion,
    UnsupportedFormat,
    UntrustedSource,
    SourceUrlMissing,
    SizeExceedsPolicy,
    ChecksumMissing,
    ChecksumMismatch,
    ManualModelNotAllowed,
    ManifestMissing,
}

impl ModelDownloadRejectReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::UnknownModel => "unknown_model",
            Self::InvalidModelId => "invalid_model_id",
            Self::InvalidVersion => "invalid_version",
            Self::UnsupportedFormat => "unsupported_format",
            Self::UntrustedSource => "untrusted_source",
            Self::SourceUrlMissing => "source_url_missing",
            Self::SizeExceedsPolicy => "size_exceeds_policy",
            Self::ChecksumMissing => "checksum_missing",
            Self::ChecksumMismatch => "checksum_mismatch",
            Self::ManualModelNotAllowed => "manual_model_not_allowed",
            Self::ManifestMissing => "manifest_missing",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllowedModelSource {
    HuggingFace,
}

impl AllowedModelSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::HuggingFace => "huggingface",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllowedModelFormat {
    Gguf,
}

impl AllowedModelFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Gguf => "gguf",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelChecksumStatus {
    Verified,
    Pending,
    Missing,
    Mismatch,
}

impl ModelChecksumStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::Pending => "pending",
            Self::Missing => "missing",
            Self::Mismatch => "mismatch",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ModelDownloadRequest<'a> {
    pub model_id: &'a str,
    pub version: &'a str,
    pub source_url: Option<&'a str>,
    pub source_kind: Option<&'a str>,
    pub format: Option<&'a str>,
    pub size_bytes: Option<u64>,
    pub expected_sha256: Option<&'a str>,
    pub actual_sha256: Option<&'a str>,
    pub registry_known: bool,
    pub manual_model: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelDownloadDecision {
    pub model_id: String,
    pub status: ModelDownloadPolicyStatus,
    pub reasons: Vec<ModelDownloadRejectReason>,
    pub source: Option<AllowedModelSource>,
    pub format: Option<AllowedModelFormat>,
    pub source_trusted: bool,
    pub format_allowed: bool,
    pub checksum_status: ModelChecksumStatus,
}

impl ModelDownloadDecision {
    pub fn permits_download(&self) -> bool {
        self.status.permits_download()
    }

    pub fn policy_reason(&self) -> String {
        if self.reasons.is_empty() {
            return self.status.as_str().to_string();
        }
        self.reasons
            .iter()
            .map(|reason| reason.as_str())
            .collect::<Vec<_>>()
            .join(",")
    }
}

#[derive(Debug, Clone)]
pub struct ModelDownloadPolicy {
    max_model_size_bytes: u64,
    allow_manual_models: bool,
}

impl Default for ModelDownloadPolicy {
    fn default() -> Self {
        Self {
            max_model_size_bytes: DEFAULT_MAX_MODEL_SIZE_BYTES,
            allow_manual_models: false,
        }
    }
}

impl ModelDownloadPolicy {
    pub fn with_max_model_size_bytes(mut self, max_model_size_bytes: u64) -> Self {
        self.max_model_size_bytes = max_model_size_bytes;
        self
    }

    pub fn allow_manual_models(mut self, allow_manual_models: bool) -> Self {
        self.allow_manual_models = allow_manual_models;
        self
    }

    pub fn evaluate_descriptor(&self, model: &ModelDescriptor) -> ModelDownloadDecision {
        self.evaluate(&ModelDownloadRequest {
            model_id: &model.id,
            version: &model.version,
            source_url: Some(&model.download_url),
            source_kind: Some("registry"),
            format: None,
            size_bytes: Some(model.size_bytes),
            expected_sha256: Some(&model.hash),
            actual_sha256: None,
            registry_known: true,
            manual_model: false,
        })
    }

    pub fn evaluate(&self, request: &ModelDownloadRequest<'_>) -> ModelDownloadDecision {
        let mut reasons = Vec::new();

        if !valid_model_id(request.model_id) {
            reasons.push(ModelDownloadRejectReason::InvalidModelId);
        }
        if !valid_version(request.version) {
            reasons.push(ModelDownloadRejectReason::InvalidVersion);
        }
        if !request.registry_known {
            reasons.push(ModelDownloadRejectReason::UnknownModel);
        }
        if request.manual_model && !self.allow_manual_models {
            reasons.push(ModelDownloadRejectReason::ManualModelNotAllowed);
        }

        let source = request.source_url.and_then(allowed_source_from_url);
        if request
            .source_url
            .map(|url| url.trim().is_empty())
            .unwrap_or(true)
        {
            reasons.push(ModelDownloadRejectReason::SourceUrlMissing);
        } else if source.is_none() {
            reasons.push(ModelDownloadRejectReason::UntrustedSource);
        }

        let format = request
            .format
            .and_then(allowed_format_from_value)
            .or_else(|| request.source_url.and_then(allowed_format_from_url));
        if format.is_none() {
            reasons.push(ModelDownloadRejectReason::UnsupportedFormat);
        }

        match request.size_bytes {
            Some(0) | None => reasons.push(ModelDownloadRejectReason::ManifestMissing),
            Some(size) if size > self.max_model_size_bytes => {
                reasons.push(ModelDownloadRejectReason::SizeExceedsPolicy)
            }
            Some(_) => {}
        }

        let checksum_status = checksum_status(request.expected_sha256, request.actual_sha256);
        match checksum_status {
            ModelChecksumStatus::Missing => {
                reasons.push(ModelDownloadRejectReason::ChecksumMissing)
            }
            ModelChecksumStatus::Mismatch => {
                reasons.push(ModelDownloadRejectReason::ChecksumMismatch)
            }
            ModelChecksumStatus::Verified | ModelChecksumStatus::Pending => {}
        }

        dedup_reasons(&mut reasons);
        let status = status_for_reasons(&reasons);

        ModelDownloadDecision {
            model_id: request.model_id.to_string(),
            status,
            source,
            format,
            source_trusted: source.is_some(),
            format_allowed: format.is_some(),
            checksum_status,
            reasons,
        }
    }
}

fn status_for_reasons(reasons: &[ModelDownloadRejectReason]) -> ModelDownloadPolicyStatus {
    if reasons.is_empty() {
        return ModelDownloadPolicyStatus::Allowed;
    }
    if reasons
        .iter()
        .all(|reason| *reason == ModelDownloadRejectReason::ChecksumMissing)
    {
        return ModelDownloadPolicyStatus::PendingChecksum;
    }
    if reasons.contains(&ModelDownloadRejectReason::ChecksumMismatch)
        || reasons.contains(&ModelDownloadRejectReason::ManualModelNotAllowed)
    {
        return ModelDownloadPolicyStatus::Quarantined;
    }
    ModelDownloadPolicyStatus::Blocked
}

fn checksum_status(
    expected_sha256: Option<&str>,
    actual_sha256: Option<&str>,
) -> ModelChecksumStatus {
    let Some(expected) = expected_sha256.map(str::trim) else {
        return ModelChecksumStatus::Missing;
    };
    if expected.is_empty() || expected.ends_with("_placeholder") || expected == "skip" {
        return ModelChecksumStatus::Missing;
    }
    match actual_sha256
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(actual) if actual.eq_ignore_ascii_case(expected) => ModelChecksumStatus::Verified,
        Some(_) => ModelChecksumStatus::Mismatch,
        None => ModelChecksumStatus::Pending,
    }
}

fn valid_model_id(model_id: &str) -> bool {
    let trimmed = model_id.trim();
    !trimmed.is_empty()
        && trimmed.len() <= 128
        && trimmed
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
        && trimmed
            .chars()
            .next()
            .is_some_and(|ch| ch.is_ascii_alphanumeric())
        && trimmed
            .chars()
            .last()
            .is_some_and(|ch| ch.is_ascii_alphanumeric())
}

fn valid_version(version: &str) -> bool {
    let trimmed = version.trim();
    !trimmed.is_empty()
        && trimmed.len() <= 64
        && trimmed
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
        && trimmed.chars().any(|ch| ch.is_ascii_alphanumeric())
}

fn allowed_source_from_url(url: &str) -> Option<AllowedModelSource> {
    let host = https_host(url)?;
    if host == "huggingface.co" || host.ends_with(".huggingface.co") || host == "hf.co" {
        Some(AllowedModelSource::HuggingFace)
    } else {
        None
    }
}

fn https_host(url: &str) -> Option<&str> {
    let rest = url.trim().strip_prefix("https://")?;
    let host = rest
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default()
        .split('@')
        .next_back()
        .unwrap_or_default();
    let host = host.split(':').next().unwrap_or_default();
    (!host.is_empty()).then_some(host)
}

fn allowed_format_from_value(value: &str) -> Option<AllowedModelFormat> {
    if value.trim().eq_ignore_ascii_case("gguf") {
        Some(AllowedModelFormat::Gguf)
    } else {
        None
    }
}

fn allowed_format_from_url(url: &str) -> Option<AllowedModelFormat> {
    let path = url
        .trim()
        .split(['?', '#'])
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    if path.ends_with(".gguf") {
        Some(AllowedModelFormat::Gguf)
    } else {
        None
    }
}

fn dedup_reasons(reasons: &mut Vec<ModelDownloadRejectReason>) {
    let mut deduped = Vec::new();
    for reason in reasons.iter() {
        if !deduped.contains(reason) {
            deduped.push(*reason);
        }
    }
    *reasons = deduped;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ModelRegistry;

    fn request<'a>(
        model_id: &'a str,
        version: &'a str,
        source_url: &'a str,
    ) -> ModelDownloadRequest<'a> {
        ModelDownloadRequest {
            model_id,
            version,
            source_url: Some(source_url),
            source_kind: Some("registry"),
            format: None,
            size_bytes: Some(669_000_000),
            expected_sha256: Some(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            ),
            actual_sha256: None,
            registry_known: true,
            manual_model: false,
        }
    }

    #[test]
    fn download_policy_allows_known_registry_model() {
        let registry = ModelRegistry::new();
        let model = registry.get("tinyllama-1b").unwrap();
        let decision = ModelDownloadPolicy::default().evaluate_descriptor(model);

        assert_eq!(decision.status, ModelDownloadPolicyStatus::PendingChecksum);
        assert!(decision.permits_download());
        assert_eq!(decision.source, Some(AllowedModelSource::HuggingFace));
        assert_eq!(decision.format, Some(AllowedModelFormat::Gguf));
    }

    #[test]
    fn download_policy_rejects_unknown_model() {
        let mut req = request(
            "unknown-model",
            "1.0",
            "https://huggingface.co/org/model/resolve/main/model.gguf",
        );
        req.registry_known = false;

        let decision = ModelDownloadPolicy::default().evaluate(&req);

        assert_eq!(decision.status, ModelDownloadPolicyStatus::Blocked);
        assert!(decision
            .reasons
            .contains(&ModelDownloadRejectReason::UnknownModel));
        assert!(!decision.permits_download());
    }

    #[test]
    fn download_policy_rejects_invalid_model_id() {
        let decision = ModelDownloadPolicy::default().evaluate(&request(
            "../bad-model",
            "1.0",
            "https://huggingface.co/org/model/resolve/main/model.gguf",
        ));

        assert!(decision
            .reasons
            .contains(&ModelDownloadRejectReason::InvalidModelId));
        assert!(!decision.permits_download());
    }

    #[test]
    fn download_policy_rejects_invalid_version() {
        let decision = ModelDownloadPolicy::default().evaluate(&request(
            "tinyllama-1b",
            "1.0 beta!",
            "https://huggingface.co/org/model/resolve/main/model.gguf",
        ));

        assert!(decision
            .reasons
            .contains(&ModelDownloadRejectReason::InvalidVersion));
    }

    #[test]
    fn download_policy_rejects_untrusted_source() {
        let decision = ModelDownloadPolicy::default().evaluate(&request(
            "tinyllama-1b",
            "1.0",
            "https://example.com/model.gguf",
        ));

        assert!(decision
            .reasons
            .contains(&ModelDownloadRejectReason::UntrustedSource));
        assert!(!decision.source_trusted);
    }

    #[test]
    fn download_policy_rejects_unsupported_format() {
        let decision = ModelDownloadPolicy::default().evaluate(&request(
            "tinyllama-1b",
            "1.0",
            "https://huggingface.co/org/model/resolve/main/model.bin",
        ));

        assert!(decision
            .reasons
            .contains(&ModelDownloadRejectReason::UnsupportedFormat));
        assert!(!decision.format_allowed);
    }

    #[test]
    fn download_policy_flags_missing_checksum() {
        let mut req = request(
            "tinyllama-1b",
            "1.0",
            "https://huggingface.co/org/model/resolve/main/model.gguf",
        );
        req.expected_sha256 = Some("");

        let decision = ModelDownloadPolicy::default().evaluate(&req);

        assert_eq!(decision.status, ModelDownloadPolicyStatus::PendingChecksum);
        assert_eq!(decision.checksum_status, ModelChecksumStatus::Missing);
        assert!(decision.permits_download());
    }

    #[test]
    fn download_policy_blocks_checksum_mismatch() {
        let mut req = request(
            "tinyllama-1b",
            "1.0",
            "https://huggingface.co/org/model/resolve/main/model.gguf",
        );
        req.expected_sha256 =
            Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        req.actual_sha256 =
            Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        let decision = ModelDownloadPolicy::default().evaluate(&req);

        assert_eq!(decision.status, ModelDownloadPolicyStatus::Quarantined);
        assert!(decision
            .reasons
            .contains(&ModelDownloadRejectReason::ChecksumMismatch));
        assert!(!decision.permits_download());
    }

    #[test]
    fn download_policy_rejects_manual_unregistered_model() {
        let mut req = request(
            "manual-model",
            "1.0",
            "https://huggingface.co/org/model/resolve/main/model.gguf",
        );
        req.registry_known = false;
        req.manual_model = true;

        let decision = ModelDownloadPolicy::default().evaluate(&req);

        assert_eq!(decision.status, ModelDownloadPolicyStatus::Quarantined);
        assert!(decision
            .reasons
            .contains(&ModelDownloadRejectReason::ManualModelNotAllowed));
        assert!(!decision.permits_download());
    }

    #[test]
    fn download_policy_rejects_size_above_policy() {
        let mut req = request(
            "tinyllama-1b",
            "1.0",
            "https://huggingface.co/org/model/resolve/main/model.gguf",
        );
        req.size_bytes = Some(DEFAULT_MAX_MODEL_SIZE_BYTES + 1);

        let decision = ModelDownloadPolicy::default().evaluate(&req);

        assert!(decision
            .reasons
            .contains(&ModelDownloadRejectReason::SizeExceedsPolicy));
    }
}
