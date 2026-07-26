mod authority;
mod configuration;
mod error;
mod evaluation;
mod evidence;

pub use authority::RuntimeCompatibilityAuthority;
pub use configuration::{
    RuntimeCompatibilityConfigurationError, RuntimeCompatibilityConfigurationErrorCode,
    RuntimeLanguageAvailability, RuntimeLanguageDecision, RuntimeLanguageMode,
    RuntimeNetworkAvailability, RuntimeResourceEnvelope,
};
pub use error::{
    RuntimeCompatibilityError, RuntimeCompatibilityErrorCode, RuntimeCompatibilityRequirement,
};
pub(crate) use evaluation::resolve_compatible_resource_profile;
pub(crate) use evidence::RuntimeCompatibilityAuthorityIdentity;
pub use evidence::{RuntimeCompatibilityEvidence, RuntimeCompatibilityEvidenceStatus};
