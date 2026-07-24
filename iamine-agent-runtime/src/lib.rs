mod contract;
mod error;
mod foundation;
mod limits;
mod owner;
mod reference;
mod resolver;
mod review_evidence;
mod runtime_compatibility;

pub use contract::DeclaredAgentPackage;
pub use error::{ResolverError, ResolverErrorCode};
pub use foundation::{
    inspect_runtime_foundation, RuntimeFoundationReport, RuntimeFoundationStatus,
};
pub use limits::{
    ResolverLimits, MAX_PACKAGE_REFERENCE_BYTES, MAX_PACKAGE_REFERENCE_COMPONENTS,
    MAX_PACKAGE_REFERENCE_COUNT, MAX_PACKAGE_REFERENCE_FILE_BYTES,
    MAX_PACKAGE_REFERENCE_TOTAL_BYTES,
};
pub use owner::{RuntimeOwner, RuntimeOwnerState, RuntimeOwnerStatus};
pub use reference::{PackageReferenceKind, ResolvedPackageReferences, ResolvedReference};
pub use resolver::PackageReferenceResolver;
pub use review_evidence::{
    DependencyPolicyReviewDecision, HumanReviewDecision, LanguagePolicyReviewDecision,
    LocalRegistryReviewDecision, PackageReviewAuthority, PackageReviewDecisions,
    PackageReviewEvidence, PackageReviewEvidenceStatus, PackageReviewRequirement,
    PackageReviewSubject, ReviewEvidenceError, ReviewEvidenceErrorCode,
};
pub use runtime_compatibility::{
    RuntimeCompatibilityAuthority, RuntimeCompatibilityConfigurationError,
    RuntimeCompatibilityConfigurationErrorCode, RuntimeCompatibilityError,
    RuntimeCompatibilityErrorCode, RuntimeCompatibilityEvidence,
    RuntimeCompatibilityEvidenceStatus, RuntimeCompatibilityRequirement,
    RuntimeLanguageAvailability, RuntimeLanguageDecision, RuntimeLanguageMode,
    RuntimeNetworkAvailability, RuntimeResourceEnvelope,
};
