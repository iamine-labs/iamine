#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LocalRegistryReviewDecision {
    Candidate,
    UnderReview,
    Blocked,
    RegistryReviewReady,
    Deprecated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum LanguagePolicyReviewDecision {
    RustOfficialAllowed,
    Experimental,
    Deferred,
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DependencyPolicyReviewDecision {
    Allowed,
    NeedsJustification,
    Deferred,
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum HumanReviewDecision {
    IndependentApproved,
    Missing,
    SelfApproved,
    Rejected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PackageReviewDecisions {
    registry: LocalRegistryReviewDecision,
    language: LanguagePolicyReviewDecision,
    dependencies: DependencyPolicyReviewDecision,
    human: HumanReviewDecision,
}

impl PackageReviewDecisions {
    pub const fn new(
        registry: LocalRegistryReviewDecision,
        language: LanguagePolicyReviewDecision,
        dependencies: DependencyPolicyReviewDecision,
        human: HumanReviewDecision,
    ) -> Self {
        Self {
            registry,
            language,
            dependencies,
            human,
        }
    }

    pub(crate) const fn registry(self) -> LocalRegistryReviewDecision {
        self.registry
    }

    pub(crate) const fn language(self) -> LanguagePolicyReviewDecision {
        self.language
    }

    pub(crate) const fn dependencies(self) -> DependencyPolicyReviewDecision {
        self.dependencies
    }

    pub(crate) const fn human(self) -> HumanReviewDecision {
        self.human
    }
}
