#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScopeRequestClassification {
    InScopeCandidate,
    Ambiguous,
    Dangerous,
    CrossDomain,
    PermissionEscalation,
    PromptInjection,
    RoleConfusion,
}

use std::fmt;

#[derive(Clone, Copy)]
pub struct ScopeRequestRef<'a> {
    package_id: &'a str,
    task_type: &'a str,
    task: &'a str,
    operation: &'a str,
    input_classes: &'a [&'a str],
    classification: ScopeRequestClassification,
}

impl fmt::Debug for ScopeRequestRef<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScopeRequestRef")
            .field("input_class_count", &self.input_classes.len())
            .field("classification", &self.classification)
            .finish()
    }
}

impl<'a> ScopeRequestRef<'a> {
    pub const fn new(
        package_id: &'a str,
        task_type: &'a str,
        task: &'a str,
        operation: &'a str,
        input_classes: &'a [&'a str],
        classification: ScopeRequestClassification,
    ) -> Self {
        Self {
            package_id,
            task_type,
            task,
            operation,
            input_classes,
            classification,
        }
    }

    pub(crate) const fn package_id(&self) -> &str {
        self.package_id
    }

    pub fn targets_package(&self, package_id: &str) -> bool {
        self.package_id == package_id
    }

    pub(crate) const fn task_type(&self) -> &str {
        self.task_type
    }

    pub(crate) const fn task(&self) -> &str {
        self.task
    }

    pub(crate) const fn operation(&self) -> &str {
        self.operation
    }

    pub(crate) const fn input_classes(&self) -> &[&str] {
        self.input_classes
    }

    pub(crate) const fn classification(&self) -> ScopeRequestClassification {
        self.classification
    }
}
