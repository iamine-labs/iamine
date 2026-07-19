use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PermissionConfirmation {
    NotProvided,
    TrustedOrchestratorConfirmed,
}

#[derive(Clone, Copy)]
pub struct PermissionRequestRef<'a> {
    package_id: &'a str,
    action: &'a str,
    required_categories: &'a [&'a str],
    confirmation: PermissionConfirmation,
}

impl fmt::Debug for PermissionRequestRef<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PermissionRequestRef")
            .field("required_category_count", &self.required_categories.len())
            .field("confirmation", &self.confirmation)
            .finish()
    }
}

impl<'a> PermissionRequestRef<'a> {
    pub const fn new(
        package_id: &'a str,
        action: &'a str,
        required_categories: &'a [&'a str],
        confirmation: PermissionConfirmation,
    ) -> Self {
        Self {
            package_id,
            action,
            required_categories,
            confirmation,
        }
    }

    pub(crate) const fn package_id(&self) -> &str {
        self.package_id
    }

    pub(crate) const fn action(&self) -> &str {
        self.action
    }

    pub(crate) const fn required_categories(&self) -> &[&str] {
        self.required_categories
    }

    pub(crate) const fn confirmation(&self) -> PermissionConfirmation {
        self.confirmation
    }
}
