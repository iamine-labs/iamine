use std::{fmt, sync::Arc};

use crate::{
    execution_lifecycle::ExecutionIdentity,
    package_load_evidence_integration::PackageLoadEvidenceIdentity,
    package_loader::PackageLoaderAuthorityIdentity, sandbox_enforcement::SandboxEvidenceIdentity,
    ExecutionAuthorizationEvidence, LoadedAgentPackage, PackageReviewSubject,
};

use super::authority::RuntimeExecutorAuthorityIdentity;
use super::program::{OfficialRustProgram, OfficialRustProgramIdentity};

#[must_use]
pub struct RuntimeExecutionPermit<'subject> {
    pub(super) executor: Arc<RuntimeExecutorAuthorityIdentity>,
    pub(super) loader: Arc<PackageLoaderAuthorityIdentity>,
    pub(super) load_evidence: Arc<PackageLoadEvidenceIdentity>,
    pub(super) execution: Arc<ExecutionIdentity>,
    pub(super) sandbox: Arc<SandboxEvidenceIdentity>,
    pub(super) program: Arc<OfficialRustProgramIdentity>,
    pub(super) subject: PackageReviewSubject<'subject>,
    pub(super) lifecycle_revision: u8,
}

impl<'subject> RuntimeExecutionPermit<'subject> {
    pub(super) fn new(
        executor: Arc<RuntimeExecutorAuthorityIdentity>,
        loaded: &LoadedAgentPackage<'subject>,
        authorization: &ExecutionAuthorizationEvidence<'subject>,
        program: &OfficialRustProgram<'subject>,
    ) -> Self {
        Self {
            executor,
            loader: Arc::clone(loaded.authority()),
            load_evidence: Arc::clone(loaded.evidence()),
            execution: Arc::clone(authorization.execution()),
            sandbox: Arc::clone(authorization.sandbox()),
            program: Arc::clone(program.identity()),
            subject: loaded.subject(),
            lifecycle_revision: authorization.lifecycle_revision(),
        }
    }

    pub const fn lifecycle_revision(&self) -> u8 {
        self.lifecycle_revision
    }

    pub const fn execution_authorized(&self) -> bool {
        true
    }

    pub const fn package_loaded(&self) -> bool {
        true
    }

    pub const fn execution_started(&self) -> bool {
        false
    }

    pub const fn runtime_active(&self) -> bool {
        false
    }
}

impl fmt::Debug for RuntimeExecutionPermit<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeExecutionPermit")
            .field("executor", &"[redacted]")
            .field("loader", &"[redacted]")
            .field("load_evidence", &"[redacted]")
            .field("execution", &"[redacted]")
            .field("sandbox", &"[redacted]")
            .field("program", &"[redacted]")
            .field("subject", &"[redacted]")
            .field("lifecycle_revision", &self.lifecycle_revision)
            .field("execution_authorized", &true)
            .field("package_loaded", &true)
            .field("execution_started", &false)
            .field("runtime_active", &false)
            .finish()
    }
}
