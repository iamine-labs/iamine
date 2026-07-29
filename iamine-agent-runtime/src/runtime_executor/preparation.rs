use std::fmt;

use crate::{
    ExecutionAuthorizationAuthority, ExecutionAuthorizationEvidence, ExecutionAuthorizationRequest,
    LoadedAgentPackage, PackageLoadEvidence, PackageLoadEvidenceAuthority, PackageLoaderAuthority,
};

use super::{OfficialRustProgram, OfficialRustProgramRegistry};

#[must_use]
pub struct RuntimeExecutionPreparation<'input, 'context, 'subject> {
    pub(super) loaded: &'input LoadedAgentPackage<'subject>,
    pub(super) authorization_request: &'input ExecutionAuthorizationRequest<'context, 'subject>,
    pub(super) loader: Option<(
        &'input PackageLoaderAuthority,
        &'input PackageLoadEvidenceAuthority,
        &'input PackageLoadEvidence<'subject>,
    )>,
    pub(super) authorization: Option<(
        &'input ExecutionAuthorizationAuthority,
        &'input ExecutionAuthorizationEvidence<'subject>,
    )>,
    pub(super) program: Option<(
        &'input OfficialRustProgramRegistry,
        &'input OfficialRustProgram<'subject>,
    )>,
}

impl<'input, 'context, 'subject> RuntimeExecutionPreparation<'input, 'context, 'subject> {
    pub const fn new(
        loaded: &'input LoadedAgentPackage<'subject>,
        authorization_request: &'input ExecutionAuthorizationRequest<'context, 'subject>,
    ) -> Self {
        Self {
            loaded,
            authorization_request,
            loader: None,
            authorization: None,
            program: None,
        }
    }

    pub fn with_loader(
        mut self,
        authority: &'input PackageLoaderAuthority,
        evidence_authority: &'input PackageLoadEvidenceAuthority,
        evidence: &'input PackageLoadEvidence<'subject>,
    ) -> Self {
        self.loader = Some((authority, evidence_authority, evidence));
        self
    }

    pub fn with_authorization(
        mut self,
        authority: &'input ExecutionAuthorizationAuthority,
        evidence: &'input ExecutionAuthorizationEvidence<'subject>,
    ) -> Self {
        self.authorization = Some((authority, evidence));
        self
    }

    pub fn with_program(
        mut self,
        registry: &'input OfficialRustProgramRegistry,
        program: &'input OfficialRustProgram<'subject>,
    ) -> Self {
        self.program = Some((registry, program));
        self
    }
}

impl fmt::Debug for RuntimeExecutionPreparation<'_, '_, '_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeExecutionPreparation")
            .field("loaded", &"[redacted]")
            .field("authorization_request", &"[redacted]")
            .field("loader_present", &self.loader.is_some())
            .field("authorization_present", &self.authorization.is_some())
            .field("program_present", &self.program.is_some())
            .finish()
    }
}
