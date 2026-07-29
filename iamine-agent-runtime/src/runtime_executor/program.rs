use std::{fmt, sync::Arc, time::Instant};

use crate::{
    CancellationHandle, OutputClassification, PackageReviewSubject, SandboxResourceLimits,
};

#[derive(Debug)]
pub(crate) struct OfficialRustProgramRegistryIdentity;

#[derive(Debug)]
pub(crate) struct OfficialRustProgramIdentity;

pub type OfficialRustProgramHandler =
    for<'context> fn(
        &RuntimeExecutionContext<'context>,
        &str,
    ) -> Result<OfficialRustProgramOutput, OfficialRustProgramFailure>;

pub struct OfficialRustProgramRegistry {
    identity: Arc<OfficialRustProgramRegistryIdentity>,
}

impl OfficialRustProgramRegistry {
    pub fn new_operator_local() -> Self {
        Self {
            identity: Arc::new(OfficialRustProgramRegistryIdentity),
        }
    }

    pub fn register<'subject>(
        &self,
        subject: PackageReviewSubject<'subject>,
        handler: OfficialRustProgramHandler,
    ) -> OfficialRustProgram<'subject> {
        OfficialRustProgram {
            registry: Arc::clone(&self.identity),
            identity: Arc::new(OfficialRustProgramIdentity),
            subject,
            handler,
        }
    }

    pub fn verifies(
        &self,
        program: &OfficialRustProgram<'_>,
        subject: PackageReviewSubject<'_>,
    ) -> bool {
        Arc::ptr_eq(&self.identity, &program.registry) && program.subject.same_as(subject)
    }
}

impl Default for OfficialRustProgramRegistry {
    fn default() -> Self {
        Self::new_operator_local()
    }
}

impl fmt::Debug for OfficialRustProgramRegistry {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OfficialRustProgramRegistry")
            .field("identity", &"[redacted]")
            .finish()
    }
}

#[must_use]
pub struct OfficialRustProgram<'subject> {
    registry: Arc<OfficialRustProgramRegistryIdentity>,
    identity: Arc<OfficialRustProgramIdentity>,
    subject: PackageReviewSubject<'subject>,
    handler: OfficialRustProgramHandler,
}

impl OfficialRustProgram<'_> {
    pub(crate) fn invoke(
        &self,
        context: &RuntimeExecutionContext<'_>,
        input: &str,
    ) -> Result<OfficialRustProgramOutput, OfficialRustProgramFailure> {
        (self.handler)(context, input)
    }

    pub(crate) const fn identity(&self) -> &Arc<OfficialRustProgramIdentity> {
        &self.identity
    }
}

impl fmt::Debug for OfficialRustProgram<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OfficialRustProgram")
            .field("registry", &"[redacted]")
            .field("identity", &"[redacted]")
            .field("subject", &"[redacted]")
            .field("handler", &"[redacted]")
            .finish()
    }
}

pub struct RuntimeExecutionContext<'context> {
    cancellation: &'context CancellationHandle,
    deadline: Instant,
    limits: SandboxResourceLimits,
}

impl<'context> RuntimeExecutionContext<'context> {
    pub(crate) const fn new(
        cancellation: &'context CancellationHandle,
        deadline: Instant,
        limits: SandboxResourceLimits,
    ) -> Self {
        Self {
            cancellation,
            deadline,
            limits,
        }
    }

    pub fn checkpoint(&self) -> Result<(), RuntimeExecutionInterrupt> {
        if self.cancellation.cancellation_requested() {
            return Err(RuntimeExecutionInterrupt::Cancelled);
        }
        if Instant::now() >= self.deadline {
            return Err(RuntimeExecutionInterrupt::TimedOut);
        }
        Ok(())
    }

    pub const fn resource_limits(&self) -> SandboxResourceLimits {
        self.limits
    }

    pub const fn network_allowed(&self) -> bool {
        false
    }

    pub const fn shell_allowed(&self) -> bool {
        false
    }

    pub const fn child_processes_allowed(&self) -> bool {
        false
    }

    pub const fn persistence_allowed(&self) -> bool {
        false
    }
}

impl fmt::Debug for RuntimeExecutionContext<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RuntimeExecutionContext")
            .field("cancellation", &"[redacted]")
            .field("deadline", &"[redacted]")
            .field("limits", &"[redacted]")
            .field("network_allowed", &false)
            .field("shell_allowed", &false)
            .field("child_processes_allowed", &false)
            .field("persistence_allowed", &false)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum RuntimeExecutionInterrupt {
    Cancelled,
    TimedOut,
}

#[must_use]
pub struct OfficialRustProgramOutput {
    classification: OutputClassification,
    operator_reviewed_redacted_content: Box<str>,
}

impl OfficialRustProgramOutput {
    pub fn operator_reviewed(
        classification: OutputClassification,
        redacted_content: impl Into<Box<str>>,
    ) -> Self {
        Self {
            classification,
            operator_reviewed_redacted_content: redacted_content.into(),
        }
    }

    pub const fn classification(&self) -> OutputClassification {
        self.classification
    }

    pub fn redacted_content(&self) -> &str {
        &self.operator_reviewed_redacted_content
    }
}

impl fmt::Debug for OfficialRustProgramOutput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OfficialRustProgramOutput")
            .field("classification", &self.classification)
            .field(
                "redacted_content_bytes",
                &self.operator_reviewed_redacted_content.len(),
            )
            .field("redacted_content", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum OfficialRustProgramFailureCode {
    RejectedInput,
    Interrupted,
    ExecutionFailed,
}

impl OfficialRustProgramFailureCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RejectedInput => "rejected_input",
            Self::Interrupted => "interrupted",
            Self::ExecutionFailed => "execution_failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OfficialRustProgramFailure {
    code: OfficialRustProgramFailureCode,
}

impl OfficialRustProgramFailure {
    pub const fn new(code: OfficialRustProgramFailureCode) -> Self {
        Self { code }
    }

    pub const fn code(self) -> OfficialRustProgramFailureCode {
        self.code
    }
}

impl From<RuntimeExecutionInterrupt> for OfficialRustProgramFailure {
    fn from(_: RuntimeExecutionInterrupt) -> Self {
        Self::new(OfficialRustProgramFailureCode::Interrupted)
    }
}

impl fmt::Display for OfficialRustProgramFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.as_str())
    }
}

impl std::error::Error for OfficialRustProgramFailure {}
