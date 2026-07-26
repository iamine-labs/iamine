use std::fmt;

pub const MAX_AGENT_TIMEOUT_MS: u64 = 3_600_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum AgentTimeoutClass {
    PermissionWait,
    ScopeCheck,
    SandboxStart,
    Execution,
    Handoff,
    Cleanup,
}

impl AgentTimeoutClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PermissionWait => "permission_wait_timeout",
            Self::ScopeCheck => "scope_check_timeout",
            Self::SandboxStart => "sandbox_start_timeout",
            Self::Execution => "execution_timeout",
            Self::Handoff => "handoff_timeout",
            Self::Cleanup => "cleanup_timeout",
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::PermissionWait => 0,
            Self::ScopeCheck => 1,
            Self::SandboxStart => 2,
            Self::Execution => 3,
            Self::Handoff => 4,
            Self::Cleanup => 5,
        }
    }
}

pub const AGENT_TIMEOUT_CLASSES: [AgentTimeoutClass; 6] = [
    AgentTimeoutClass::PermissionWait,
    AgentTimeoutClass::ScopeCheck,
    AgentTimeoutClass::SandboxStart,
    AgentTimeoutClass::Execution,
    AgentTimeoutClass::Handoff,
    AgentTimeoutClass::Cleanup,
];

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct TimeoutCancelPolicy {
    timeout_ms: [u64; AGENT_TIMEOUT_CLASSES.len()],
}

impl TimeoutCancelPolicy {
    pub fn new(
        permission_wait_ms: u64,
        scope_check_ms: u64,
        sandbox_start_ms: u64,
        execution_ms: u64,
        handoff_ms: u64,
        cleanup_ms: u64,
    ) -> Result<Self, TimeoutCancelConfigurationError> {
        let timeout_ms = [
            permission_wait_ms,
            scope_check_ms,
            sandbox_start_ms,
            execution_ms,
            handoff_ms,
            cleanup_ms,
        ];
        for timeout_class in AGENT_TIMEOUT_CLASSES {
            let value = timeout_ms[timeout_class.index()];
            if value == 0 {
                return Err(TimeoutCancelConfigurationError::new(
                    TimeoutCancelConfigurationErrorCode::ZeroTimeout,
                    timeout_class,
                ));
            }
            if value > MAX_AGENT_TIMEOUT_MS {
                return Err(TimeoutCancelConfigurationError::new(
                    TimeoutCancelConfigurationErrorCode::TimeoutTooLarge,
                    timeout_class,
                ));
            }
        }
        Ok(Self { timeout_ms })
    }

    pub const fn timeout_ms(self, timeout_class: AgentTimeoutClass) -> u64 {
        self.timeout_ms[timeout_class.index()]
    }
}

impl fmt::Debug for TimeoutCancelPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TimeoutCancelPolicy")
            .field("timeouts", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TimeoutCancelConfigurationErrorCode {
    ZeroTimeout,
    TimeoutTooLarge,
}

impl TimeoutCancelConfigurationErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ZeroTimeout => "zero_timeout",
            Self::TimeoutTooLarge => "timeout_too_large",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::ZeroTimeout => "timeout must be non-zero",
            Self::TimeoutTooLarge => "timeout exceeds the supported maximum",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeoutCancelConfigurationError {
    code: TimeoutCancelConfigurationErrorCode,
    timeout_class: AgentTimeoutClass,
}

impl TimeoutCancelConfigurationError {
    const fn new(
        code: TimeoutCancelConfigurationErrorCode,
        timeout_class: AgentTimeoutClass,
    ) -> Self {
        Self {
            code,
            timeout_class,
        }
    }

    pub const fn code(self) -> TimeoutCancelConfigurationErrorCode {
        self.code
    }

    pub const fn timeout_class(self) -> AgentTimeoutClass {
        self.timeout_class
    }
}

impl fmt::Display for TimeoutCancelConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.message())
    }
}

impl std::error::Error for TimeoutCancelConfigurationError {}
