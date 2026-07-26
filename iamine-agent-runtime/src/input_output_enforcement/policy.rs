use std::fmt;

pub const MAX_INPUT_OUTPUT_RECORD_BYTES: usize = 64 * 1024;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct InputOutputPolicy {
    max_input_bytes: usize,
    max_output_bytes: usize,
    operator_visible_outputs: bool,
}

impl InputOutputPolicy {
    pub fn new(
        max_input_bytes: usize,
        max_output_bytes: usize,
        operator_visible_outputs: bool,
    ) -> Result<Self, InputOutputConfigurationError> {
        validate_limit(
            max_input_bytes,
            InputOutputConfigurationErrorCode::ZeroInputLimit,
            InputOutputConfigurationErrorCode::InputLimitTooLarge,
        )?;
        validate_limit(
            max_output_bytes,
            InputOutputConfigurationErrorCode::ZeroOutputLimit,
            InputOutputConfigurationErrorCode::OutputLimitTooLarge,
        )?;
        Ok(Self {
            max_input_bytes,
            max_output_bytes,
            operator_visible_outputs,
        })
    }

    pub const fn max_input_bytes(self) -> usize {
        self.max_input_bytes
    }

    pub const fn max_output_bytes(self) -> usize {
        self.max_output_bytes
    }

    pub const fn operator_visible_outputs(self) -> bool {
        self.operator_visible_outputs
    }
}

impl fmt::Debug for InputOutputPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InputOutputPolicy")
            .field("max_input_bytes", &"[redacted]")
            .field("max_output_bytes", &"[redacted]")
            .field("operator_visible_outputs", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum InputOutputConfigurationErrorCode {
    ZeroInputLimit,
    ZeroOutputLimit,
    InputLimitTooLarge,
    OutputLimitTooLarge,
}

impl InputOutputConfigurationErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ZeroInputLimit => "zero_input_limit",
            Self::ZeroOutputLimit => "zero_output_limit",
            Self::InputLimitTooLarge => "input_limit_too_large",
            Self::OutputLimitTooLarge => "output_limit_too_large",
        }
    }

    const fn message(self) -> &'static str {
        match self {
            Self::ZeroInputLimit => "input limit must be non-zero",
            Self::ZeroOutputLimit => "output limit must be non-zero",
            Self::InputLimitTooLarge => "input limit exceeds the hard record limit",
            Self::OutputLimitTooLarge => "output limit exceeds the hard record limit",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InputOutputConfigurationError {
    code: InputOutputConfigurationErrorCode,
}

impl InputOutputConfigurationError {
    const fn new(code: InputOutputConfigurationErrorCode) -> Self {
        Self { code }
    }

    pub const fn code(self) -> InputOutputConfigurationErrorCode {
        self.code
    }
}

impl fmt::Display for InputOutputConfigurationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code.message())
    }
}

impl std::error::Error for InputOutputConfigurationError {}

fn validate_limit(
    limit: usize,
    zero: InputOutputConfigurationErrorCode,
    too_large: InputOutputConfigurationErrorCode,
) -> Result<(), InputOutputConfigurationError> {
    if limit == 0 {
        return Err(InputOutputConfigurationError::new(zero));
    }
    if limit > MAX_INPUT_OUTPUT_RECORD_BYTES {
        return Err(InputOutputConfigurationError::new(too_large));
    }
    Ok(())
}
