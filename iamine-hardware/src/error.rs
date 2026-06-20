use std::fmt;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HardwareProfileError {
    Io(String),
    Json(String),
    Validation(String),
    LockAlreadyHeld(PathBuf),
}

impl fmt::Display for HardwareProfileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) => write!(formatter, "hardware profile io error: {message}"),
            Self::Json(message) => write!(formatter, "hardware profile json error: {message}"),
            Self::Validation(message) => {
                write!(formatter, "hardware profile validation error: {message}")
            }
            Self::LockAlreadyHeld(path) => {
                write!(
                    formatter,
                    "hardware profile lock already held: {}",
                    path.display()
                )
            }
        }
    }
}

impl std::error::Error for HardwareProfileError {}

impl From<std::io::Error> for HardwareProfileError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

impl From<serde_json::Error> for HardwareProfileError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error.to_string())
    }
}

pub type Result<T> = std::result::Result<T, HardwareProfileError>;
