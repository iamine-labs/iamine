#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkerInferenceBackend {
    Real,
    Mock,
}

impl WorkerInferenceBackend {
    pub(crate) fn from_env_value(value: Option<&str>) -> Self {
        if value
            .map(|value| value.eq_ignore_ascii_case("mock"))
            .unwrap_or(false)
        {
            Self::Mock
        } else {
            Self::Real
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Real => "real",
            Self::Mock => "mock",
        }
    }

    pub(crate) fn is_mock(self) -> bool {
        self == Self::Mock
    }

    pub(crate) fn is_real(self) -> bool {
        self == Self::Real
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_policy_selects_mock_from_env() {
        assert_eq!(
            WorkerInferenceBackend::from_env_value(Some("mock")),
            WorkerInferenceBackend::Mock
        );
        assert_eq!(
            WorkerInferenceBackend::from_env_value(Some("MOCK")),
            WorkerInferenceBackend::Mock
        );
    }

    #[test]
    fn backend_policy_defaults_to_real_for_unknown_or_empty_env() {
        assert_eq!(
            WorkerInferenceBackend::from_env_value(None),
            WorkerInferenceBackend::Real
        );
        assert_eq!(
            WorkerInferenceBackend::from_env_value(Some("cpu")),
            WorkerInferenceBackend::Real
        );
    }
}
