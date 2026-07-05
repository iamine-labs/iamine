use crate::arg_values::{values_from_repeated_flag, RepeatedFlagArgError};
use libp2p::PeerId;
use std::collections::HashSet;
use std::error::Error;
use std::fmt;
use std::str::FromStr;

pub const TESTNET_ADMISSION_FLAG: &str = "--testnet-admission";
pub const TESTNET_ALLOW_PEER_FLAG: &str = "--testnet-allow-peer";
pub const MAX_TESTNET_ALLOWED_PEERS: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestnetAdmissionMode {
    Open,
    Allowlist,
}

impl TestnetAdmissionMode {
    pub fn parse(value: &str) -> Result<Self, TestnetAdmissionModeParseError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "open" | "off" | "disabled" | "none" => Ok(Self::Open),
            "allowlist" | "allow-list" | "operator" | "operator-configured" | "private" => {
                Ok(Self::Allowlist)
            }
            _ => Err(TestnetAdmissionModeParseError),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Allowlist => "allowlist",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct TestnetAdmissionModeParseError;

impl fmt::Debug for TestnetAdmissionModeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for TestnetAdmissionModeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "testnet admission must be open or allowlist")
    }
}

impl Error for TestnetAdmissionModeParseError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestnetAdmissionPolicy {
    mode: TestnetAdmissionMode,
    allowed_peers: Vec<PeerId>,
}

impl TestnetAdmissionPolicy {
    pub fn open() -> Self {
        Self {
            mode: TestnetAdmissionMode::Open,
            allowed_peers: Vec::new(),
        }
    }

    pub fn allowlist(allowed_peers: Vec<PeerId>) -> Self {
        Self {
            mode: TestnetAdmissionMode::Allowlist,
            allowed_peers,
        }
    }

    pub fn mode(&self) -> TestnetAdmissionMode {
        self.mode
    }

    pub fn mode_name(&self) -> &'static str {
        self.mode.as_str()
    }

    pub fn allowed_peers(&self) -> &[PeerId] {
        &self.allowed_peers
    }

    pub fn allowed_peer_count(&self) -> usize {
        self.allowed_peers.len()
    }

    pub fn is_restricted(&self) -> bool {
        matches!(self.mode, TestnetAdmissionMode::Allowlist)
    }

    pub fn allows_peer(&self, peer_id: &PeerId) -> bool {
        match self.mode {
            TestnetAdmissionMode::Open => true,
            TestnetAdmissionMode::Allowlist => self.allowed_peers.contains(peer_id),
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum TestnetAdmissionArgError {
    MissingMode,
    InvalidMode {
        source: TestnetAdmissionModeParseError,
    },
    TooManyModes,
    MissingAllowedPeer,
    InvalidAllowedPeer,
    TooManyAllowedPeers {
        max: usize,
    },
    AllowedPeersRequireAllowlist,
    AllowlistRequiresAllowedPeer,
}

impl fmt::Debug for TestnetAdmissionArgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for TestnetAdmissionArgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingMode => write!(f, "--testnet-admission requires a mode value"),
            Self::InvalidMode { source } => {
                write!(f, "invalid --testnet-admission value: {}", source)
            }
            Self::TooManyModes => write!(f, "--testnet-admission may be configured only once"),
            Self::MissingAllowedPeer => write!(f, "--testnet-allow-peer requires a peer id value"),
            Self::InvalidAllowedPeer => {
                write!(f, "invalid --testnet-allow-peer value: expected peer id")
            }
            Self::TooManyAllowedPeers { max } => {
                write!(
                    f,
                    "too many testnet allowed peers configured; maximum is {}",
                    max
                )
            }
            Self::AllowedPeersRequireAllowlist => write!(
                f,
                "--testnet-allow-peer requires --testnet-admission allowlist"
            ),
            Self::AllowlistRequiresAllowedPeer => write!(
                f,
                "--testnet-admission allowlist requires at least one --testnet-allow-peer"
            ),
        }
    }
}

impl Error for TestnetAdmissionArgError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidMode { source } => Some(source),
            _ => None,
        }
    }
}

pub fn testnet_admission_policy_from_args(
    args: &[String],
) -> Result<TestnetAdmissionPolicy, TestnetAdmissionArgError> {
    let mode_values =
        values_from_repeated_flag(args, TESTNET_ADMISSION_FLAG).map_err(|error| match error {
            RepeatedFlagArgError::MissingValue => TestnetAdmissionArgError::MissingMode,
        })?;
    if mode_values.len() > 1 {
        return Err(TestnetAdmissionArgError::TooManyModes);
    }

    let mode = match mode_values.first() {
        Some(value) => TestnetAdmissionMode::parse(value)
            .map_err(|source| TestnetAdmissionArgError::InvalidMode { source })?,
        None => TestnetAdmissionMode::Open,
    };

    let allowed_peer_values =
        values_from_repeated_flag(args, TESTNET_ALLOW_PEER_FLAG).map_err(|error| match error {
            RepeatedFlagArgError::MissingValue => TestnetAdmissionArgError::MissingAllowedPeer,
        })?;
    let allowed_peers = allowed_peers_from_values(allowed_peer_values)?;

    if !allowed_peers.is_empty() && !matches!(mode, TestnetAdmissionMode::Allowlist) {
        return Err(TestnetAdmissionArgError::AllowedPeersRequireAllowlist);
    }

    if matches!(mode, TestnetAdmissionMode::Allowlist) && allowed_peers.is_empty() {
        return Err(TestnetAdmissionArgError::AllowlistRequiresAllowedPeer);
    }

    Ok(match mode {
        TestnetAdmissionMode::Open => TestnetAdmissionPolicy::open(),
        TestnetAdmissionMode::Allowlist => TestnetAdmissionPolicy::allowlist(allowed_peers),
    })
}

pub fn allowed_peers_from_values<'a, I>(values: I) -> Result<Vec<PeerId>, TestnetAdmissionArgError>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut seen = HashSet::new();
    let mut peers = Vec::new();

    for value in values {
        if peers.len() >= MAX_TESTNET_ALLOWED_PEERS {
            return Err(TestnetAdmissionArgError::TooManyAllowedPeers {
                max: MAX_TESTNET_ALLOWED_PEERS,
            });
        }

        let peer = PeerId::from_str(value.trim())
            .map_err(|_| TestnetAdmissionArgError::InvalidAllowedPeer)?;
        if seen.insert(peer) {
            peers.push(peer);
        }
    }

    Ok(peers)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn testnet_admission_defaults_to_open() {
        let result = testnet_admission_policy_from_args(&args(&["iamine-node"]));
        let Ok(policy) = result else {
            assert!(result.is_ok(), "default policy should parse");
            return;
        };

        assert_eq!(policy.mode(), TestnetAdmissionMode::Open);
        assert!(!policy.is_restricted());
        assert!(policy.allows_peer(&PeerId::random()));
    }

    #[test]
    fn testnet_admission_accepts_allowlist_peer_ids() {
        let peer_a = PeerId::random();
        let peer_b = PeerId::random();
        let result = testnet_admission_policy_from_args(&args(&[
            "iamine-node",
            "--testnet-admission=allowlist",
            "--testnet-allow-peer",
            &peer_a.to_string(),
            &format!("--testnet-allow-peer={peer_b}"),
        ]));
        let Ok(policy) = result else {
            assert!(result.is_ok(), "allowlist should parse");
            return;
        };

        assert_eq!(policy.mode(), TestnetAdmissionMode::Allowlist);
        assert_eq!(policy.allowed_peer_count(), 2);
        assert!(policy.allows_peer(&peer_a));
        assert!(policy.allows_peer(&peer_b));
        assert!(!policy.allows_peer(&PeerId::random()));
    }

    #[test]
    fn testnet_admission_rejects_allow_peer_without_allowlist() {
        let peer = PeerId::random();
        let result = testnet_admission_policy_from_args(&args(&[
            "iamine-node",
            &format!("--testnet-allow-peer={peer}"),
        ]));

        assert!(matches!(
            result,
            Err(TestnetAdmissionArgError::AllowedPeersRequireAllowlist)
        ));
    }

    #[test]
    fn testnet_admission_requires_peer_for_allowlist() {
        let result = testnet_admission_policy_from_args(&args(&[
            "iamine-node",
            "--testnet-admission=allowlist",
        ]));

        assert!(matches!(
            result,
            Err(TestnetAdmissionArgError::AllowlistRequiresAllowedPeer)
        ));
    }

    #[test]
    fn testnet_admission_rejects_invalid_peer_ids() {
        let result = testnet_admission_policy_from_args(&args(&[
            "iamine-node",
            "--testnet-admission=allowlist",
            "--testnet-allow-peer=not-a-peer",
        ]));

        assert!(matches!(
            result,
            Err(TestnetAdmissionArgError::InvalidAllowedPeer)
        ));
    }

    #[test]
    fn testnet_admission_deduplicates_peer_ids() {
        let peer = PeerId::random();
        let result = testnet_admission_policy_from_args(&args(&[
            "iamine-node",
            "--testnet-admission=allowlist",
            &format!("--testnet-allow-peer={peer}"),
            &format!("--testnet-allow-peer={peer}"),
        ]));
        let Ok(policy) = result else {
            assert!(result.is_ok(), "allowlist should parse");
            return;
        };

        assert_eq!(policy.allowed_peer_count(), 1);
        assert!(policy.allows_peer(&peer));
    }
}
