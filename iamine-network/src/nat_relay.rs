use crate::arg_values::{values_from_repeated_flag, RepeatedFlagArgError};
use crate::bootnode::{routing_addr_without_trailing_peer, trailing_peer_id};
use libp2p::{multiaddr::Protocol, Multiaddr, PeerId};
use std::collections::HashSet;
use std::error::Error;
use std::fmt;

pub const RELAY_POLICY_FLAG: &str = "--relay-policy";
pub const RELAY_PEER_FLAG: &str = "--relay-peer";
pub const MAX_RELAY_PEERS: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelayPolicyMode {
    Disabled,
    OperatorConfigured,
}

impl RelayPolicyMode {
    pub fn parse(value: &str) -> Result<Self, RelayPolicyModeParseError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "off" | "disabled" | "none" => Ok(Self::Disabled),
            "operator" | "operator-configured" | "explicit" | "relay" => {
                Ok(Self::OperatorConfigured)
            }
            _ => Err(RelayPolicyModeParseError),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::OperatorConfigured => "operator-configured",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct RelayPolicyModeParseError;

impl fmt::Debug for RelayPolicyModeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for RelayPolicyModeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "relay policy must be disabled or operator-configured")
    }
}

impl Error for RelayPolicyModeParseError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelayPeerSeed {
    dial_addr: Multiaddr,
    routing_addr: Multiaddr,
    peer_id: PeerId,
}

impl RelayPeerSeed {
    pub fn parse(value: &str) -> Result<Self, RelayPeerSeedParseError> {
        let value = value.trim();
        if value.is_empty() {
            return Err(RelayPeerSeedParseError::EmptyAddress);
        }

        let dial_addr = value
            .parse::<Multiaddr>()
            .map_err(|_| RelayPeerSeedParseError::InvalidAddress)?;
        if contains_relay_circuit(&dial_addr) {
            return Err(RelayPeerSeedParseError::CircuitAddress);
        }
        let Some(peer_id) = trailing_peer_id(&dial_addr) else {
            return Err(RelayPeerSeedParseError::MissingPeerId);
        };
        let routing_addr = routing_addr_without_trailing_peer(&dial_addr);

        Ok(Self {
            dial_addr,
            routing_addr,
            peer_id,
        })
    }

    pub fn dial_addr(&self) -> &Multiaddr {
        &self.dial_addr
    }

    pub fn routing_addr(&self) -> &Multiaddr {
        &self.routing_addr
    }

    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }
}

fn contains_relay_circuit(addr: &Multiaddr) -> bool {
    addr.iter()
        .any(|protocol| matches!(protocol, Protocol::P2pCircuit))
}

#[derive(Clone, PartialEq, Eq)]
pub enum RelayPeerSeedParseError {
    EmptyAddress,
    InvalidAddress,
    MissingPeerId,
    CircuitAddress,
}

impl fmt::Debug for RelayPeerSeedParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for RelayPeerSeedParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyAddress => write!(f, "relay peer address is empty"),
            Self::InvalidAddress => write!(f, "relay peer address is not a valid multiaddr"),
            Self::MissingPeerId => write!(f, "relay peer address must end with /p2p/<peer_id>"),
            Self::CircuitAddress => write!(
                f,
                "relay peer address must identify the relay node, not a p2p-circuit path"
            ),
        }
    }
}

impl Error for RelayPeerSeedParseError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NatRelayPolicy {
    mode: RelayPolicyMode,
    relay_peers: Vec<RelayPeerSeed>,
}

impl NatRelayPolicy {
    pub fn disabled() -> Self {
        Self {
            mode: RelayPolicyMode::Disabled,
            relay_peers: Vec::new(),
        }
    }

    pub fn operator_configured(relay_peers: Vec<RelayPeerSeed>) -> Self {
        Self {
            mode: RelayPolicyMode::OperatorConfigured,
            relay_peers,
        }
    }

    pub fn mode(&self) -> RelayPolicyMode {
        self.mode
    }

    pub fn relay_peers(&self) -> &[RelayPeerSeed] {
        &self.relay_peers
    }

    pub fn is_enabled(&self) -> bool {
        matches!(self.mode, RelayPolicyMode::OperatorConfigured)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum NatRelayArgError {
    MissingPolicy,
    InvalidPolicy { source: RelayPolicyModeParseError },
    TooManyPolicies,
    MissingRelayPeer,
    InvalidRelayPeer { source: RelayPeerSeedParseError },
    TooManyRelayPeers { max: usize },
    RelayPeersRequireOperatorPolicy,
}

impl fmt::Debug for NatRelayArgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for NatRelayArgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingPolicy => write!(f, "--relay-policy requires a value"),
            Self::InvalidPolicy { source } => write!(f, "invalid --relay-policy value: {}", source),
            Self::TooManyPolicies => write!(f, "--relay-policy may be configured only once"),
            Self::MissingRelayPeer => write!(f, "--relay-peer requires a multiaddr value"),
            Self::InvalidRelayPeer { source } => {
                write!(f, "invalid --relay-peer value: {}", source)
            }
            Self::TooManyRelayPeers { max } => {
                write!(f, "too many relay peers configured; maximum is {}", max)
            }
            Self::RelayPeersRequireOperatorPolicy => write!(
                f,
                "--relay-peer requires --relay-policy operator-configured"
            ),
        }
    }
}

impl Error for NatRelayArgError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidPolicy { source } => Some(source),
            Self::InvalidRelayPeer { source } => Some(source),
            _ => None,
        }
    }
}

pub fn nat_relay_policy_from_args(args: &[String]) -> Result<NatRelayPolicy, NatRelayArgError> {
    let policy_values =
        values_from_repeated_flag(args, RELAY_POLICY_FLAG).map_err(|error| match error {
            RepeatedFlagArgError::MissingValue => NatRelayArgError::MissingPolicy,
        })?;
    if policy_values.len() > 1 {
        return Err(NatRelayArgError::TooManyPolicies);
    }

    let mode = match policy_values.first() {
        Some(value) => RelayPolicyMode::parse(value)
            .map_err(|source| NatRelayArgError::InvalidPolicy { source })?,
        None => RelayPolicyMode::Disabled,
    };

    let relay_peer_values =
        values_from_repeated_flag(args, RELAY_PEER_FLAG).map_err(|error| match error {
            RepeatedFlagArgError::MissingValue => NatRelayArgError::MissingRelayPeer,
        })?;
    let relay_peers = relay_peers_from_values(relay_peer_values)?;

    if !relay_peers.is_empty() && !matches!(mode, RelayPolicyMode::OperatorConfigured) {
        return Err(NatRelayArgError::RelayPeersRequireOperatorPolicy);
    }

    Ok(match mode {
        RelayPolicyMode::Disabled => NatRelayPolicy::disabled(),
        RelayPolicyMode::OperatorConfigured => NatRelayPolicy::operator_configured(relay_peers),
    })
}

pub fn relay_peers_from_values<'a, I>(values: I) -> Result<Vec<RelayPeerSeed>, NatRelayArgError>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut seen = HashSet::new();
    let mut relay_peers = Vec::new();

    for value in values {
        if relay_peers.len() >= MAX_RELAY_PEERS {
            return Err(NatRelayArgError::TooManyRelayPeers {
                max: MAX_RELAY_PEERS,
            });
        }

        let peer = RelayPeerSeed::parse(value)
            .map_err(|source| NatRelayArgError::InvalidRelayPeer { source })?;
        if seen.insert(peer.peer_id()) {
            relay_peers.push(peer);
        }
    }

    Ok(relay_peers)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn relay_policy_defaults_to_disabled() {
        let result = nat_relay_policy_from_args(&args(&["iamine-node"]));
        let Ok(policy) = result else {
            assert!(result.is_ok(), "default policy should parse");
            return;
        };

        assert_eq!(policy.mode(), RelayPolicyMode::Disabled);
        assert!(policy.relay_peers().is_empty());
        assert!(!policy.is_enabled());
    }

    #[test]
    fn relay_policy_accepts_operator_configured_peers() {
        let relay_a = PeerId::random();
        let relay_b = PeerId::random();
        let addr_a = format!("/ip4/127.0.0.1/tcp/9101/p2p/{}", relay_a);
        let addr_b = format!("/dns4/relay.example.test/tcp/9102/p2p/{}", relay_b);

        let result = nat_relay_policy_from_args(&args(&[
            "iamine-node",
            "--relay-policy=operator-configured",
            "--relay-peer",
            &addr_a,
            &format!("--relay-peer={addr_b}"),
        ]));
        let Ok(policy) = result else {
            assert!(result.is_ok(), "operator relay policy should parse");
            return;
        };

        assert_eq!(policy.mode(), RelayPolicyMode::OperatorConfigured);
        assert_eq!(policy.relay_peers().len(), 2);
        assert_eq!(policy.relay_peers()[0].peer_id(), relay_a);
        assert_eq!(
            policy.relay_peers()[0].routing_addr().to_string(),
            "/ip4/127.0.0.1/tcp/9101"
        );
        assert_eq!(policy.relay_peers()[1].peer_id(), relay_b);
        assert!(policy.is_enabled());
    }

    #[test]
    fn relay_policy_rejects_unknown_mode() {
        let result =
            nat_relay_policy_from_args(&args(&["iamine-node", "--relay-policy=automatic"]));

        assert!(matches!(
            result,
            Err(NatRelayArgError::InvalidPolicy { .. })
        ));
    }

    #[test]
    fn relay_policy_accepts_disabled_modes() {
        for mode in ["disabled", "off", "none"] {
            let result = nat_relay_policy_from_args(&args(&[
                "iamine-node",
                &format!("--relay-policy={mode}"),
            ]));
            let Ok(policy) = result else {
                assert!(result.is_ok(), "disabled relay policy should parse");
                return;
            };

            assert_eq!(policy.mode(), RelayPolicyMode::Disabled);
            assert!(!policy.is_enabled());
        }
    }

    #[test]
    fn relay_policy_rejects_missing_values() {
        let missing_policy = nat_relay_policy_from_args(&args(&["iamine-node", "--relay-policy"]));
        let missing_peer = nat_relay_policy_from_args(&args(&[
            "iamine-node",
            "--relay-policy=operator-configured",
            "--relay-peer",
        ]));

        assert!(matches!(
            missing_policy,
            Err(NatRelayArgError::MissingPolicy)
        ));
        assert!(matches!(
            missing_peer,
            Err(NatRelayArgError::MissingRelayPeer)
        ));
    }

    #[test]
    fn relay_peer_requires_operator_policy() {
        let relay = PeerId::random();
        let addr = format!("/ip4/127.0.0.1/tcp/9101/p2p/{}", relay);
        let result = nat_relay_policy_from_args(&args(&["iamine-node", "--relay-peer", &addr]));

        assert!(matches!(
            result,
            Err(NatRelayArgError::RelayPeersRequireOperatorPolicy)
        ));
    }

    #[test]
    fn relay_peer_requires_peer_id() {
        let result = nat_relay_policy_from_args(&args(&[
            "iamine-node",
            "--relay-policy=operator-configured",
            "--relay-peer=/ip4/127.0.0.1/tcp/9101",
        ]));

        assert!(matches!(
            result,
            Err(NatRelayArgError::InvalidRelayPeer {
                source: RelayPeerSeedParseError::MissingPeerId
            })
        ));
    }

    #[test]
    fn relay_peer_rejects_p2p_circuit_destination_paths() {
        let relay = PeerId::random();
        let target = PeerId::random();
        let addr = format!(
            "/ip4/127.0.0.1/tcp/9101/p2p/{}/p2p-circuit/p2p/{}",
            relay, target
        );
        let result = nat_relay_policy_from_args(&args(&[
            "iamine-node",
            "--relay-policy=operator-configured",
            "--relay-peer",
            &addr,
        ]));

        assert!(matches!(
            result,
            Err(NatRelayArgError::InvalidRelayPeer {
                source: RelayPeerSeedParseError::CircuitAddress
            })
        ));
    }

    #[test]
    fn relay_peers_deduplicate_by_peer_id() {
        let relay = PeerId::random();
        let first = format!("/ip4/127.0.0.1/tcp/9101/p2p/{}", relay);
        let duplicate = format!("/ip4/127.0.0.1/tcp/9102/p2p/{}", relay);

        let result = relay_peers_from_values([first.as_str(), duplicate.as_str()]);
        let Ok(peers) = result else {
            assert!(result.is_ok(), "relay peers should parse");
            return;
        };

        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].dial_addr().to_string(), first);
    }

    #[test]
    fn relay_peer_set_is_bounded() {
        let values: Vec<String> = (0..=MAX_RELAY_PEERS)
            .map(|offset| {
                format!(
                    "/ip4/127.0.0.1/tcp/{}/p2p/{}",
                    9100 + offset,
                    PeerId::random()
                )
            })
            .collect();
        let result = relay_peers_from_values(values.iter().map(String::as_str));

        assert!(matches!(
            result,
            Err(NatRelayArgError::TooManyRelayPeers {
                max: MAX_RELAY_PEERS
            })
        ));
    }
}
