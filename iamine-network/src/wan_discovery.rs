use crate::arg_values::{values_from_repeated_flag, RepeatedFlagArgError};
use crate::bootnode::{routing_addr_without_trailing_peer, trailing_peer_id};
use libp2p::{Multiaddr, PeerId};
use std::collections::HashSet;
use std::error::Error;
use std::fmt;

pub const WAN_PEER_FLAG: &str = "--wan-peer";
pub const MAX_WAN_DISCOVERY_PEERS: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WanPeerSeed {
    dial_addr: Multiaddr,
    routing_addr: Multiaddr,
    peer_id: PeerId,
}

impl WanPeerSeed {
    pub fn parse(value: &str) -> Result<Self, WanPeerSeedParseError> {
        let value = value.trim();
        if value.is_empty() {
            return Err(WanPeerSeedParseError::EmptyAddress);
        }

        let dial_addr = value
            .parse::<Multiaddr>()
            .map_err(|_| WanPeerSeedParseError::InvalidAddress)?;
        let Some(peer_id) = trailing_peer_id(&dial_addr) else {
            return Err(WanPeerSeedParseError::MissingPeerId);
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

#[derive(Clone, PartialEq, Eq)]
pub enum WanPeerSeedParseError {
    EmptyAddress,
    InvalidAddress,
    MissingPeerId,
}

impl fmt::Debug for WanPeerSeedParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for WanPeerSeedParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyAddress => write!(f, "WAN peer address is empty"),
            Self::InvalidAddress => write!(f, "WAN peer address is not a valid multiaddr"),
            Self::MissingPeerId => write!(f, "WAN peer address must end with /p2p/<peer_id>"),
        }
    }
}

impl Error for WanPeerSeedParseError {}

#[derive(Clone, PartialEq, Eq)]
pub enum WanPeerArgError {
    MissingAddress,
    InvalidAddress { source: WanPeerSeedParseError },
    TooManyPeers { max: usize },
}

impl fmt::Debug for WanPeerArgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for WanPeerArgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingAddress => write!(f, "--wan-peer requires a multiaddr value"),
            Self::InvalidAddress { source } => write!(f, "invalid --wan-peer value: {}", source),
            Self::TooManyPeers { max } => {
                write!(f, "too many WAN peers configured; maximum is {}", max)
            }
        }
    }
}

impl Error for WanPeerArgError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidAddress { source } => Some(source),
            _ => None,
        }
    }
}

pub fn wan_peer_seeds_from_args(args: &[String]) -> Result<Vec<WanPeerSeed>, WanPeerArgError> {
    let values = values_from_repeated_flag(args, WAN_PEER_FLAG).map_err(|error| match error {
        RepeatedFlagArgError::MissingValue => WanPeerArgError::MissingAddress,
    })?;

    wan_peer_seeds_from_values(values)
}

pub fn wan_peer_seeds_from_values<'a, I>(values: I) -> Result<Vec<WanPeerSeed>, WanPeerArgError>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut seen = HashSet::new();
    let mut seeds = Vec::new();

    for value in values {
        if seeds.len() >= MAX_WAN_DISCOVERY_PEERS {
            return Err(WanPeerArgError::TooManyPeers {
                max: MAX_WAN_DISCOVERY_PEERS,
            });
        }

        let seed = WanPeerSeed::parse(value)
            .map_err(|source| WanPeerArgError::InvalidAddress { source })?;
        if seen.insert(seed.peer_id()) {
            seeds.push(seed);
        }
    }

    Ok(seeds)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn wan_peer_args_accept_repeated_flag_forms() {
        let peer_a = PeerId::random();
        let peer_b = PeerId::random();
        let addr_a = format!("/ip4/127.0.0.1/tcp/9001/p2p/{}", peer_a);
        let addr_b = format!("/dns4/example.test/tcp/9002/p2p/{}", peer_b);

        let seeds = wan_peer_seeds_from_args(&args(&[
            "iamine-node",
            "--wan-peer",
            &addr_a,
            &format!("--wan-peer={addr_b}"),
        ]))
        .expect("WAN peers should parse");

        assert_eq!(seeds.len(), 2);
        assert_eq!(seeds[0].peer_id(), peer_a);
        assert_eq!(
            seeds[0].routing_addr().to_string(),
            "/ip4/127.0.0.1/tcp/9001"
        );
        assert_eq!(seeds[1].peer_id(), peer_b);
        assert_eq!(
            seeds[1].routing_addr().to_string(),
            "/dns4/example.test/tcp/9002"
        );
    }

    #[test]
    fn wan_peer_args_reject_missing_value() {
        let result = wan_peer_seeds_from_args(&args(&["iamine-node", "--wan-peer"]));

        assert!(matches!(result, Err(WanPeerArgError::MissingAddress)));
    }

    #[test]
    fn wan_peer_args_reject_invalid_multiaddr() {
        let result = wan_peer_seeds_from_args(&args(&["iamine-node", "--wan-peer=bad"]));

        assert!(matches!(
            result,
            Err(WanPeerArgError::InvalidAddress {
                source: WanPeerSeedParseError::InvalidAddress
            })
        ));
    }

    #[test]
    fn wan_peer_args_require_peer_id() {
        let result = wan_peer_seeds_from_args(&args(&[
            "iamine-node",
            "--wan-peer=/ip4/127.0.0.1/tcp/9001",
        ]));

        assert!(matches!(
            result,
            Err(WanPeerArgError::InvalidAddress {
                source: WanPeerSeedParseError::MissingPeerId
            })
        ));
    }

    #[test]
    fn wan_peer_values_deduplicate_by_peer_id() {
        let peer = PeerId::random();
        let first = format!("/ip4/127.0.0.1/tcp/9001/p2p/{}", peer);
        let duplicate = format!("/ip4/127.0.0.1/tcp/9002/p2p/{}", peer);
        let seeds = wan_peer_seeds_from_values([first.as_str(), duplicate.as_str()])
            .expect("WAN peers should parse");

        assert_eq!(seeds.len(), 1);
        assert_eq!(seeds[0].dial_addr().to_string(), first);
    }

    #[test]
    fn wan_peer_values_are_bounded() {
        let values: Vec<String> = (0..=MAX_WAN_DISCOVERY_PEERS)
            .map(|offset| {
                format!(
                    "/ip4/127.0.0.1/tcp/{}/p2p/{}",
                    9000 + offset,
                    PeerId::random()
                )
            })
            .collect();
        let result = wan_peer_seeds_from_values(values.iter().map(String::as_str));

        assert!(matches!(
            result,
            Err(WanPeerArgError::TooManyPeers {
                max: MAX_WAN_DISCOVERY_PEERS
            })
        ));
    }
}
