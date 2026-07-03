use libp2p::{multiaddr::Protocol, Multiaddr, PeerId};
use std::collections::HashSet;
use std::error::Error;
use std::fmt;

pub const BOOTNODE_FLAG: &str = "--bootnode";
pub const MAX_BOOTNODE_ADDRESSES: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bootnode {
    dial_addr: Multiaddr,
    routing_addr: Multiaddr,
    peer_id: Option<PeerId>,
}

impl Bootnode {
    pub fn parse(value: &str) -> Result<Self, BootnodeParseError> {
        let value = value.trim();
        if value.is_empty() {
            return Err(BootnodeParseError::EmptyAddress);
        }

        let dial_addr = value
            .parse::<Multiaddr>()
            .map_err(|_| BootnodeParseError::InvalidAddress)?;
        let peer_id = trailing_peer_id(&dial_addr);
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

    pub fn peer_id(&self) -> Option<PeerId> {
        self.peer_id
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum BootnodeParseError {
    EmptyAddress,
    InvalidAddress,
}

impl fmt::Debug for BootnodeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for BootnodeParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyAddress => write!(f, "bootnode address is empty"),
            Self::InvalidAddress => write!(f, "bootnode address is not a valid multiaddr"),
        }
    }
}

impl Error for BootnodeParseError {}

#[derive(Clone, PartialEq, Eq)]
pub enum BootnodeArgError {
    MissingAddress,
    InvalidAddress { source: BootnodeParseError },
    TooManyAddresses { max: usize },
}

impl fmt::Debug for BootnodeArgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for BootnodeArgError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingAddress => write!(f, "--bootnode requires a multiaddr value"),
            Self::InvalidAddress { source } => write!(f, "invalid --bootnode value: {}", source),
            Self::TooManyAddresses { max } => {
                write!(f, "too many bootnodes configured; maximum is {}", max)
            }
        }
    }
}

impl Error for BootnodeArgError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidAddress { source } => Some(source),
            _ => None,
        }
    }
}

pub fn bootnodes_from_args(args: &[String]) -> Result<Vec<Bootnode>, BootnodeArgError> {
    let mut values = Vec::new();
    let mut index = 0;

    while index < args.len() {
        let arg = &args[index];
        if arg == BOOTNODE_FLAG {
            let Some(value) = args.get(index + 1) else {
                return Err(BootnodeArgError::MissingAddress);
            };
            if value.starts_with("--") {
                return Err(BootnodeArgError::MissingAddress);
            }
            values.push(value.as_str());
            index += 2;
            continue;
        }

        if let Some(value) = arg.strip_prefix("--bootnode=") {
            values.push(value);
        }
        index += 1;
    }

    bootnodes_from_values(values)
}

pub fn bootnodes_from_values<'a, I>(values: I) -> Result<Vec<Bootnode>, BootnodeArgError>
where
    I: IntoIterator<Item = &'a str>,
{
    let mut seen = HashSet::new();
    let mut bootnodes = Vec::new();

    for value in values {
        if bootnodes.len() >= MAX_BOOTNODE_ADDRESSES {
            return Err(BootnodeArgError::TooManyAddresses {
                max: MAX_BOOTNODE_ADDRESSES,
            });
        }

        let bootnode =
            Bootnode::parse(value).map_err(|source| BootnodeArgError::InvalidAddress { source })?;
        if seen.insert(bootnode.dial_addr().to_string()) {
            bootnodes.push(bootnode);
        }
    }

    Ok(bootnodes)
}

fn trailing_peer_id(addr: &Multiaddr) -> Option<PeerId> {
    match addr.iter().last() {
        Some(Protocol::P2p(peer_id)) => Some(peer_id),
        _ => None,
    }
}

fn routing_addr_without_trailing_peer(addr: &Multiaddr) -> Multiaddr {
    let protocols: Vec<_> = addr.iter().collect();
    let strip_peer = matches!(protocols.last(), Some(Protocol::P2p(_)));
    if !strip_peer {
        return addr.clone();
    }

    let keep_count = protocols.len().saturating_sub(1);
    let mut routing_addr = Multiaddr::empty();
    for protocol in protocols.into_iter().take(keep_count) {
        routing_addr.push(protocol);
    }
    routing_addr
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| value.to_string()).collect()
    }

    #[test]
    fn bootnodes_from_args_accepts_repeated_flag_forms() {
        let peer_id = PeerId::random();
        let peer_addr = format!("/ip4/127.0.0.1/tcp/9999/p2p/{}", peer_id);
        let result = bootnodes_from_args(&args(&[
            "iamine-node",
            "--bootnode=/ip4/127.0.0.1/tcp/9001",
            "--bootnode",
            &peer_addr,
        ]));
        assert!(result.is_ok(), "bootnodes should parse: {result:?}");
        let bootnodes: Vec<Bootnode> = result.ok().into_iter().flatten().collect();

        assert_eq!(bootnodes.len(), 2);
        assert_eq!(
            bootnodes[0].dial_addr().to_string(),
            "/ip4/127.0.0.1/tcp/9001"
        );
        assert_eq!(bootnodes[1].peer_id(), Some(peer_id));
        assert_eq!(
            bootnodes[1].routing_addr().to_string(),
            "/ip4/127.0.0.1/tcp/9999"
        );
    }

    #[test]
    fn bootnodes_from_args_rejects_missing_value() {
        let result = bootnodes_from_args(&args(&["iamine-node", "--bootnode"]));

        assert!(matches!(result, Err(BootnodeArgError::MissingAddress)));
    }

    #[test]
    fn bootnodes_from_args_rejects_invalid_multiaddr() {
        let result = bootnodes_from_args(&args(&["iamine-node", "--bootnode=bad"]));

        assert!(matches!(
            result,
            Err(BootnodeArgError::InvalidAddress {
                source: BootnodeParseError::InvalidAddress
            })
        ));
    }

    #[test]
    fn bootnodes_from_values_deduplicates_addresses() {
        let result = bootnodes_from_values(["/ip4/127.0.0.1/tcp/9001", "/ip4/127.0.0.1/tcp/9001"]);
        assert!(result.is_ok(), "bootnodes should parse: {result:?}");
        let bootnodes: Vec<Bootnode> = result.ok().into_iter().flatten().collect();

        assert_eq!(bootnodes.len(), 1);
    }

    #[test]
    fn bootnodes_from_values_is_bounded() {
        let values: Vec<String> = (0..=MAX_BOOTNODE_ADDRESSES)
            .map(|offset| format!("/ip4/127.0.0.1/tcp/{}", 9000 + offset))
            .collect();
        let result = bootnodes_from_values(values.iter().map(String::as_str));

        assert!(matches!(
            result,
            Err(BootnodeArgError::TooManyAddresses {
                max: MAX_BOOTNODE_ADDRESSES
            })
        ));
    }
}
