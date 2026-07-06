pub const IAMINE_SECURE_TRANSPORT_POLICY: &str = "tcp-noise-yamux-v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum P2pBaseTransport {
    Tcp,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum P2pSecurityProtocol {
    Noise,
    Plaintext,
    Unauthenticated,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum P2pMultiplexer {
    Yamux,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum P2pUpgradeVersion {
    V1,
    Unsupported,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SecureTransportProfile {
    pub base_transport: P2pBaseTransport,
    pub security_protocol: P2pSecurityProtocol,
    pub multiplexer: P2pMultiplexer,
    pub upgrade_version: P2pUpgradeVersion,
}

impl SecureTransportProfile {
    pub const fn current() -> Self {
        Self {
            base_transport: P2pBaseTransport::Tcp,
            security_protocol: P2pSecurityProtocol::Noise,
            multiplexer: P2pMultiplexer::Yamux,
            upgrade_version: P2pUpgradeVersion::V1,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecureTransportDecision {
    Allowed,
    UnsupportedBaseTransport,
    PlaintextTransportRejected,
    UnauthenticatedTransportRejected,
    UnsupportedSecurityProtocol,
    UnsupportedMultiplexer,
    UnsupportedUpgradeVersion,
}

impl SecureTransportDecision {
    pub fn is_allowed(self) -> bool {
        matches!(self, Self::Allowed)
    }

    pub fn reason_code(self) -> Option<&'static str> {
        match self {
            Self::Allowed => None,
            Self::UnsupportedBaseTransport => Some("unsupported_base_transport"),
            Self::PlaintextTransportRejected => Some("plaintext_transport_rejected"),
            Self::UnauthenticatedTransportRejected => Some("unauthenticated_transport_rejected"),
            Self::UnsupportedSecurityProtocol => Some("unsupported_security_protocol"),
            Self::UnsupportedMultiplexer => Some("unsupported_multiplexer"),
            Self::UnsupportedUpgradeVersion => Some("unsupported_upgrade_version"),
        }
    }
}

pub fn current_secure_transport_profile() -> SecureTransportProfile {
    SecureTransportProfile::current()
}

pub fn secure_transport_decision(profile: &SecureTransportProfile) -> SecureTransportDecision {
    if profile.base_transport != P2pBaseTransport::Tcp {
        return SecureTransportDecision::UnsupportedBaseTransport;
    }

    match profile.security_protocol {
        P2pSecurityProtocol::Noise => {}
        P2pSecurityProtocol::Plaintext => {
            return SecureTransportDecision::PlaintextTransportRejected;
        }
        P2pSecurityProtocol::Unauthenticated => {
            return SecureTransportDecision::UnauthenticatedTransportRejected;
        }
        P2pSecurityProtocol::Unsupported => {
            return SecureTransportDecision::UnsupportedSecurityProtocol;
        }
    }

    if profile.multiplexer != P2pMultiplexer::Yamux {
        return SecureTransportDecision::UnsupportedMultiplexer;
    }

    if profile.upgrade_version != P2pUpgradeVersion::V1 {
        return SecureTransportDecision::UnsupportedUpgradeVersion;
    }

    SecureTransportDecision::Allowed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_secure_transport_profile_is_allowed() {
        let profile = current_secure_transport_profile();
        let decision = secure_transport_decision(&profile);

        assert_eq!(IAMINE_SECURE_TRANSPORT_POLICY, "tcp-noise-yamux-v1");
        assert_eq!(decision, SecureTransportDecision::Allowed);
        assert!(decision.is_allowed());
    }

    #[test]
    fn plaintext_transport_is_rejected_as_downgrade() {
        let profile = SecureTransportProfile {
            security_protocol: P2pSecurityProtocol::Plaintext,
            ..SecureTransportProfile::current()
        };

        let decision = secure_transport_decision(&profile);

        assert_eq!(
            decision,
            SecureTransportDecision::PlaintextTransportRejected
        );
        assert_eq!(decision.reason_code(), Some("plaintext_transport_rejected"));
    }

    #[test]
    fn unauthenticated_transport_is_rejected_as_downgrade() {
        let profile = SecureTransportProfile {
            security_protocol: P2pSecurityProtocol::Unauthenticated,
            ..SecureTransportProfile::current()
        };

        let decision = secure_transport_decision(&profile);

        assert_eq!(
            decision,
            SecureTransportDecision::UnauthenticatedTransportRejected
        );
        assert_eq!(
            decision.reason_code(),
            Some("unauthenticated_transport_rejected")
        );
    }

    #[test]
    fn unsupported_muxer_is_rejected() {
        let profile = SecureTransportProfile {
            multiplexer: P2pMultiplexer::Unsupported,
            ..SecureTransportProfile::current()
        };

        let decision = secure_transport_decision(&profile);

        assert_eq!(decision, SecureTransportDecision::UnsupportedMultiplexer);
        assert_eq!(decision.reason_code(), Some("unsupported_multiplexer"));
    }
}
