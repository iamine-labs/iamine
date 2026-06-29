pub const IAMINE_PROTOCOL_VERSION: &str = "1.0";
pub const IAMINE_IDENTIFY_PROTOCOL: &str = "/iamine/1.0";
pub const IAMINE_TASK_PROTOCOL: &str = "/iamine/task/1.0";
pub const IAMINE_RESULT_PROTOCOL: &str = "/iamine/result/1.0";

pub const REQUIRED_STREAM_PROTOCOLS: [&str; 2] = [IAMINE_TASK_PROTOCOL, IAMINE_RESULT_PROTOCOL];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerProtocolDecision {
    Compatible,
    MissingIdentifyProtocol,
    UnsupportedIdentifyProtocol,
    MissingRequiredStreamProtocol,
}

impl PeerProtocolDecision {
    pub fn is_compatible(self) -> bool {
        matches!(self, Self::Compatible)
    }

    pub fn reason_code(self) -> Option<&'static str> {
        match self {
            Self::Compatible => None,
            Self::MissingIdentifyProtocol => Some("missing_identify_protocol"),
            Self::UnsupportedIdentifyProtocol => Some("unsupported_identify_protocol"),
            Self::MissingRequiredStreamProtocol => Some("missing_required_stream_protocol"),
        }
    }
}

pub fn peer_protocol_decision<'a, I>(
    remote_identify_protocol: &str,
    remote_stream_protocols: I,
) -> PeerProtocolDecision
where
    I: IntoIterator<Item = &'a str>,
{
    let remote_identify_protocol = remote_identify_protocol.trim();
    if remote_identify_protocol.is_empty() {
        return PeerProtocolDecision::MissingIdentifyProtocol;
    }
    if remote_identify_protocol != IAMINE_IDENTIFY_PROTOCOL {
        return PeerProtocolDecision::UnsupportedIdentifyProtocol;
    }

    let supported_protocols: Vec<&str> = remote_stream_protocols.into_iter().collect();
    if REQUIRED_STREAM_PROTOCOLS.iter().all(|required| {
        supported_protocols
            .iter()
            .any(|supported| supported == required)
    }) {
        PeerProtocolDecision::Compatible
    } else {
        PeerProtocolDecision::MissingRequiredStreamProtocol
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_constants_preserve_current_wire_contract() {
        assert_eq!(IAMINE_PROTOCOL_VERSION, "1.0");
        assert_eq!(IAMINE_IDENTIFY_PROTOCOL, "/iamine/1.0");
        assert_eq!(IAMINE_TASK_PROTOCOL, "/iamine/task/1.0");
        assert_eq!(IAMINE_RESULT_PROTOCOL, "/iamine/result/1.0");
    }

    #[test]
    fn compatible_peer_requires_identify_and_stream_protocols() {
        let decision = peer_protocol_decision(
            IAMINE_IDENTIFY_PROTOCOL,
            [IAMINE_TASK_PROTOCOL, IAMINE_RESULT_PROTOCOL],
        );
        assert_eq!(decision, PeerProtocolDecision::Compatible);
        assert!(decision.is_compatible());
    }

    #[test]
    fn missing_identify_protocol_is_rejected_explicitly() {
        let decision = peer_protocol_decision("", [IAMINE_TASK_PROTOCOL, IAMINE_RESULT_PROTOCOL]);
        assert_eq!(decision, PeerProtocolDecision::MissingIdentifyProtocol);
        assert_eq!(decision.reason_code(), Some("missing_identify_protocol"));
    }

    #[test]
    fn unsupported_identify_protocol_is_rejected_explicitly() {
        let decision = peer_protocol_decision(
            "/iamine/2.0",
            [IAMINE_TASK_PROTOCOL, IAMINE_RESULT_PROTOCOL],
        );
        assert_eq!(decision, PeerProtocolDecision::UnsupportedIdentifyProtocol);
        assert_eq!(
            decision.reason_code(),
            Some("unsupported_identify_protocol")
        );
    }

    #[test]
    fn missing_required_stream_protocol_is_rejected_explicitly() {
        let decision = peer_protocol_decision(IAMINE_IDENTIFY_PROTOCOL, [IAMINE_TASK_PROTOCOL]);
        assert_eq!(
            decision,
            PeerProtocolDecision::MissingRequiredStreamProtocol
        );
        assert_eq!(
            decision.reason_code(),
            Some("missing_required_stream_protocol")
        );
    }
}
