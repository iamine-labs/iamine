use crate::{log_observability_event, IamineBehaviour};
use iamine_network::{peer_protocol_decision, LogLevel, PeerProtocolDecision};
use libp2p::{identify, swarm::Swarm, PeerId};
use serde_json::Map;

pub(crate) fn handle_identify_event(swarm: &mut Swarm<IamineBehaviour>, event: identify::Event) {
    match event {
        identify::Event::Received { peer_id, info } => {
            let stream_protocols: Vec<String> =
                info.protocols.iter().map(ToString::to_string).collect();
            let decision = peer_protocol_decision(
                &info.protocol_version,
                stream_protocols.iter().map(String::as_str),
            );
            log_protocol_decision(&peer_id, &info.protocol_version, decision);
            if !decision.is_compatible() {
                let _ = swarm.disconnect_peer_id(peer_id);
            }
        }
        identify::Event::Error { peer_id, error } => {
            let mut fields = Map::new();
            fields.insert("peer_id".to_string(), peer_id.to_string().into());
            fields.insert("error".to_string(), error.to_string().into());
            log_observability_event(
                LogLevel::Warn,
                "p2p_protocol_identify_failed",
                "network",
                None,
                None,
                None,
                fields,
            );
        }
        identify::Event::Sent { .. } | identify::Event::Pushed { .. } => {}
    }
}

fn log_protocol_decision(
    peer_id: &PeerId,
    remote_identify_protocol: &str,
    decision: PeerProtocolDecision,
) {
    let mut fields = Map::new();
    fields.insert("peer_id".to_string(), peer_id.to_string().into());
    fields.insert(
        "remote_identify_protocol".to_string(),
        remote_identify_protocol.to_string().into(),
    );
    fields.insert("compatible".to_string(), decision.is_compatible().into());
    if let Some(reason_code) = decision.reason_code() {
        fields.insert("reason_code".to_string(), reason_code.into());
    }
    log_observability_event(
        if decision.is_compatible() {
            LogLevel::Info
        } else {
            LogLevel::Warn
        },
        "p2p_protocol_version_checked",
        "network",
        None,
        None,
        None,
        fields,
    );
}

#[cfg(test)]
mod tests {
    use iamine_network::{
        peer_protocol_decision, PeerProtocolDecision, IAMINE_IDENTIFY_PROTOCOL,
        IAMINE_RESULT_PROTOCOL, IAMINE_TASK_PROTOCOL,
    };

    #[test]
    fn p2p_protocol_decision_accepts_current_protocol_set() {
        let decision = peer_protocol_decision(
            IAMINE_IDENTIFY_PROTOCOL,
            [IAMINE_TASK_PROTOCOL, IAMINE_RESULT_PROTOCOL],
        );
        assert_eq!(decision, PeerProtocolDecision::Compatible);
    }

    #[test]
    fn p2p_protocol_decision_rejects_missing_result_stream() {
        let decision = peer_protocol_decision(IAMINE_IDENTIFY_PROTOCOL, [IAMINE_TASK_PROTOCOL]);
        assert_eq!(
            decision,
            PeerProtocolDecision::MissingRequiredStreamProtocol
        );
    }
}
