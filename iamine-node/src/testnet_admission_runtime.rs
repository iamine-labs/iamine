use crate::{log_observability_event, IamineBehaviour};
use iamine_network::{LogLevel, TestnetAdmissionPolicy};
use libp2p::{swarm::Swarm, PeerId};
use serde_json::Map;

pub(crate) fn should_accept_runtime_peer(
    peer_id: &PeerId,
    policy: &TestnetAdmissionPolicy,
    source: &'static str,
) -> bool {
    if policy.allows_peer(peer_id) {
        return true;
    }

    log_testnet_admission_rejection(peer_id, source, "ignored");
    false
}

pub(crate) fn enforce_runtime_peer_admission(
    swarm: &mut Swarm<IamineBehaviour>,
    peer_id: &PeerId,
    policy: &TestnetAdmissionPolicy,
    source: &'static str,
) -> bool {
    if should_accept_runtime_peer(peer_id, policy, source) {
        return true;
    }

    let _ = swarm.disconnect_peer_id(*peer_id);
    false
}

fn log_testnet_admission_rejection(peer_id: &PeerId, source: &'static str, action: &'static str) {
    let mut fields = Map::new();
    fields.insert("peer_id".to_string(), peer_id.to_string().into());
    fields.insert("source".to_string(), source.into());
    fields.insert("policy".to_string(), "testnet_admission_allowlist".into());
    fields.insert("action".to_string(), action.into());
    log_observability_event(
        LogLevel::Warn,
        "testnet_admission_peer_rejected",
        "network",
        None,
        None,
        None,
        fields,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_peer_acceptance_preserves_open_policy() {
        let policy = TestnetAdmissionPolicy::open();

        assert!(should_accept_runtime_peer(
            &PeerId::random(),
            &policy,
            "test"
        ));
    }

    #[test]
    fn runtime_peer_acceptance_rejects_peers_outside_allowlist() {
        let allowed = PeerId::random();
        let denied = PeerId::random();
        let policy = TestnetAdmissionPolicy::allowlist(vec![allowed]);

        assert!(should_accept_runtime_peer(&allowed, &policy, "test"));
        assert!(!should_accept_runtime_peer(&denied, &policy, "test"));
    }
}
