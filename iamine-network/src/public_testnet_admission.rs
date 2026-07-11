use libp2p::PeerId;
use std::collections::HashSet;
use std::error::Error;
use std::fmt;

pub const DEFAULT_PUBLIC_TESTNET_MAX_NODES_PER_OPERATOR: usize = 1;
pub const MAX_PUBLIC_TESTNET_ADMITTED_PEERS: usize = 256;
pub const MAX_PUBLIC_TESTNET_REMOVED_PEERS: usize = 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicTestnetAdmissionMode {
    Closed,
    Controlled,
}

impl PublicTestnetAdmissionMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Controlled => "controlled",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicTestnetAbuseControls {
    require_identity_registration: bool,
    require_secure_transport: bool,
}

impl PublicTestnetAbuseControls {
    pub fn strict() -> Self {
        Self {
            require_identity_registration: true,
            require_secure_transport: true,
        }
    }

    pub fn requires_identity_registration(&self) -> bool {
        self.require_identity_registration
    }

    pub fn requires_secure_transport(&self) -> bool {
        self.require_secure_transport
    }
}

impl Default for PublicTestnetAbuseControls {
    fn default() -> Self {
        Self::strict()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicTestnetAdmissionPolicy {
    mode: PublicTestnetAdmissionMode,
    admitted_peers: Vec<PeerId>,
    removed_peers: Vec<PeerId>,
    max_nodes_per_operator: usize,
    abuse_controls: PublicTestnetAbuseControls,
}

impl PublicTestnetAdmissionPolicy {
    pub fn closed() -> Self {
        Self {
            mode: PublicTestnetAdmissionMode::Closed,
            admitted_peers: Vec::new(),
            removed_peers: Vec::new(),
            max_nodes_per_operator: DEFAULT_PUBLIC_TESTNET_MAX_NODES_PER_OPERATOR,
            abuse_controls: PublicTestnetAbuseControls::strict(),
        }
    }

    pub fn controlled(
        admitted_peers: Vec<PeerId>,
    ) -> Result<Self, PublicTestnetAdmissionPolicyError> {
        Self::controlled_with_controls(
            admitted_peers,
            Vec::new(),
            DEFAULT_PUBLIC_TESTNET_MAX_NODES_PER_OPERATOR,
            PublicTestnetAbuseControls::strict(),
        )
    }

    pub fn controlled_with_controls(
        admitted_peers: Vec<PeerId>,
        removed_peers: Vec<PeerId>,
        max_nodes_per_operator: usize,
        abuse_controls: PublicTestnetAbuseControls,
    ) -> Result<Self, PublicTestnetAdmissionPolicyError> {
        if max_nodes_per_operator == 0 {
            return Err(PublicTestnetAdmissionPolicyError::MaxNodesPerOperatorMustBeNonZero);
        }

        let admitted_peers = dedupe_peers(admitted_peers, MAX_PUBLIC_TESTNET_ADMITTED_PEERS)
            .map_err(
                |_| PublicTestnetAdmissionPolicyError::TooManyAdmittedPeers {
                    max: MAX_PUBLIC_TESTNET_ADMITTED_PEERS,
                },
            )?;
        if admitted_peers.is_empty() {
            return Err(PublicTestnetAdmissionPolicyError::ControlledRequiresAdmittedPeer);
        }

        let removed_peers =
            dedupe_peers(removed_peers, MAX_PUBLIC_TESTNET_REMOVED_PEERS).map_err(|_| {
                PublicTestnetAdmissionPolicyError::TooManyRemovedPeers {
                    max: MAX_PUBLIC_TESTNET_REMOVED_PEERS,
                }
            })?;

        Ok(Self {
            mode: PublicTestnetAdmissionMode::Controlled,
            admitted_peers,
            removed_peers,
            max_nodes_per_operator,
            abuse_controls,
        })
    }

    pub fn mode(&self) -> PublicTestnetAdmissionMode {
        self.mode
    }

    pub fn mode_name(&self) -> &'static str {
        self.mode.as_str()
    }

    pub fn admitted_peers(&self) -> &[PeerId] {
        &self.admitted_peers
    }

    pub fn removed_peers(&self) -> &[PeerId] {
        &self.removed_peers
    }

    pub fn admitted_peer_count(&self) -> usize {
        self.admitted_peers.len()
    }

    pub fn removed_peer_count(&self) -> usize {
        self.removed_peers.len()
    }

    pub fn max_nodes_per_operator(&self) -> usize {
        self.max_nodes_per_operator
    }

    pub fn abuse_controls(&self) -> PublicTestnetAbuseControls {
        self.abuse_controls
    }

    pub fn evaluate(
        &self,
        candidate: &PublicTestnetAdmissionCandidate,
    ) -> PublicTestnetAdmissionDecision {
        if matches!(self.mode, PublicTestnetAdmissionMode::Closed) {
            return PublicTestnetAdmissionDecision::rejected(
                PublicTestnetAdmissionReason::PublicTestnetClosed,
            );
        }

        if self.removed_peers.contains(&candidate.peer_id) {
            return PublicTestnetAdmissionDecision::rejected(
                PublicTestnetAdmissionReason::PeerRemoved,
            );
        }

        if !self.admitted_peers.contains(&candidate.peer_id) {
            return PublicTestnetAdmissionDecision::rejected(
                PublicTestnetAdmissionReason::PeerNotAdmitted,
            );
        }

        if candidate.operator_node_count >= self.max_nodes_per_operator {
            return PublicTestnetAdmissionDecision::rejected(
                PublicTestnetAdmissionReason::OperatorNodeLimitExceeded,
            );
        }

        if self.abuse_controls.require_identity_registration && !candidate.identity_registered {
            return PublicTestnetAdmissionDecision::rejected(
                PublicTestnetAdmissionReason::IdentityRegistrationRequired,
            );
        }

        if self.abuse_controls.require_secure_transport && !candidate.secure_transport_authenticated
        {
            return PublicTestnetAdmissionDecision::rejected(
                PublicTestnetAdmissionReason::SecureTransportRequired,
            );
        }

        PublicTestnetAdmissionDecision::accepted()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicTestnetAdmissionCandidate {
    peer_id: PeerId,
    operator_node_count: usize,
    identity_registered: bool,
    secure_transport_authenticated: bool,
}

impl PublicTestnetAdmissionCandidate {
    pub fn new(peer_id: PeerId) -> Self {
        Self {
            peer_id,
            operator_node_count: 0,
            identity_registered: false,
            secure_transport_authenticated: false,
        }
    }

    pub fn with_operator_node_count(mut self, operator_node_count: usize) -> Self {
        self.operator_node_count = operator_node_count;
        self
    }

    pub fn with_identity_registered(mut self, identity_registered: bool) -> Self {
        self.identity_registered = identity_registered;
        self
    }

    pub fn with_secure_transport_authenticated(
        mut self,
        secure_transport_authenticated: bool,
    ) -> Self {
        self.secure_transport_authenticated = secure_transport_authenticated;
        self
    }

    pub fn peer_id(&self) -> &PeerId {
        &self.peer_id
    }

    pub fn operator_node_count(&self) -> usize {
        self.operator_node_count
    }

    pub fn identity_registered(&self) -> bool {
        self.identity_registered
    }

    pub fn secure_transport_authenticated(&self) -> bool {
        self.secure_transport_authenticated
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicTestnetAdmissionDecision {
    admitted: bool,
    reason: PublicTestnetAdmissionReason,
}

impl PublicTestnetAdmissionDecision {
    pub fn accepted() -> Self {
        Self {
            admitted: true,
            reason: PublicTestnetAdmissionReason::Accepted,
        }
    }

    pub fn rejected(reason: PublicTestnetAdmissionReason) -> Self {
        Self {
            admitted: false,
            reason,
        }
    }

    pub fn is_admitted(&self) -> bool {
        self.admitted
    }

    pub fn reason(&self) -> PublicTestnetAdmissionReason {
        self.reason
    }

    pub fn reason_code(&self) -> &'static str {
        self.reason.as_str()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublicTestnetAdmissionReason {
    Accepted,
    PublicTestnetClosed,
    PeerNotAdmitted,
    PeerRemoved,
    OperatorNodeLimitExceeded,
    IdentityRegistrationRequired,
    SecureTransportRequired,
}

impl PublicTestnetAdmissionReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::PublicTestnetClosed => "public_testnet_closed",
            Self::PeerNotAdmitted => "peer_not_admitted",
            Self::PeerRemoved => "peer_removed",
            Self::OperatorNodeLimitExceeded => "operator_node_limit_exceeded",
            Self::IdentityRegistrationRequired => "identity_registration_required",
            Self::SecureTransportRequired => "secure_transport_required",
        }
    }
}

#[derive(Clone, PartialEq, Eq)]
pub enum PublicTestnetAdmissionPolicyError {
    ControlledRequiresAdmittedPeer,
    TooManyAdmittedPeers { max: usize },
    TooManyRemovedPeers { max: usize },
    MaxNodesPerOperatorMustBeNonZero,
}

impl fmt::Debug for PublicTestnetAdmissionPolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for PublicTestnetAdmissionPolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ControlledRequiresAdmittedPeer => {
                write!(
                    f,
                    "controlled public testnet admission requires an admitted peer"
                )
            }
            Self::TooManyAdmittedPeers { max } => {
                write!(
                    f,
                    "too many public testnet admitted peers; maximum is {max}"
                )
            }
            Self::TooManyRemovedPeers { max } => {
                write!(f, "too many public testnet removed peers; maximum is {max}")
            }
            Self::MaxNodesPerOperatorMustBeNonZero => {
                write!(f, "max public testnet nodes per operator must be nonzero")
            }
        }
    }
}

impl Error for PublicTestnetAdmissionPolicyError {}

fn dedupe_peers(
    peers: Vec<PeerId>,
    max: usize,
) -> Result<Vec<PeerId>, PublicTestnetAdmissionPolicyError> {
    if peers.len() > max {
        return Err(PublicTestnetAdmissionPolicyError::TooManyAdmittedPeers { max });
    }

    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for peer in peers {
        if seen.insert(peer) {
            deduped.push(peer);
        }
    }

    Ok(deduped)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn admitted_candidate(peer: PeerId) -> PublicTestnetAdmissionCandidate {
        PublicTestnetAdmissionCandidate::new(peer)
            .with_identity_registered(true)
            .with_secure_transport_authenticated(true)
    }

    fn peer_set(count: usize) -> Vec<PeerId> {
        (0..count).map(|_| PeerId::random()).collect()
    }

    #[test]
    fn public_testnet_admission_defaults_closed() {
        let policy = PublicTestnetAdmissionPolicy::closed();
        let candidate = admitted_candidate(PeerId::random());
        let decision = policy.evaluate(&candidate);

        assert_eq!(policy.mode(), PublicTestnetAdmissionMode::Closed);
        assert!(!decision.is_admitted());
        assert_eq!(
            decision.reason(),
            PublicTestnetAdmissionReason::PublicTestnetClosed
        );
    }

    #[test]
    fn public_testnet_controlled_policy_requires_admitted_peer() {
        let result = PublicTestnetAdmissionPolicy::controlled(Vec::new());

        assert!(matches!(
            result,
            Err(PublicTestnetAdmissionPolicyError::ControlledRequiresAdmittedPeer)
        ));
    }

    #[test]
    fn public_testnet_controlled_policy_admits_matching_candidate() {
        let peer = PeerId::random();
        let result = PublicTestnetAdmissionPolicy::controlled(vec![peer]);
        let Ok(policy) = result else {
            assert!(result.is_ok(), "controlled policy should parse");
            return;
        };
        let decision = policy.evaluate(&admitted_candidate(peer));

        assert!(decision.is_admitted());
        assert_eq!(decision.reason_code(), "accepted");
    }

    #[test]
    fn public_testnet_removal_overrides_admission() {
        let peer = PeerId::random();
        let result = PublicTestnetAdmissionPolicy::controlled_with_controls(
            vec![peer],
            vec![peer],
            1,
            PublicTestnetAbuseControls::strict(),
        );
        let Ok(policy) = result else {
            assert!(result.is_ok(), "controlled policy should parse");
            return;
        };
        let decision = policy.evaluate(&admitted_candidate(peer));

        assert!(!decision.is_admitted());
        assert_eq!(decision.reason(), PublicTestnetAdmissionReason::PeerRemoved);
    }

    #[test]
    fn public_testnet_abuse_controls_require_identity_and_secure_transport() {
        let peer = PeerId::random();
        let result = PublicTestnetAdmissionPolicy::controlled(vec![peer]);
        let Ok(policy) = result else {
            assert!(result.is_ok(), "controlled policy should parse");
            return;
        };

        assert!(policy.abuse_controls().requires_identity_registration());
        assert!(policy.abuse_controls().requires_secure_transport());

        let missing_identity =
            PublicTestnetAdmissionCandidate::new(peer).with_secure_transport_authenticated(true);
        let missing_secure_transport =
            PublicTestnetAdmissionCandidate::new(peer).with_identity_registered(true);

        assert_eq!(
            policy.evaluate(&missing_identity).reason(),
            PublicTestnetAdmissionReason::IdentityRegistrationRequired
        );
        assert_eq!(
            policy.evaluate(&missing_secure_transport).reason(),
            PublicTestnetAdmissionReason::SecureTransportRequired
        );
    }

    #[test]
    fn public_testnet_operator_limit_blocks_extra_node() {
        let peer = PeerId::random();
        let result = PublicTestnetAdmissionPolicy::controlled_with_controls(
            vec![peer],
            Vec::new(),
            1,
            PublicTestnetAbuseControls::strict(),
        );
        let Ok(policy) = result else {
            assert!(result.is_ok(), "controlled policy should parse");
            return;
        };
        let candidate = admitted_candidate(peer).with_operator_node_count(1);

        assert_eq!(
            policy.evaluate(&candidate).reason(),
            PublicTestnetAdmissionReason::OperatorNodeLimitExceeded
        );
    }

    #[test]
    fn public_testnet_policy_deduplicates_peer_sets() {
        let admitted = PeerId::random();
        let removed = PeerId::random();
        let result = PublicTestnetAdmissionPolicy::controlled_with_controls(
            vec![admitted, admitted],
            vec![removed, removed],
            2,
            PublicTestnetAbuseControls::strict(),
        );
        let Ok(policy) = result else {
            assert!(result.is_ok(), "controlled policy should parse");
            return;
        };

        assert_eq!(policy.admitted_peer_count(), 1);
        assert_eq!(policy.removed_peer_count(), 1);
    }

    #[test]
    fn public_testnet_policy_rejects_unbounded_peer_sets() {
        let admitted_result = PublicTestnetAdmissionPolicy::controlled(peer_set(
            MAX_PUBLIC_TESTNET_ADMITTED_PEERS + 1,
        ));
        let admitted = PeerId::random();
        let removed_result = PublicTestnetAdmissionPolicy::controlled_with_controls(
            vec![admitted],
            peer_set(MAX_PUBLIC_TESTNET_REMOVED_PEERS + 1),
            1,
            PublicTestnetAbuseControls::strict(),
        );

        assert!(matches!(
            admitted_result,
            Err(PublicTestnetAdmissionPolicyError::TooManyAdmittedPeers {
                max: MAX_PUBLIC_TESTNET_ADMITTED_PEERS
            })
        ));
        assert!(matches!(
            removed_result,
            Err(PublicTestnetAdmissionPolicyError::TooManyRemovedPeers {
                max: MAX_PUBLIC_TESTNET_REMOVED_PEERS
            })
        ));
    }

    #[test]
    fn public_testnet_policy_rejects_zero_operator_limit() {
        let peer = PeerId::random();
        let result = PublicTestnetAdmissionPolicy::controlled_with_controls(
            vec![peer],
            Vec::new(),
            0,
            PublicTestnetAbuseControls::strict(),
        );

        assert!(matches!(
            result,
            Err(PublicTestnetAdmissionPolicyError::MaxNodesPerOperatorMustBeNonZero)
        ));
    }
}
