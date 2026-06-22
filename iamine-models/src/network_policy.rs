use crate::ModelDescriptor;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NetworkPolicyMetadata {
    pub policy_class: Option<NetworkPolicyClass>,
    pub revision: Option<String>,
}

impl Default for NetworkPolicyMetadata {
    fn default() -> Self {
        Self::missing()
    }
}

impl NetworkPolicyMetadata {
    pub fn missing() -> Self {
        Self {
            policy_class: None,
            revision: None,
        }
    }

    pub fn distributed_allowed(revision: impl Into<String>) -> Self {
        Self {
            policy_class: Some(NetworkPolicyClass::DistributedAllowed),
            revision: Some(revision.into()),
        }
    }

    pub fn local_only(revision: impl Into<String>) -> Self {
        Self {
            policy_class: Some(NetworkPolicyClass::LocalOnly),
            revision: Some(revision.into()),
        }
    }

    pub fn blocked(revision: impl Into<String>) -> Self {
        Self {
            policy_class: Some(NetworkPolicyClass::Blocked),
            revision: Some(revision.into()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NetworkPolicyClass {
    DistributedAllowed,
    LocalOnly,
    Blocked,
}

impl NetworkPolicyClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DistributedAllowed => "distributed_allowed",
            Self::LocalOnly => "local_only",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkPolicyOperation {
    List,
    Download,
    Install,
    ExistingExecution,
    NetworkInference,
}

impl NetworkPolicyOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::List => "list",
            Self::Download => "download",
            Self::Install => "install",
            Self::ExistingExecution => "existing_execution",
            Self::NetworkInference => "network_inference",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkPolicyStatus {
    Allowed,
    LocalOnly,
    PendingMetadata,
    LegacyExecution,
    Blocked,
}

impl NetworkPolicyStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Allowed => "allowed",
            Self::LocalOnly => "local_only",
            Self::PendingMetadata => "pending_metadata",
            Self::LegacyExecution => "legacy_execution",
            Self::Blocked => "blocked",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NetworkPolicyReason {
    NetworkPolicyAllowed,
    LocalOnly,
    NetworkPolicyMissing,
    NetworkPolicyBlocked,
    LegacyInstalledModel,
}

impl NetworkPolicyReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NetworkPolicyAllowed => "network_policy_allowed",
            Self::LocalOnly => "local_only",
            Self::NetworkPolicyMissing => "network_policy_missing",
            Self::NetworkPolicyBlocked => "network_policy_blocked",
            Self::LegacyInstalledModel => "legacy_installed_model",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkPolicyDecision {
    pub status: NetworkPolicyStatus,
    pub reason: NetworkPolicyReason,
    pub permits_operation: bool,
    pub permits_distributed_inference: bool,
}

impl NetworkPolicyDecision {
    pub fn policy_reason(&self) -> &'static str {
        self.reason.as_str()
    }
}

#[derive(Debug, Clone, Default)]
pub struct ModelNetworkPolicy;

impl ModelNetworkPolicy {
    pub fn evaluate_descriptor(
        &self,
        model: &ModelDescriptor,
        operation: NetworkPolicyOperation,
        installed: bool,
    ) -> NetworkPolicyDecision {
        self.evaluate(Some(&model.network_policy), operation, installed)
    }

    pub fn evaluate(
        &self,
        metadata: Option<&NetworkPolicyMetadata>,
        operation: NetworkPolicyOperation,
        installed: bool,
    ) -> NetworkPolicyDecision {
        let Some(metadata) = metadata else {
            return missing_metadata_decision(operation, installed);
        };
        let Some(policy_class) = metadata.policy_class else {
            return missing_metadata_decision(operation, installed);
        };

        match policy_class {
            NetworkPolicyClass::DistributedAllowed => decision(
                NetworkPolicyStatus::Allowed,
                NetworkPolicyReason::NetworkPolicyAllowed,
                true,
                true,
            ),
            NetworkPolicyClass::LocalOnly => {
                let permits_operation = operation != NetworkPolicyOperation::NetworkInference;
                decision(
                    NetworkPolicyStatus::LocalOnly,
                    NetworkPolicyReason::LocalOnly,
                    permits_operation,
                    false,
                )
            }
            NetworkPolicyClass::Blocked => decision(
                NetworkPolicyStatus::Blocked,
                NetworkPolicyReason::NetworkPolicyBlocked,
                operation == NetworkPolicyOperation::List,
                false,
            ),
        }
    }
}

fn missing_metadata_decision(
    operation: NetworkPolicyOperation,
    installed: bool,
) -> NetworkPolicyDecision {
    let legacy_execution = installed && operation == NetworkPolicyOperation::ExistingExecution;
    decision(
        if legacy_execution {
            NetworkPolicyStatus::LegacyExecution
        } else {
            NetworkPolicyStatus::PendingMetadata
        },
        if legacy_execution {
            NetworkPolicyReason::LegacyInstalledModel
        } else {
            NetworkPolicyReason::NetworkPolicyMissing
        },
        legacy_execution || operation == NetworkPolicyOperation::List,
        false,
    )
}

fn decision(
    status: NetworkPolicyStatus,
    reason: NetworkPolicyReason,
    permits_operation: bool,
    permits_distributed_inference: bool,
) -> NetworkPolicyDecision {
    NetworkPolicyDecision {
        status,
        reason,
        permits_operation,
        permits_distributed_inference,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evaluate(
        metadata: &NetworkPolicyMetadata,
        operation: NetworkPolicyOperation,
    ) -> NetworkPolicyDecision {
        ModelNetworkPolicy.evaluate(Some(metadata), operation, false)
    }

    #[test]
    fn distributed_allowed_permits_network_inference() {
        let decision = evaluate(
            &NetworkPolicyMetadata::distributed_allowed("test-fixture"),
            NetworkPolicyOperation::NetworkInference,
        );

        assert_eq!(decision.status, NetworkPolicyStatus::Allowed);
        assert_eq!(decision.reason, NetworkPolicyReason::NetworkPolicyAllowed);
        assert!(decision.permits_operation);
        assert!(decision.permits_distributed_inference);
    }

    #[test]
    fn local_only_permits_install_but_blocks_network_inference() {
        let metadata = NetworkPolicyMetadata::local_only("test-fixture");

        let install = evaluate(&metadata, NetworkPolicyOperation::Install);
        let network = evaluate(&metadata, NetworkPolicyOperation::NetworkInference);

        assert!(install.permits_operation);
        assert_eq!(network.status, NetworkPolicyStatus::LocalOnly);
        assert_eq!(network.reason, NetworkPolicyReason::LocalOnly);
        assert!(!network.permits_operation);
        assert!(!network.permits_distributed_inference);
    }

    #[test]
    fn missing_metadata_blocks_new_install() {
        let decision = evaluate(
            &NetworkPolicyMetadata::missing(),
            NetworkPolicyOperation::Install,
        );

        assert_eq!(decision.status, NetworkPolicyStatus::PendingMetadata);
        assert_eq!(decision.reason, NetworkPolicyReason::NetworkPolicyMissing);
        assert!(!decision.permits_operation);
    }

    #[test]
    fn missing_metadata_remains_visible_for_list() {
        let decision = evaluate(
            &NetworkPolicyMetadata::missing(),
            NetworkPolicyOperation::List,
        );

        assert_eq!(decision.status, NetworkPolicyStatus::PendingMetadata);
        assert!(decision.permits_operation);
    }

    #[test]
    fn legacy_installed_model_can_continue_existing_execution_without_network() {
        let decision = ModelNetworkPolicy.evaluate(
            Some(&NetworkPolicyMetadata::missing()),
            NetworkPolicyOperation::ExistingExecution,
            true,
        );

        assert_eq!(decision.status, NetworkPolicyStatus::LegacyExecution);
        assert_eq!(decision.reason, NetworkPolicyReason::LegacyInstalledModel);
        assert!(decision.permits_operation);
        assert!(!decision.permits_distributed_inference);
    }

    #[test]
    fn blocked_policy_blocks_new_download() {
        let decision = evaluate(
            &NetworkPolicyMetadata::blocked("test-fixture"),
            NetworkPolicyOperation::Download,
        );

        assert_eq!(decision.status, NetworkPolicyStatus::Blocked);
        assert_eq!(decision.reason, NetworkPolicyReason::NetworkPolicyBlocked);
        assert!(!decision.permits_operation);
    }
}
