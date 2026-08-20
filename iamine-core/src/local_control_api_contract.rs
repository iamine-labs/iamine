use crate::{
    InterfaceOperation, InterfaceOperationClass, InterfaceOperatorAction, InterfaceProblem,
    InterfaceProblemCode, InterfaceRequest, InterfaceResponse,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use uuid::Uuid;

pub const LOCAL_CONTROL_API_SCHEMA_VERSION: &str = "1.0.0";
pub const LOCAL_CONTROL_OPERATION_PATH: &str = "/api/v1/operations";
pub const MAX_LOCAL_CONTROL_REQUEST_BYTES: usize = 64 * 1024;
pub const MAX_LOCAL_CONTROL_RESPONSE_BYTES: usize = 512 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum LocalControlContractError {
    #[error("local control API schema is unsupported")]
    UnsupportedSchema,
    #[error("local control request ID is invalid")]
    InvalidRequestId,
    #[error("local control transport is not loopback HTTP")]
    NonLoopbackTransport,
    #[error("local control peer is not loopback")]
    NonLoopbackPeer,
    #[error("local control request method is not allowed")]
    MethodNotAllowed,
    #[error("local control request route is unsupported")]
    UnsupportedRoute,
    #[error("local control request media type is unsupported")]
    UnsupportedMediaType,
    #[error("local control request origin is rejected")]
    OriginRejected,
    #[error("local control request exceeds its byte limit")]
    RequestTooLarge,
    #[error("local control response exceeds its byte limit")]
    ResponseTooLarge,
    #[error("local control response operation does not match its request")]
    OperationMismatch,
}

impl LocalControlContractError {
    pub const fn interface_problem(self) -> InterfaceProblem {
        let (code, operator_action) = match self {
            Self::UnsupportedSchema => (
                InterfaceProblemCode::UnsupportedSchema,
                InterfaceOperatorAction::ContactSupport,
            ),
            Self::NonLoopbackTransport | Self::NonLoopbackPeer | Self::OriginRejected => (
                InterfaceProblemCode::PolicyBlocked,
                InterfaceOperatorAction::None,
            ),
            Self::ResponseTooLarge => (
                InterfaceProblemCode::InternalFailure,
                InterfaceOperatorAction::ContactSupport,
            ),
            Self::InvalidRequestId
            | Self::MethodNotAllowed
            | Self::UnsupportedRoute
            | Self::UnsupportedMediaType
            | Self::RequestTooLarge
            | Self::OperationMismatch => (
                InterfaceProblemCode::InvalidRequest,
                InterfaceOperatorAction::None,
            ),
        };
        InterfaceProblem::new(code, operator_action)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct LocalControlSchemaVersion(String);

impl LocalControlSchemaVersion {
    pub fn current() -> Self {
        Self(LOCAL_CONTROL_API_SCHEMA_VERSION.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl TryFrom<String> for LocalControlSchemaVersion {
    type Error = LocalControlContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value == LOCAL_CONTROL_API_SCHEMA_VERSION {
            Ok(Self(value))
        } else {
            Err(LocalControlContractError::UnsupportedSchema)
        }
    }
}

impl From<LocalControlSchemaVersion> for String {
    fn from(value: LocalControlSchemaVersion) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct LocalControlRequestId(String);

impl LocalControlRequestId {
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for LocalControlRequestId {
    fn default() -> Self {
        Self::new()
    }
}

impl TryFrom<String> for LocalControlRequestId {
    type Error = LocalControlContractError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let parsed =
            Uuid::parse_str(&value).map_err(|_| LocalControlContractError::InvalidRequestId)?;
        if parsed.is_nil() || parsed.to_string() != value {
            return Err(LocalControlContractError::InvalidRequestId);
        }
        Ok(Self(value))
    }
}

impl From<LocalControlRequestId> for String {
    fn from(value: LocalControlRequestId) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalControlRequest<T> {
    pub schema_version: LocalControlSchemaVersion,
    pub request_id: LocalControlRequestId,
    pub interface: InterfaceRequest<T>,
}

impl<T> LocalControlRequest<T> {
    pub fn new(interface: InterfaceRequest<T>) -> Self {
        Self {
            schema_version: LocalControlSchemaVersion::current(),
            request_id: LocalControlRequestId::new(),
            interface,
        }
    }

    pub fn with_request_id(
        request_id: LocalControlRequestId,
        interface: InterfaceRequest<T>,
    ) -> Self {
        Self {
            schema_version: LocalControlSchemaVersion::current(),
            request_id,
            interface,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalControlResponse<T> {
    pub schema_version: LocalControlSchemaVersion,
    pub request_id: LocalControlRequestId,
    pub interface: InterfaceResponse<T>,
}

impl<T> LocalControlResponse<T> {
    pub fn try_for_request<P>(
        request: &LocalControlRequest<P>,
        interface: InterfaceResponse<T>,
    ) -> Result<Self, LocalControlContractError> {
        if request.interface.operation != interface.operation {
            return Err(LocalControlContractError::OperationMismatch);
        }
        Ok(Self {
            schema_version: LocalControlSchemaVersion::current(),
            request_id: request.request_id.clone(),
            interface,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalControlTransport {
    LoopbackHttp,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalControlPeer {
    Ipv4Loopback,
    Ipv6Loopback,
    NonLoopback,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalControlClient {
    BrowserDashboard,
    LocalNative,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalControlOrigin {
    SameOrigin,
    NoBrowserOrigin,
    Missing,
    CrossOrigin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalControlMethod {
    Post,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalControlRoute {
    Operations,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalControlMediaType {
    ApplicationJson,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalControlIngress {
    pub transport: LocalControlTransport,
    pub peer: LocalControlPeer,
    pub client: LocalControlClient,
    pub origin: LocalControlOrigin,
    pub method: LocalControlMethod,
    pub route: LocalControlRoute,
    pub media_type: LocalControlMediaType,
    pub encoded_body_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalControlAuthorizationRequirement {
    ReadOnlySession,
    PlannedMutation,
    RuntimeMutation,
    AgentRuntime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalControlReplayRequirement {
    NotRequired,
    SingleUseAuthorizationEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocalControlAuditRequirement {
    RequestDecision,
    RequestDecisionAndAuthorization,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LocalControlAuthorizationHandoff {
    pub operation: InterfaceOperation,
    pub requirement: LocalControlAuthorizationRequirement,
    pub replay: LocalControlReplayRequirement,
    pub audit: LocalControlAuditRequirement,
}

impl LocalControlAuthorizationHandoff {
    pub const fn for_operation(operation: InterfaceOperation) -> Self {
        let (requirement, replay, audit) = match operation.class() {
            InterfaceOperationClass::ReadOnlyDiagnostic
            | InterfaceOperationClass::ReadOnlyOperational => (
                LocalControlAuthorizationRequirement::ReadOnlySession,
                LocalControlReplayRequirement::NotRequired,
                LocalControlAuditRequirement::RequestDecision,
            ),
            InterfaceOperationClass::PlannedMutation => (
                LocalControlAuthorizationRequirement::PlannedMutation,
                LocalControlReplayRequirement::SingleUseAuthorizationEvidence,
                LocalControlAuditRequirement::RequestDecisionAndAuthorization,
            ),
            InterfaceOperationClass::RuntimeMutation => (
                LocalControlAuthorizationRequirement::RuntimeMutation,
                LocalControlReplayRequirement::SingleUseAuthorizationEvidence,
                LocalControlAuditRequirement::RequestDecisionAndAuthorization,
            ),
            InterfaceOperationClass::AgentOperation => (
                LocalControlAuthorizationRequirement::AgentRuntime,
                LocalControlReplayRequirement::SingleUseAuthorizationEvidence,
                LocalControlAuditRequirement::RequestDecisionAndAuthorization,
            ),
        };
        Self {
            operation,
            requirement,
            replay,
            audit,
        }
    }

    pub const fn authorizes_action(&self) -> bool {
        false
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalControlValidatedRequest {
    pub request_id: LocalControlRequestId,
    pub authorization: LocalControlAuthorizationHandoff,
}

impl LocalControlValidatedRequest {
    pub const fn authorizes_action(&self) -> bool {
        false
    }
}

impl LocalControlIngress {
    pub fn validate<T>(
        self,
        request: &LocalControlRequest<T>,
    ) -> Result<LocalControlValidatedRequest, LocalControlContractError> {
        if self.transport != LocalControlTransport::LoopbackHttp {
            return Err(LocalControlContractError::NonLoopbackTransport);
        }
        if self.peer == LocalControlPeer::NonLoopback {
            return Err(LocalControlContractError::NonLoopbackPeer);
        }
        if self.method != LocalControlMethod::Post {
            return Err(LocalControlContractError::MethodNotAllowed);
        }
        if self.route != LocalControlRoute::Operations {
            return Err(LocalControlContractError::UnsupportedRoute);
        }
        if self.media_type != LocalControlMediaType::ApplicationJson {
            return Err(LocalControlContractError::UnsupportedMediaType);
        }
        if self.encoded_body_bytes > MAX_LOCAL_CONTROL_REQUEST_BYTES {
            return Err(LocalControlContractError::RequestTooLarge);
        }
        let origin_is_valid = matches!(
            (self.client, self.origin),
            (
                LocalControlClient::BrowserDashboard,
                LocalControlOrigin::SameOrigin
            ) | (
                LocalControlClient::LocalNative,
                LocalControlOrigin::NoBrowserOrigin
            )
        );
        if !origin_is_valid {
            return Err(LocalControlContractError::OriginRejected);
        }

        let authorization =
            LocalControlAuthorizationHandoff::for_operation(request.interface.operation);

        Ok(LocalControlValidatedRequest {
            request_id: request.request_id.clone(),
            authorization,
        })
    }
}

pub fn validate_local_control_response_size(
    encoded_body_bytes: usize,
) -> Result<(), LocalControlContractError> {
    if encoded_body_bytes > MAX_LOCAL_CONTROL_RESPONSE_BYTES {
        Err(LocalControlContractError::ResponseTooLarge)
    } else {
        Ok(())
    }
}
