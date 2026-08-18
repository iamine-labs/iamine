use iamine_core::{
    validate_local_control_response_size, InterfaceEvidenceScope, InterfaceOperation,
    InterfaceOperationId, InterfaceOutcome, InterfaceProvenance, InterfaceRedaction,
    InterfaceRequest, InterfaceResponse, InterfaceWarnings, LocalControlAuditRequirement,
    LocalControlAuthorizationRequirement, LocalControlClient, LocalControlContractError,
    LocalControlIngress, LocalControlMediaType, LocalControlMethod, LocalControlOrigin,
    LocalControlPeer, LocalControlReplayRequirement, LocalControlRequest, LocalControlRequestId,
    LocalControlResponse, LocalControlRoute, LocalControlTransport,
    LOCAL_CONTROL_API_SCHEMA_VERSION, LOCAL_CONTROL_OPERATION_PATH,
    MAX_LOCAL_CONTROL_REQUEST_BYTES, MAX_LOCAL_CONTROL_RESPONSE_BYTES,
};
use serde_json::json;

const REQUEST_ID: &str = "45c3a273-f010-4d63-99cb-6fd29c553c48";

fn request(
    operation_id: InterfaceOperationId,
) -> Result<LocalControlRequest<()>, LocalControlContractError> {
    Ok(LocalControlRequest::with_request_id(
        LocalControlRequestId::try_from(REQUEST_ID.to_string())?,
        InterfaceRequest::new(InterfaceOperation::new(operation_id), ()),
    ))
}

fn browser_ingress(encoded_body_bytes: usize) -> LocalControlIngress {
    LocalControlIngress {
        transport: LocalControlTransport::LoopbackHttp,
        peer: LocalControlPeer::Ipv4Loopback,
        client: LocalControlClient::BrowserDashboard,
        origin: LocalControlOrigin::SameOrigin,
        method: LocalControlMethod::Post,
        route: LocalControlRoute::Operations,
        media_type: LocalControlMediaType::ApplicationJson,
        encoded_body_bytes,
    }
}

#[test]
fn request_and_response_have_a_versioned_bounded_shape() -> Result<(), Box<dyn std::error::Error>> {
    let request = request(InterfaceOperationId::NodeEvidenceRead)?;
    let response = LocalControlResponse::try_for_request(
        &request,
        InterfaceResponse::new(
            InterfaceOperation::new(InterfaceOperationId::NodeEvidenceRead),
            InterfaceOutcome::Success {
                data: "ready",
                provenance: InterfaceProvenance::owner(
                    InterfaceEvidenceScope::CurrentSnapshot,
                    InterfaceRedaction::Applied,
                ),
                warnings: InterfaceWarnings::empty(),
            },
        ),
    )?;

    assert_eq!(
        serde_json::to_value(&request)?,
        json!({
            "schema_version": LOCAL_CONTROL_API_SCHEMA_VERSION,
            "request_id": REQUEST_ID,
            "interface": {
                "schema_version": "1.0.0",
                "operation": {
                    "id": "node_evidence_read",
                    "class": "read_only_diagnostic"
                },
                "payload": null
            }
        })
    );
    assert_eq!(serde_json::to_value(&response)?["request_id"], REQUEST_ID);
    assert_eq!(
        serde_json::to_value(&response)?["interface"]["outcome"]["success"]["data"],
        "ready"
    );
    assert_eq!(LOCAL_CONTROL_OPERATION_PATH, "/api/v1/operations");
    Ok(())
}

#[test]
fn incompatible_schema_invalid_id_and_unknown_fields_fail_closed() {
    let incompatible = json!({
        "schema_version": "9.0.0",
        "request_id": REQUEST_ID,
        "interface": {
            "schema_version": "1.0.0",
            "operation": { "id": "node_evidence_read", "class": "read_only_diagnostic" },
            "payload": null
        }
    });
    assert!(serde_json::from_value::<LocalControlRequest<()>>(incompatible).is_err());

    let invalid_id = json!({
        "schema_version": LOCAL_CONTROL_API_SCHEMA_VERSION,
        "request_id": "dashboard-request-1",
        "interface": {
            "schema_version": "1.0.0",
            "operation": { "id": "node_evidence_read", "class": "read_only_diagnostic" },
            "payload": null
        }
    });
    assert!(serde_json::from_value::<LocalControlRequest<()>>(invalid_id).is_err());

    let nil_id = json!({
        "schema_version": LOCAL_CONTROL_API_SCHEMA_VERSION,
        "request_id": "00000000-0000-0000-0000-000000000000",
        "interface": {
            "schema_version": "1.0.0",
            "operation": { "id": "node_evidence_read", "class": "read_only_diagnostic" },
            "payload": null
        }
    });
    assert!(serde_json::from_value::<LocalControlRequest<()>>(nil_id).is_err());

    let unknown = json!({
        "schema_version": LOCAL_CONTROL_API_SCHEMA_VERSION,
        "request_id": REQUEST_ID,
        "interface": {
            "schema_version": "1.0.0",
            "operation": { "id": "node_evidence_read", "class": "read_only_diagnostic" },
            "payload": null
        },
        "token": "must-not-be-accepted"
    });
    assert!(serde_json::from_value::<LocalControlRequest<()>>(unknown).is_err());
}

#[test]
fn loopback_browser_read_reaches_a_non_authorizing_handoff() -> Result<(), LocalControlContractError>
{
    let validated =
        browser_ingress(1024).validate(&request(InterfaceOperationId::NodeEvidenceRead)?)?;

    assert_eq!(validated.request_id.as_str(), REQUEST_ID);
    assert_eq!(
        validated.authorization.requirement,
        LocalControlAuthorizationRequirement::ReadOnlySession
    );
    assert_eq!(
        validated.authorization.replay,
        LocalControlReplayRequirement::NotRequired
    );
    assert_eq!(
        validated.authorization.audit,
        LocalControlAuditRequirement::RequestDecision
    );
    assert!(!validated.authorization.authorizes_action());
    assert!(!validated.authorizes_action());
    Ok(())
}

#[test]
fn ipv6_loopback_native_client_has_an_explicit_non_browser_origin(
) -> Result<(), LocalControlContractError> {
    let ingress = LocalControlIngress {
        peer: LocalControlPeer::Ipv6Loopback,
        client: LocalControlClient::LocalNative,
        origin: LocalControlOrigin::NoBrowserOrigin,
        ..browser_ingress(1024)
    };

    assert!(ingress
        .validate(&request(InterfaceOperationId::HardwareProfileRead)?)
        .is_ok());
    Ok(())
}

#[test]
fn non_loopback_transport_peer_and_browser_origin_are_rejected(
) -> Result<(), LocalControlContractError> {
    let request = request(InterfaceOperationId::NodeEvidenceRead)?;
    let cases = [
        (
            LocalControlIngress {
                transport: LocalControlTransport::Other,
                ..browser_ingress(1)
            },
            LocalControlContractError::NonLoopbackTransport,
        ),
        (
            LocalControlIngress {
                peer: LocalControlPeer::NonLoopback,
                ..browser_ingress(1)
            },
            LocalControlContractError::NonLoopbackPeer,
        ),
        (
            LocalControlIngress {
                origin: LocalControlOrigin::CrossOrigin,
                ..browser_ingress(1)
            },
            LocalControlContractError::OriginRejected,
        ),
        (
            LocalControlIngress {
                origin: LocalControlOrigin::Missing,
                ..browser_ingress(1)
            },
            LocalControlContractError::OriginRejected,
        ),
    ];

    for (ingress, expected) in cases {
        assert_eq!(ingress.validate(&request), Err(expected));
    }
    Ok(())
}

#[test]
fn method_media_type_and_request_size_are_bounded() -> Result<(), LocalControlContractError> {
    let request = request(InterfaceOperationId::NodeEvidenceRead)?;
    assert_eq!(
        LocalControlIngress {
            method: LocalControlMethod::Other,
            ..browser_ingress(1)
        }
        .validate(&request),
        Err(LocalControlContractError::MethodNotAllowed)
    );
    assert_eq!(
        LocalControlIngress {
            route: LocalControlRoute::Other,
            ..browser_ingress(1)
        }
        .validate(&request),
        Err(LocalControlContractError::UnsupportedRoute)
    );
    assert_eq!(
        LocalControlIngress {
            media_type: LocalControlMediaType::Other,
            ..browser_ingress(1)
        }
        .validate(&request),
        Err(LocalControlContractError::UnsupportedMediaType)
    );
    assert!(browser_ingress(MAX_LOCAL_CONTROL_REQUEST_BYTES)
        .validate(&request)
        .is_ok());
    assert_eq!(
        browser_ingress(MAX_LOCAL_CONTROL_REQUEST_BYTES + 1).validate(&request),
        Err(LocalControlContractError::RequestTooLarge)
    );
    Ok(())
}

#[test]
fn mutations_and_agent_intents_require_replay_and_audit_handoffs(
) -> Result<(), LocalControlContractError> {
    let cases = [
        (
            InterfaceOperationId::HardwareRefreshPlan,
            LocalControlAuthorizationRequirement::PlannedMutation,
        ),
        (
            InterfaceOperationId::WorkerLifecycle,
            LocalControlAuthorizationRequirement::RuntimeMutation,
        ),
        (
            InterfaceOperationId::AgentExecution,
            LocalControlAuthorizationRequirement::AgentRuntime,
        ),
    ];

    for (operation_id, requirement) in cases {
        let validated = browser_ingress(1024).validate(&request(operation_id)?)?;
        assert_eq!(validated.authorization.requirement, requirement);
        assert_eq!(
            validated.authorization.replay,
            LocalControlReplayRequirement::SingleUseAuthorizationEvidence
        );
        assert_eq!(
            validated.authorization.audit,
            LocalControlAuditRequirement::RequestDecisionAndAuthorization
        );
        assert!(!validated.authorizes_action());
    }
    Ok(())
}

#[test]
fn response_operation_and_size_cannot_drift_from_the_request(
) -> Result<(), LocalControlContractError> {
    let request = request(InterfaceOperationId::NodeEvidenceRead)?;
    let mismatched = InterfaceResponse::new(
        InterfaceOperation::new(InterfaceOperationId::ClusterStatusRead),
        InterfaceOutcome::<()>::Unknown {
            problem: LocalControlContractError::OperationMismatch.interface_problem(),
            provenance: InterfaceProvenance::owner(
                InterfaceEvidenceScope::NoEvidence,
                InterfaceRedaction::NotRequired,
            ),
            warnings: InterfaceWarnings::empty(),
        },
    );

    assert_eq!(
        LocalControlResponse::try_for_request(&request, mismatched),
        Err(LocalControlContractError::OperationMismatch)
    );
    assert_eq!(
        validate_local_control_response_size(MAX_LOCAL_CONTROL_RESPONSE_BYTES),
        Ok(())
    );
    assert_eq!(
        validate_local_control_response_size(MAX_LOCAL_CONTROL_RESPONSE_BYTES + 1),
        Err(LocalControlContractError::ResponseTooLarge)
    );
    Ok(())
}

#[test]
fn ingress_errors_map_to_stable_redacted_interface_problems() {
    let blocked = LocalControlContractError::NonLoopbackPeer.interface_problem();
    let invalid = LocalControlContractError::RequestTooLarge.interface_problem();
    let internal = LocalControlContractError::ResponseTooLarge.interface_problem();

    assert_eq!(format!("{:?}", blocked.code), "PolicyBlocked");
    assert_eq!(format!("{:?}", invalid.code), "InvalidRequest");
    assert_eq!(format!("{:?}", internal.code), "InternalFailure");
    assert!(!format!("{blocked:?}{invalid:?}{internal:?}").contains("127.0.0.1"));
}
