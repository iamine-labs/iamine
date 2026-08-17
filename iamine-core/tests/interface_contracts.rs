use iamine_core::{
    InterfaceContractError, InterfaceEvent, InterfaceEventPayload, InterfaceEventStream,
    InterfaceEvidenceScope, InterfaceOperation, InterfaceOperationClass, InterfaceOperationId,
    InterfaceOperatorAction, InterfaceOutcome, InterfaceOutcomeStatus, InterfaceProblem,
    InterfaceProblemCode, InterfaceProvenance, InterfaceProvenanceSource, InterfaceRedaction,
    InterfaceRequest, InterfaceResponse, InterfaceWarning, InterfaceWarningCode, InterfaceWarnings,
    INTERFACE_CONTRACT_SCHEMA_VERSION, MAX_INTERFACE_WARNINGS,
};
use serde_json::json;

fn read_operation() -> InterfaceOperation {
    InterfaceOperation::new(InterfaceOperationId::NodeEvidenceRead)
}

#[test]
fn request_and_response_use_the_current_schema() {
    let request = InterfaceRequest::new(read_operation(), ());
    let response = InterfaceResponse::new(
        read_operation(),
        InterfaceOutcome::Success {
            data: "evidence",
            provenance: InterfaceProvenance::owner(
                InterfaceEvidenceScope::CurrentSnapshot,
                InterfaceRedaction::Applied,
            ),
            warnings: InterfaceWarnings::empty(),
        },
    );

    let request_json = serde_json::to_value(request).unwrap();
    let response_json = serde_json::to_value(response).unwrap();
    assert_eq!(
        request_json,
        json!({
            "schema_version": INTERFACE_CONTRACT_SCHEMA_VERSION,
            "operation": {
                "id": "node_evidence_read",
                "class": "read_only_diagnostic"
            },
            "payload": null
        })
    );
    assert_eq!(
        response_json,
        json!({
            "schema_version": INTERFACE_CONTRACT_SCHEMA_VERSION,
            "operation": {
                "id": "node_evidence_read",
                "class": "read_only_diagnostic"
            },
            "outcome": {
                "success": {
                    "data": "evidence",
                    "provenance": {
                        "source": "owner_module",
                        "evidence_scope": "current_snapshot",
                        "redaction": "applied",
                        "authoritative": true
                    },
                    "warnings": []
                }
            }
        })
    );
}

#[test]
fn operation_ids_have_one_canonical_class() {
    use InterfaceOperationClass::{
        AgentOperation, PlannedMutation, ReadOnlyDiagnostic, ReadOnlyOperational, RuntimeMutation,
    };
    use InterfaceOperationId::*;

    let cases = [
        (NodeEvidenceRead, ReadOnlyDiagnostic),
        (HardwareProfileRead, ReadOnlyDiagnostic),
        (NodeConfigStatusRead, ReadOnlyDiagnostic),
        (NodeIdentityStatusRead, ReadOnlyDiagnostic),
        (ClusterStatusRead, ReadOnlyOperational),
        (TaskStatsRead, ReadOnlyOperational),
        (TaskTraceRead, ReadOnlyOperational),
        (ModelCatalogRead, ReadOnlyOperational),
        (SupportBundlePlanRead, PlannedMutation),
        (NodeConfigMigrationPlan, PlannedMutation),
        (NodeConfigRollbackPlan, PlannedMutation),
        (IdentityInitializationPlan, PlannedMutation),
        (HardwareRefreshPlan, PlannedMutation),
        (WorkerLifecycle, RuntimeMutation),
        (AgentPermission, AgentOperation),
        (AgentExecution, AgentOperation),
        (AgentCancellation, AgentOperation),
    ];

    for (id, expected_class) in cases {
        assert_eq!(InterfaceOperation::new(id).class(), expected_class);
    }
}

#[test]
fn incompatible_schema_is_rejected_during_deserialization() {
    let payload = json!({
        "schema_version": "9.0.0",
        "operation": {
            "id": "node_evidence_read",
            "class": "read_only_diagnostic"
        },
        "payload": null
    });

    assert!(serde_json::from_value::<InterfaceRequest<()>>(payload).is_err());
}

#[test]
fn operation_class_mismatch_is_rejected() {
    let payload = json!({
        "id": "node_evidence_read",
        "class": "runtime_mutation"
    });

    assert!(serde_json::from_value::<InterfaceOperation>(payload).is_err());
}

#[test]
fn unknown_fields_are_rejected_at_contract_boundaries() {
    let request = json!({
        "schema_version": INTERFACE_CONTRACT_SCHEMA_VERSION,
        "operation": {
            "id": "node_evidence_read",
            "class": "read_only_diagnostic"
        },
        "payload": null,
        "future_field": true
    });
    assert!(serde_json::from_value::<InterfaceRequest<()>>(request).is_err());

    let outcome = json!({
        "success": {
            "data": "evidence",
            "provenance": {
                "source": "owner_module",
                "evidence_scope": "current_snapshot",
                "redaction": "applied",
                "authoritative": true
            },
            "warnings": [],
            "future_field": true
        }
    });
    assert!(serde_json::from_value::<InterfaceOutcome<String>>(outcome).is_err());
}

#[test]
fn warnings_are_bounded() {
    let warnings = vec![
        InterfaceWarning {
            code: InterfaceWarningCode::PartialEvidence,
            operator_action: InterfaceOperatorAction::ReviewOwnerEvidence,
        };
        MAX_INTERFACE_WARNINGS + 1
    ];

    assert_eq!(
        InterfaceWarnings::try_from_items(warnings),
        Err(InterfaceContractError::TooManyWarnings)
    );
}

#[test]
fn blocked_outcome_has_no_data_even_with_warnings() {
    let outcome = InterfaceOutcome::<String>::Blocked {
        problem: InterfaceProblem::new(
            InterfaceProblemCode::PolicyBlocked,
            InterfaceOperatorAction::RequestAuthorization,
        ),
        provenance: InterfaceProvenance::owner(
            InterfaceEvidenceScope::NoEvidence,
            InterfaceRedaction::NotRequired,
        ),
        warnings: InterfaceWarnings::empty(),
    };

    assert_eq!(outcome.status(), InterfaceOutcomeStatus::Blocked);
    assert!(outcome.data().is_none());
}

#[test]
fn mock_provenance_is_explicitly_non_authoritative() {
    let provenance = InterfaceProvenance::mock(InterfaceEvidenceScope::CurrentSnapshot);
    assert_eq!(provenance.source(), InterfaceProvenanceSource::MockFixture);
    assert!(!provenance.is_authoritative());

    let payload = json!({
        "source": "mock_fixture",
        "evidence_scope": "current_snapshot",
        "redaction": "applied",
        "authoritative": true
    });
    assert!(serde_json::from_value::<InterfaceProvenance>(payload).is_err());
}

#[test]
fn events_are_ordered_and_cannot_authorize_actions() {
    let event = InterfaceEvent::new(
        7,
        InterfaceEventPayload::PermissionRequested {
            operation: InterfaceOperation::new(InterfaceOperationId::AgentPermission),
        },
    );

    assert_eq!(event.identity().sequence, 7);
    assert_eq!(event.identity().stream, InterfaceEventStream::Audit);
    assert!(!event.authorizes_action());
    let serialized = serde_json::to_value(&event).unwrap();
    assert_eq!(
        serialized,
        json!({
            "schema_version": INTERFACE_CONTRACT_SCHEMA_VERSION,
            "identity": {
                "stream": "audit",
                "sequence": 7
            },
            "payload": {
                "permission_requested": {
                    "operation": {
                        "id": "agent_permission",
                        "class": "agent_operation"
                    }
                }
            }
        })
    );
    assert!(!serialized.to_string().contains("authorize"));
}

#[test]
fn event_stream_mismatch_is_rejected_during_deserialization() {
    let event = json!({
        "schema_version": INTERFACE_CONTRACT_SCHEMA_VERSION,
        "identity": {
            "stream": "node_state",
            "sequence": 7
        },
        "payload": {
            "permission_requested": {
                "operation": {
                    "id": "agent_permission",
                    "class": "agent_operation"
                }
            }
        }
    });

    assert!(serde_json::from_value::<InterfaceEvent>(event).is_err());
}
