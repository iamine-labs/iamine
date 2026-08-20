# IAMINE Local Control API Request Catalog

## Status

```text
contract version: 1.0.0
interface version: 1.0.0
implementation status: contract only; no HTTP server is bound yet
contracted endpoint paths: 1
logical operations: 17
```

This catalog records the complete Local Control API surface approved by
`NODE-LOCAL-CONTROL-API-CONTRACT-001`. It is a consumer reference for the
dashboard, native CLI adapters, tests, and the future server implementation.
It does not authorize an operation and must not be treated as evidence that a
server or operation-specific payload is already implemented.

The Rust types in `iamine-core/src/local_control_api_contract.rs` and
`iamine-core/src/interface_contracts.rs` remain the source of truth. This file
must change with those types whenever an endpoint, operation, field, enum,
limit, or serialization rule changes.

## Transport And Endpoint

| Item | Contract |
| --- | --- |
| Transport | HTTP over an explicit IPv4 or IPv6 loopback bind only |
| Method | `POST` |
| Path | `/api/v1/operations` |
| Content type | `application/json` |
| Browser origin | Same origin is required |
| Native client origin | Must be classified explicitly as non-browser |
| Maximum encoded request | 65,536 bytes (64 KiB) |
| Maximum encoded response | 524,288 bytes (512 KiB) |
| Path parameters | None |
| Query parameters | None |
| Authentication header | No client-supplied header is approved; future adapter transport remains pending |
| Authorization evidence | Opaque server-owned Rust capability; never accepted from JSON |

The future adapter derives transport, peer, client, origin, method, route,
media type, and encoded byte count from socket and HTTP metadata. None of
those observations may be accepted as client-controlled JSON fields.

## Request Envelope

```json
{
  "schema_version": "1.0.0",
  "request_id": "45c3a273-f010-4d63-99cb-6fd29c553c48",
  "interface": {
    "schema_version": "1.0.0",
    "operation": {
      "id": "node_evidence_read",
      "class": "read_only_diagnostic"
    },
    "payload": null
  }
}
```

| JSON field | Type | Required | Rule |
| --- | --- | --- | --- |
| `schema_version` | string | Yes | Exact value `1.0.0` |
| `request_id` | UUID string | Yes | Non-null, lowercase, canonical, hyphenated UUID |
| `interface` | object | Yes | Shared interface request envelope |
| `interface.schema_version` | string | Yes | Exact value `1.0.0` |
| `interface.operation` | object | Yes | Canonical ID and its matching class |
| `interface.operation.id` | enum | Yes | One ID from the operation catalog below |
| `interface.operation.class` | enum | Yes | Must match the class assigned to the ID |
| `interface.payload` | operation-owned JSON | Yes | Schema is not frozen by the transport contract |

Unknown envelope or operation fields are rejected. `request_id` is only a
correlation identifier; it is not a credential, session, authorization grant,
nonce, or replay proof.

## Logical Operation Catalog

All logical operations use `POST /api/v1/operations`. There are no individual
URL paths per operation; adding them would create a second operation registry.

| `operation.id` | Required `operation.class` | Authorization handoff | Replay handoff | Audit handoff |
| --- | --- | --- | --- | --- |
| `node_evidence_read` | `read_only_diagnostic` | `read_only_session` | `not_required` | `request_decision` |
| `hardware_profile_read` | `read_only_diagnostic` | `read_only_session` | `not_required` | `request_decision` |
| `node_config_status_read` | `read_only_diagnostic` | `read_only_session` | `not_required` | `request_decision` |
| `node_identity_status_read` | `read_only_diagnostic` | `read_only_session` | `not_required` | `request_decision` |
| `cluster_status_read` | `read_only_operational` | `read_only_session` | `not_required` | `request_decision` |
| `task_stats_read` | `read_only_operational` | `read_only_session` | `not_required` | `request_decision` |
| `task_trace_read` | `read_only_operational` | `read_only_session` | `not_required` | `request_decision` |
| `model_catalog_read` | `read_only_operational` | `read_only_session` | `not_required` | `request_decision` |
| `support_bundle_plan_read` | `planned_mutation` | `planned_mutation` | `single_use_authorization_evidence` | `request_decision_and_authorization` |
| `node_config_migration_plan` | `planned_mutation` | `planned_mutation` | `single_use_authorization_evidence` | `request_decision_and_authorization` |
| `node_config_rollback_plan` | `planned_mutation` | `planned_mutation` | `single_use_authorization_evidence` | `request_decision_and_authorization` |
| `identity_initialization_plan` | `planned_mutation` | `planned_mutation` | `single_use_authorization_evidence` | `request_decision_and_authorization` |
| `hardware_refresh_plan` | `planned_mutation` | `planned_mutation` | `single_use_authorization_evidence` | `request_decision_and_authorization` |
| `worker_lifecycle` | `runtime_mutation` | `runtime_mutation` | `single_use_authorization_evidence` | `request_decision_and_authorization` |
| `agent_permission` | `agent_operation` | `agent_runtime` | `single_use_authorization_evidence` | `request_decision_and_authorization` |
| `agent_execution` | `agent_operation` | `agent_runtime` | `single_use_authorization_evidence` | `request_decision_and_authorization` |
| `agent_cancellation` | `agent_operation` | `agent_runtime` | `single_use_authorization_evidence` | `request_decision_and_authorization` |

An authorization handoff describes the evidence a future owner must require.
It never grants authority. Loopback reachability, same-origin validation,
request validation, request IDs, and permission-request events are also not
authorization grants.

## Payload Ownership

The transport contract deliberately does not define the JSON payload for each
operation. Before any operation is connected to a real owner, its payload must
be separately reviewed and added to this catalog with:

- exact required and optional fields;
- field types, enums, bounds, and defaults;
- privacy and redaction rules;
- owner crate or module;
- authorization evidence and single-use behavior where applicable;
- stable examples and negative tests.

Until that work is complete, a consumer must not infer payload fields from CLI
flags, frontend labels, mock fixtures, URL paths, or owner internals.

## Local Authorization Boundary

The future API adapter must resolve a server-owned `LocalSessionEvidence`
before calling local authorization. Session issuers, sessions, authorization
evidence, and consumed evidence are opaque, non-serializable Rust capabilities;
they are not request variables, headers, cookies, or bearer tokens.

| Operation class | Required local intent | Replay rule |
| --- | --- | --- |
| Read-only diagnostic | `proceed` or `confirm` | Request correlation only |
| Read-only operational | `proceed` or `confirm` | Request correlation only |
| Planned mutation | Explicit `confirm` | Single-use request ID until session expiry |
| Runtime mutation | Explicit `confirm` | Single-use request ID until session expiry |
| Agent operation | Explicit `confirm` | Single-use request ID plus agent-runtime authority |

The adapter must pass the complete approved decision to evidence consumption
so its request-decision audit handoff cannot be detached. The resulting local
consumption still does not authorize owner dispatch. Browser-to-server session
transport will be defined by `NODE-LOCAL-CONTROL-API-001`; clients must not add
an ad hoc authentication field while that contract is absent.

## Response Envelope

Every response preserves the request ID and operation from its request.

```json
{
  "schema_version": "1.0.0",
  "request_id": "45c3a273-f010-4d63-99cb-6fd29c553c48",
  "interface": {
    "schema_version": "1.0.0",
    "operation": {
      "id": "node_evidence_read",
      "class": "read_only_diagnostic"
    },
    "outcome": {
      "success": {
        "data": {},
        "provenance": {
          "source": "owner_module",
          "evidence_scope": "current_snapshot",
          "redaction": "applied",
          "authoritative": true
        },
        "warnings": []
      }
    }
  }
}
```

| JSON field | Type | Rule |
| --- | --- | --- |
| `schema_version` | string | Exact value `1.0.0` |
| `request_id` | UUID string | Must equal the request ID |
| `interface.schema_version` | string | Exact value `1.0.0` |
| `interface.operation` | object | Must equal the requested operation |
| `interface.outcome` | tagged object | Exactly one outcome variant |

### Outcome Variants

| Variant | Carries `data` | Carries `problem` | Also required |
| --- | --- | --- | --- |
| `success` | Yes | No | `provenance`, `warnings` |
| `attention` | Yes | No | `provenance`, `warnings` |
| `blocked` | No | Yes | `provenance`, `warnings` |
| `unavailable` | No | Yes | `provenance`, `warnings` |
| `stale` | Yes | No | `provenance`, `warnings` |
| `unknown` | No | Yes | `provenance`, `warnings` |

### Provenance Fields

| Field | Allowed values |
| --- | --- |
| `source` | `owner_module`, `mock_fixture` |
| `evidence_scope` | `current_snapshot`, `point_in_time`, `planned_operation`, `no_evidence` |
| `redaction` | `applied`, `not_required` |
| `authoritative` | Boolean; must be `false` for `mock_fixture` |

### Warning Fields

At most eight warnings are accepted. Each warning contains `code` and
`operator_action`.

| `code` values |
| --- |
| `partial_evidence` |
| `redacted_field` |
| `fallback_observation` |
| `deprecated_field` |
| `freshness_boundary` |

## Problems And Operator Actions

A problem contains only stable `code` and `operator_action` values. It must not
echo socket metadata, origins, headers, credentials, payloads, prompts, paths,
backend errors, or personal identifiers.

Allowed problem codes:

| `code` values |
| --- |
| `invalid_request` |
| `unsupported_schema` |
| `malformed_payload` |
| `permission_required` |
| `policy_blocked` |
| `owner_unavailable` |
| `evidence_stale` |
| `evidence_unknown` |
| `internal_failure` |

Allowed operator actions:

| `operator_action` values |
| --- |
| `none` |
| `retry` |
| `review_owner_evidence` |
| `authenticate_locally` |
| `request_authorization` |
| `contact_support` |

The current transport-error mappings are:

| Contract failure | Problem code | Operator action |
| --- | --- | --- |
| Unsupported API schema | `unsupported_schema` | `contact_support` |
| Non-loopback transport or peer | `policy_blocked` | `none` |
| Rejected browser/native origin classification | `policy_blocked` | `none` |
| Invalid UUID, method, route, media type, request size, or operation match | `invalid_request` | `none` |
| Oversized response | `internal_failure` | `contact_support` |

## Intentionally Undefined

The following items are not part of the approved contract yet:

- server process, port, startup, shutdown, or recovery behavior;
- HTTP status-code mapping;
- browser-to-server session bootstrap, cookie/header transport, or CSRF mechanism;
- operation-specific request and response payload schemas;
- persistent replay storage; current replay state is bounded and memory-only;
- audit persistence and event delivery;
- dashboard connectivity and generated TypeScript clients;
- owner dispatch, mutation, agent execution, or remote access.

These items require their owning roadmap features. Consumers must fail closed
instead of filling any gap with an ad hoc field, route, header, or policy.
