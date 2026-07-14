# AGENT-AUDIT-LOG-001

## Objective

Define the first IAMINE agent audit log contract for privacy-safe review
evidence without enabling runtime logging, agent execution, sandboxing,
permission enforcement, registry admission, or marketplace behavior.

## Scope

This feature adds:

```text
docs/agents/agent-audit-log.md
docs/architecture/agent-audit-log.md
docs/qa/agent-audit-log.md
```

It also updates the v0.11.1 Agent Architecture Foundation roadmap state for
`AGENT-AUDIT-LOG-001` and aligns package manifest audit references with the
existing skeleton path `metadata/agent-audit.toml`.

## Architecture Boundary

This feature is documentation-only. It intentionally does not modify:

- agent runtime;
- executable agent packages;
- agent skeleton generator;
- package manifest parser;
- scope manifest parser;
- capability metadata parser;
- expertise metadata parser;
- resource requirement parser;
- permission parser or runtime permission enforcement;
- runtime audit logging implementation;
- sandboxing;
- agent registry;
- router or scheduler behavior;
- worker startup or worker capability advertisement;
- model routing, model loading, backend selection, inference execution, or
  distributed inference behavior;
- hardware profiler behavior or hardware compatibility gates;
- marketplace behavior;
- public beta behavior;
- P2P, PubSub, installer, updater, rollback, reputation, reward, wallet,
  settlement, token, or mainnet behavior;
- Rust source, tests, scripts, service definitions, release artifacts,
  dependencies, package generation, or schema generation.

## Audit Policy Role

The audit policy describes what evidence future agent review and execution must
produce, redact, retain, and reject. It does not create runtime logs.

It answers:

```text
What privacy-safe evidence must exist for reviewing this agent package?
```

It must not decide:

- whether an agent is executable;
- whether runtime logging exists;
- whether permission enforcement exists;
- whether a task is in scope;
- whether a capability exists;
- whether resources are sufficient;
- whether a scheduler should choose the agent;
- whether a worker should start;
- whether an agent is trusted, reputable, certified, or rewarded.

Those decisions remain owned by later roadmap features.

## Draft Schema

The initial documentation schema is:

```text
iamine.agent.audit.draft-0.1
```

Default file name inside the skeleton:

```text
metadata/agent-audit.toml
```

This feature does not implement TOML parsing. The file name and schema are a
planning contract for later implementation.

## Required Top-Level Fields

| Field | Required | Purpose |
| --- | --- | --- |
| `schema` | yes | Audit policy schema identifier. |
| `package_id` | yes | Package that owns the audit policy. |
| `audit_profile_id` | yes | Stable audit profile identifier. |
| `audit_profile_version` | yes | Reviewable audit contract version. |
| `event_classes` | yes | Allowed future event classes. |
| `required_evidence` | yes | Evidence required before registry or runtime review. |
| `redaction_policy` | yes | Required redaction and blocked fields. |
| `retention_policy` | yes | Retention and deletion expectations. |
| `integrity_policy` | yes | Tamper-evidence expectations. |
| `access_policy` | yes | Who may view future audit evidence. |
| `failure_policy` | yes | Behavior when audit evidence is missing or unsafe. |
| `review` | yes | Human review requirements and evidence links. |

## Field Rules

### `schema`

Allowed value for this feature:

```text
iamine.agent.audit.draft-0.1
```

Unknown schema versions must block install, registry admission, and execution.

### `package_id`

The package ID must match the package manifest. It must not contain local host
identity, credentials, secrets, or private paths.

### `audit_profile_id`

The audit profile ID must be stable, lowercase, and narrow.

Allowed example:

```text
node_doctor_privacy_safe_audit
```

Blocked examples:

```text
full_logs
all_events
raw_machine_audit
operator_identity_log
mainnet_audit
```

### `event_classes`

Event classes are future labels only. They do not create runtime events.

Allowed draft classes:

```text
review_started
scope_checked
permission_checked
redaction_checked
handoff_required
refusal_recorded
human_review_recorded
```

Blocked draft classes:

```text
raw_prompt_logged
raw_output_logged
full_filesystem_snapshot
process_list_dump
credential_capture
network_packet_capture
wallet_event
mainnet_operation
```

### `required_evidence`

Required evidence must be package-relative, reviewable, and redacted. It cannot
reference user home directories, hostnames, IP addresses, raw logs, credentials,
wallet data, private paths, or private machine identifiers.

### `redaction_policy`

The redaction policy must be explicit and must block unsafe fields by default.

Required blocked fields:

- credentials;
- private keys;
- wallet keys;
- usernames;
- full hostnames;
- IP addresses;
- MAC addresses;
- serial numbers;
- disk UUIDs;
- machine IDs;
- private paths;
- raw process lists;
- unredacted logs;
- permanent hardware fingerprints.

### `retention_policy`

Retention policy is declarative only. It cannot delete files, rotate logs, or
start background jobs in this feature.

Allowed values:

```text
review_only
operator_local
delete_on_package_rejection
```

### `integrity_policy`

Integrity policy describes future tamper-evidence requirements. It cannot sign,
hash, upload, or publish artifacts in this feature.

### `access_policy`

Access policy must be least-privilege and local by default. Public publication,
third-party sharing, marketplace sharing, and mainnet sharing are blocked.

### `failure_policy`

Missing, unsafe, unredacted, contradictory, or stale audit evidence must block
install, registry admission, and execution by default.

### `review`

Review metadata must require human review before registry or runtime
eligibility. Self-attestation alone is insufficient.

## Non-Bypass Rules

- Audit policy cannot expand scope.
- Audit policy cannot create capabilities.
- Audit policy cannot grant permissions.
- Audit policy cannot authorize execution.
- Audit policy cannot implement runtime logging.
- Audit policy cannot implement sandboxing.
- Audit policy cannot start workers.
- Audit policy cannot run shell commands.
- Audit policy cannot read private files.
- Audit policy cannot load or download models.
- Audit policy cannot imply scheduler priority.
- Audit policy cannot imply node compatibility.
- Audit policy cannot imply trust, reputation, reward, or certification.
- Audit policy cannot replace capability metadata.
- Audit policy cannot replace expertise metadata.
- Audit policy cannot replace resource requirements.
- Audit policy cannot replace permission review.
- Audit policy cannot replace boundary evals.
- Audit policy cannot claim distributed model MoE.

## Dependency Boundary

This documentation feature does not add dependencies. Future source-of-truth
schema work may reuse the same dependency policy already listed for agent
metadata implementation:

```text
serde
serde_json
serde_yaml
schemars
jsonschema
thiserror
```

Dependency changes must wait for the later implementation feature that creates
source-of-truth types, generated schemas, validators, and runtime audit gates.

## Integration

This feature consumes:

```text
AGENT-CREATION-ARCHITECTURE-001
AGENT-SKELETON-STANDARD-001
AGENT-PACKAGE-MANIFEST-001
AGENT-SCOPE-MANIFEST-001
AGENT-CAPABILITY-METADATA-001
AGENT-EXPERTISE-METADATA-001
AGENT-RESOURCE-REQUIREMENTS-001
AGENT-PERMISSION-MODEL-001
```

It feeds:

```text
AGENT-REGISTRY-LOCAL-001
AGENT-SCOPE-BOUNDARY-EVALS-001
AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001
AGENT-RUNTIME-LANGUAGE-MATRIX-001
AGENT-ROUTING-CANDIDATE-SELECTION-001
```

## Failure Policy

Missing, unknown, contradictory, broad, unsafe, unredacted, stale, or
privacy-invasive audit metadata must block install, registry admission, and
execution by default.

## Risks

- Treating audit policy as runtime logging would create a false evidence
  boundary.
- Allowing raw prompts, raw outputs, private paths, or host identifiers would
  violate IAMINE privacy rules.
- Treating audit evidence as trust or reputation would bypass later gates.
- Adding parser or runtime logging behavior here would jump ahead of schema
  source-of-truth and runtime gate work.

## Recommendation

If QA confirms this feature remains documentation-only, roadmap-aligned, and
non-executable, proceed to:

```text
AGENT-REGISTRY-LOCAL-001
```
