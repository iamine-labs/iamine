# AGENT-POLICY-METADATA-VALIDATORS-001

## State

```text
READY FOR MERGE REVIEW
branch: feature/agent-policy-metadata-validators-001
base: 69e9f223924c0a41d90dd18e7585db475777a599
base tree: 7283ccd59e7284185b9b1a0664e1e4262de4b992
runtime behavior change: none
field QA: not required; pure in-memory parsing and validation only
recommendation: READY FOR ARCHITECTURE MERGE REVIEW
```

## Objective

Implement independent, fail-closed YAML validators for the Scope, Permission,
and Audit metadata references declared by `agent.yaml`. The feature establishes
typed contracts and generated JSON Schemas without resolving references,
opening package directories, loading packages, authorizing execution, or
starting an agent runtime.

## Owner Boundaries

The feature owns only `iamine-agents/src/policy_metadata/`:

```text
scope.rs       -> scope metadata contract and semantic validation
permission.rs  -> permission metadata contract and semantic validation
audit.rs       -> audit metadata contract and semantic validation
validation.rs  -> bounded shared syntax, identifier, collection, and path rules
error.rs       -> redacted error and violation types
```

`iamine-agents/src/manifest/` remains the root `agent.yaml` owner. It continues
to validate only its own shape and safe relative reference strings.
`iamine-agents/src/package_load.rs` remains always blocked. The new validators
do not change `PackageLoadStatus`, remove any blocker, or accept caller-supplied
evidence as authorization.

## Parsing Contract

Each policy receives YAML content supplied by its caller. The canonical local
validation sequence is:

```text
bounded input
-> YAML structural parse
-> generated JSON Schema validation
-> typed deserialization
-> semantic validation
-> typed metadata or redacted failure
```

The maximum input is `64 KiB`. Errors expose stable codes, fields, and static
messages only; they do not retain, echo, log, or serialize untrusted YAML,
paths, secrets, host identifiers, or other private values.

The public entry points are:

```text
parse_scope_policy_yaml
scope_policy_json_schema
parse_permission_policy_yaml
permission_policy_json_schema
parse_audit_policy_yaml
audit_policy_json_schema
```

Historical TOML examples remain planning artifacts. This feature owns the
canonical typed YAML input boundary only. A later package-reference resolver
will decide which package-relative bytes, if any, may be supplied to these
parsers.

## Scope Metadata

Schema identifier:

```text
iamine.agent.scope.draft-0.1
```

The scope validator requires a narrow package and scope identity, semantic
version, explicit task/input/operation boundaries, future permission
requirements, confirmation, handoff, orchestrator return conditions, boundary
eval requirements, and independent review.

It rejects broad identifiers, missing required deny lists, overlapping allowed
and blocked boundaries, missing orchestrator handoff, missing eval classes, a
missing permission-model requirement, and any self-approval claim.

## Permission Metadata

Schema identifier:

```text
iamine.agent.permissions.draft-0.1
```

The permission validator requires denial by default, narrow currently supported
requested categories, complete forbidden categories and blocked actions,
privacy-safe data access, bounded network/filesystem declarations, absent
process access, escalation to an orchestrator or human review, and human
review.

It does not evaluate a task, grant a permission, show a confirmation prompt,
or invoke the existing in-memory permission enforcement boundary.

## Audit Metadata

Schema identifier:

```text
iamine.agent.audit.draft-0.1
```

The audit validator requires a bounded event-class list, package-relative
evidence references, redact-by-default policy, review-only operator-local
retention, future tamper-evidence without artifact publication, local-only
access, blocking failure policy, and human review.

It does not create an audit event, persist a log, hash an artifact, or treat an
event as execution or trust evidence.

## Integration Sequence

```text
AGENT-POLICY-METADATA-VALIDATORS-001
-> AGENT-DESCRIPTIVE-METADATA-VALIDATORS-001
-> AGENT-BOUNDARY-EVAL-VALIDATOR-001
-> AGENT-RUNTIME-CORE-001
-> AGENT-PACKAGE-REFERENCE-RESOLVER-001
-> AGENT-PACKAGE-REVIEW-EVIDENCE-001
```

The package-reference resolver must later supply bounded package-relative
contents. The evidence integration feature may later consume successful typed
results, but absence, contradiction, or unknown policy metadata remains
blocked until every independent owner produces its own evidence.

## Risks

- Resolving a reference or reading a package here would mix validation with I/O.
- Reusing a policy declaration as execution authorization would create a
  fail-open path.
- Merging Scope, Permission, and Audit into one type would erase ownership and
  make future gate review ambiguous.
- Debug-printing arbitrary metadata could expose private values.
- Treating a historical TOML planning example as package-load evidence would
  bypass the future resolver and evidence-integration gates.

## Field QA Decision

Field QA is not required for this feature because no platform, filesystem,
runtime, worker, scheduler, P2P, model, or inference behavior changes. The
future reference resolver, sandbox, lifecycle, package loader, and runtime
executor require the Mac, TS140, and Proxmox/R5500 matrix before merge review.
