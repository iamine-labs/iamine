# AGENT-MANIFEST-PARSER-VALIDATOR-001

## Objective

Implement the first executable IAMINE agent contract boundary: canonical Rust
types, generated JSON Schema, bounded YAML parsing, and deny-by-default
semantic validation for the root `agent.yaml` package manifest.

This feature validates metadata. It does not make an agent executable.

## Ownership

The new `iamine-agents` crate owns root agent-manifest types and validation.
`iamine-node`, `iamine-network`, `iamine-models`, and `iamine-hardware` do not
own this parser and receive no new wiring in this feature.

```text
iamine-agents/src/manifest/schema.rs
iamine-agents/src/manifest/error.rs
iamine-agents/src/manifest/validation.rs
iamine-agents/src/manifest/mod.rs
```

This keeps `iamine-node/src/main.rs` and
`iamine-node/src/cluster_registry.rs` unchanged.

## Canonical Surface

The only accepted root authoring surface is:

```text
agent.yaml
```

The source and validation order is:

```text
Rust types
-> generated JSON Schema
-> YAML structural validation
-> typed deserialization
-> semantic validation
```

The schema identifier remains:

```text
iamine.agent.package.draft-0.1
```

The old `iamine-agent-package.toml` name was a non-executable planning
contract. This feature does not implement a TOML root parser or an automatic
format fallback.

## Public API

```text
manifest_json_schema
parse_and_validate_yaml
validate_manifest
AgentPackageManifest
ManifestError
ManifestViolations
```

`parse_and_validate_yaml` accepts a string. It does not accept a path and
performs no filesystem access.

## Validation Rules

Structural validation requires all declared fields, rejects unknown fields,
checks enum values, and uses the generated JSON Schema derived from the Rust
types.

Semantic validation enforces:

- the exact draft schema identifier;
- bounded IAMINE-scoped package IDs;
- semantic package versions;
- bounded single-line display text and summary;
- bounded, unique personas;
- bounded package-relative references without traversal or platform-absolute
  paths;
- `execution_authorized: false`;
- local development and manual review distribution only;
- no public beta, marketplace, or third-party publication;
- no credentials, host identifiers, network, destructive actions, arbitrary
  shell, or unrestricted filesystem claims;
- every required human, scope, permission, audit, and boundary-test review
  gate.

Input is limited to 64 KiB. Validation errors expose stable codes, field names,
and static messages without echoing manifest values.

## Referenced Contract Boundary

Reference values are validated only as bounded package-relative paths. Their
extensions, existence, contents, schemas, and semantic validity remain owned
by their respective roadmap features.

The parser does not open references or combine package gates. This preserves
the rule that package, scope, capability, expertise, resources, permissions,
audit, and boundary evals remain independent.

Historical child-metadata documents may still contain TOML planning examples.
They are not accepted as root manifests and are not parsed by this feature.
Each child schema must reconcile its authoring format in its own implementation
feature before package loading can exist.

## Runtime Boundary

This feature does not:

- read or write package files;
- discover package roots;
- follow manifest references;
- validate referenced metadata contents;
- install or load packages;
- register agents;
- enforce scope or permissions at execution time;
- emit audit events;
- start a sandbox, lifecycle, worker, scheduler, model, or network service;
- invoke `iamine-node lan doctor`;
- authorize functional `NODE-DOCTOR-AGENT-001`.

## Dependency Decision

The crate uses the dependency class already allowed by
`AGENT-DEPENDENCY-POLICY-001`:

```text
serde
serde_json
serde_yaml
schemars
jsonschema
thiserror
semver
```

`jsonschema` uses the current `0.48` line. `serde_yaml 0.9` is marked deprecated
upstream but remains the dependency explicitly named by the canonical roadmap;
replacement requires a separate dependency-policy decision.

## Integration

This feature consumes the closed package-manifest, schema-source, language,
dependency, capability, expertise, scope, resource, permission, audit, and
boundary-eval contracts.

It feeds:

```text
AGENT-PACKAGE-LOAD-GATE-001
AGENT-SCOPE-ENFORCEMENT-001
AGENT-PERMISSION-ENFORCEMENT-001
AGENT-AUDIT-EVENTS-001
```

None of those downstream features is implemented or authorized here.
