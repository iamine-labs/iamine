# AGENT-DESCRIPTIVE-METADATA-VALIDATORS-001

## State

```text
MERGED / VALIDATED / CLOSED
source branch: feature/agent-descriptive-metadata-validators-001
base: 4d854add37f11329bcb5519b32db3ab5cdeca07b
base tree: c3fdd27e9246516ff6b0fff398651063becfcafa
feature commit: ee8bd3baef2c95e8371397f64b2fd68d3000dce1
merge commit: b2ae7f2a3bb9f142541402346e6fe22be272f18b
tree: 942b69451487349ed4314c3e05ecb0e021a89cbc
runtime behavior change: none
field QA: not required; pure in-memory parsing and validation only
quality gate: PASS WITH ACCEPTED HARNESS EXCEPTION
post-merge validation: passed
```

## Objective

Implement independent, fail-closed YAML validators for the Capability,
Expertise, and Resource metadata references declared by `agent.yaml`. The
feature establishes typed contracts and generated JSON Schemas without
resolving package references, inspecting hardware, evaluating compatibility,
loading packages, authorizing execution, or starting an agent runtime.

## Owner Boundaries

The feature adds three separate owners under
`iamine-agents/src/descriptive_metadata/`:

```text
capability.rs  -> bounded capability declarations
expertise.rs   -> narrow domain claims, evidence, evals, and freshness
resource.rs    -> bounded resource declarations by operating mode
validation.rs  -> descriptive identifier, collection, and path rules
error.rs       -> redacted descriptive error and violation types
```

`iamine-agents/src/metadata_parser.rs` owns only the shared structural
sequence used by Policy and Descriptive metadata. It does not know feature
semantics, perform I/O, or convert a successful parse into evidence.

`iamine-agents/src/package_load.rs` remains always blocked. This feature does
not remove `CapabilityMetadataValidatorUnavailable`,
`ExpertiseMetadataValidatorUnavailable`, or
`ResourceRequirementsValidatorUnavailable`; the future resolver and evidence
integration features must establish trusted package-relative inputs first.

## Parsing Contract

Each parser receives YAML content supplied by its caller:

```text
bounded input
-> YAML structural parse
-> generated JSON Schema validation
-> typed deserialization
-> owner-specific semantic validation
-> typed metadata or redacted failure
```

The maximum input is `64 KiB`. Unknown fields fail at the generated JSON
Schema boundary. Error values expose stable codes, static fields, and static
messages only; they do not retain or echo YAML, paths, secrets, host identity,
or private values.

The public entry points are:

```text
parse_capability_metadata_yaml
capability_metadata_json_schema
parse_expertise_metadata_yaml
expertise_metadata_json_schema
parse_resource_requirements_yaml
resource_requirements_json_schema
```

Historical TOML examples remain planning artifacts. This feature owns the
canonical typed YAML content boundary only. It does not select or read a file.

## Capability Metadata

Schema identifier:

```text
iamine.agent.capabilities.draft-0.1
```

The validator requires narrow task, operation, input, output, mode,
limitation, risk, and review declarations. Only `local_readonly`,
`local_planning`, and `lan_readonly` are structural mode values.

It rejects broad or unsafe operations, privacy-sensitive input classes,
promissory output classes, duplicate declarations, authority or trust claims,
self-approval, and unsafe review evidence paths. Capability metadata remains a
claim; it is not scope, permission, trust, routing, or execution evidence.

## Expertise Metadata

Schema identifier:

```text
iamine.agent.expertise.draft-0.1
```

The validator requires a narrow domain, task families, existing capability
references, non-promissory claims, package-relative typed evidence, explicit
limitations, bounded freshness, and independent review.

Its eval declaration must cover in-domain, adjacent-domain refusal, dangerous
refusal, ambiguity handoff, capability mismatch, stale metadata, privacy
rejection, prompt injection, and role confusion. The parser validates only the
declaration; it does not execute those evals or certify expertise.

## Resource Requirements

Schema identifier:

```text
iamine.agent.resources.draft-0.1
```

CPU, memory, storage, and network maps must exactly match the declared
operating modes. Numeric values are unit-explicit, internally ordered, and
bounded. Network declarations cannot open ports or download artifacts; model
declarations cannot authorize download or loading; accelerators cannot be
mandatory in this phase.

Constraint and privacy fields must explicitly deny dynamic probes, unrestricted
filesystem access, background downloads, model loading, service or VM
mutation, worker startup, scheduler override, runtime priority, raw hardware
inventory, permanent fingerprints, private paths, host identity, and
credentials.

These declarations do not inspect hardware and do not decide compatibility,
backend availability, scheduler placement, worker startup, or model admission.

## Integration Sequence

```text
AGENT-POLICY-METADATA-VALIDATORS-001
-> AGENT-DESCRIPTIVE-METADATA-VALIDATORS-001
-> AGENT-BOUNDARY-EVAL-VALIDATOR-001
-> AGENT-RUNTIME-CORE-001
-> AGENT-PACKAGE-REFERENCE-RESOLVER-001
-> AGENT-PACKAGE-REVIEW-EVIDENCE-001
```

Successful typed metadata may become an input to later evidence integration,
but it cannot bypass missing Scope, Permission, Audit, boundary eval, review,
compatibility, sandbox, lifecycle, authorization, loader, or executor owners.

## Risks

- Treating descriptive claims as trusted evidence would create a fail-open
  package path.
- Treating resources as compatibility would bypass hardware and backend gates.
- Treating expertise as certification or routing priority would bypass review.
- Resolving references here would mix pure contracts with package I/O.
- Combining the three contracts would erase independent ownership.
- Duplicating the structural parser would let Policy and Descriptive metadata
  drift; the internal shared parser prevents that without sharing semantics.

## Field QA Decision

Field QA is not required because no platform, filesystem, runtime, worker,
scheduler, P2P, model, inference, or hardware behavior changes. The future
reference resolver, compatibility gate, sandbox, loader, and executor require
the Mac, TS140, and Proxmox/R5500 matrix before merge review.
