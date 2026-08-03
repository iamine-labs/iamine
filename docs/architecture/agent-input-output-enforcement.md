# AGENT-INPUT-OUTPUT-ENFORCEMENT-001

## State

```text
MERGED / VALIDATED / CLOSED
branch: feature/agent-input-output-enforcement-001
base: 025a8423fd9111e8efc642548a8b1a4b5dcbf2e7
base tree: e249ce7239eeb2329012a332240ed5900dc46368
source commit: 2043ade58896f1b2b66bf98a0856b824c2abe6c9
source tree: ecd79cb66884c257cb6fc6bf8ec2d87836dba1e6
QA evidence commit: c2d75b508d8e4b11ec406bceaeb113e5c5f220ca
QA evidence tree: 684c1101e70bf8b78020accf6bec57d3089299d2
merge commit: 1ec29389c0c955996aae0a492457f70a46e72096
merge tree: 684c1101e70bf8b78020accf6bec57d3089299d2
local validation: PASS
quality gate: PASS WITH WARNINGS
architecture checkpoint: PASS
runtime behavior change: passive in-memory input/output enforcement
field QA: not required for this in-memory boundary
final architecture review: PASS
post-merge validation: PASS
```

## Objective

Establish typed, bounded, fail-closed input and output records after exact
runtime compatibility evidence exists. The decision remains bound to the
reviewed manifest, resolved package references, compatibility authority, and
an operator-local input/output authority.

This feature does not redact raw content by itself. An operator-controlled
caller must first attest that content is already redacted. It does not load,
execute, persist, transport, publish, hand off, schedule, or install anything.

## Ownership

The implementation belongs to `iamine-agent-runtime`:

```text
input_output_enforcement/authority.rs
input_output_enforcement/error.rs
input_output_enforcement/evaluation.rs
input_output_enforcement/evidence.rs
input_output_enforcement/policy.rs
input_output_enforcement/record.rs
input_output_enforcement/redaction.rs
```

No behavior is added to `iamine-node`, `main.rs`, `cluster_registry.rs`,
workers, schedulers, P2P, PubSub, models, inference, hardware profiling, audit
storage, package loading, sandboxing, or execution.

## Trust Boundary

`InputOutputEnforcementAuthority` is an operator-local in-memory capability.
It first requires the exact `RuntimeCompatibilityAuthority` to verify the
exact `RuntimeCompatibilityEvidence` and `PackageReviewSubject`.

Established enforcement evidence has a private identity. Every
`OperatorRedactedInput` and `OperatorRedactedOutput` is bound to one exact
evidence instance. A different authority, manifest object, reference
resolution, subject, evidence instance, or unattested string fails closed.

Package content cannot construct authorities, evidence, redaction
attestations, or enforced records because their constructors and identities
are private to the owner module.

## Policy

`InputOutputPolicy` defines:

- a non-zero input byte limit;
- a non-zero output byte limit;
- whether redacted outputs may be operator-visible.

Each configured limit is capped at 64 KiB. Content must be non-empty, remain
within its configured byte limit, and contain no control characters.

The policy does not inspect or infer whether content is private. The
operator-controlled caller owns redaction and may mint an opaque attestation
only through the exact enforcement authority and evidence.

## Record Contract

The schema identifier is:

```text
iamine.agent.input_output.enforced-0.1
```

Records derive `agent_id`, `task_type`, and `scope_id` from the exact reviewed
manifest and its validated scope reference. They do not accept those fields
from record content.

Input classifications:

```text
task_descriptor
operator_intent
declared_scope
permission_grant_reference
resource_hint
risk_hint
context_pointer
```

Output classifications:

```text
result_summary
action_report
diagnostic_report
blocked_action_report
clarification_request
handoff_request
refusal_report
error_report
```

Every record reports `operator_attested` redaction. Input visibility is false.
Output visibility follows only the operator policy. All records keep
persistence, transport, and handoff false. Output records also keep execution
success false, including the handoff and action classes.

## Non-Bypass Rules

- Exact runtime compatibility evidence is required first.
- Enforcement evidence does not imply a per-record redaction attestation.
- A redaction attestation cannot be reused across evidence instances.
- Classification does not grant scope, permission, handoff, or execution.
- Operator visibility does not grant persistence or transport.
- A handoff request is data, not authorization to hand off.
- An action report is data, not proof that an action executed.
- Enforcement does not remove package-load blockers.
- No package, sandbox, process, model, worker, or network runtime starts.

## Privacy

Debug output redacts authorities, evidence identities, subjects, record
identifiers, policies, and content. Errors expose only static codes,
requirements, and messages. No diagnostic includes package values, scope
values, record content, paths, host identifiers, credentials, prompts,
outputs, process lists, or hardware fingerprints.

## Integration

```text
AGENT-RUNTIME-COMPATIBILITY-GATE-001
-> AGENT-INPUT-OUTPUT-ENFORCEMENT-001
-> AGENT-RUNTIME-SANDBOX-ENFORCEMENT-001
```

The static `InputOutputEnforcementUnavailable` package-load blocker remains in
place. A later evidence-integration owner may consume this evidence only after
the sandbox and remaining independent gates exist.

## Field QA Decision

Field QA is not required. The feature is a deterministic in-memory validator
and record boundary. It does not read platform state, create files, bind
sockets, start processes, inspect hardware, load models, or change node,
worker, scheduler, network, model, or inference behavior.

Mac, TS140, and Proxmox/R5500 would exercise the same synthetic Rust tests and
would not add platform evidence. This decision follows the v0.11.2
reconciliation for pure validators.

## Risks

- Treating caller assertions as proof that a redaction engine ran.
- Exposing an operator authority to package-controlled code.
- Reusing an attestation for another evidence instance.
- Treating output classification as execution success.
- Treating operator visibility as persistence or transport permission.
- Combining sandbox, lifecycle, audit, or handoff logic into this owner.

## Architecture Checkpoint

```text
owner crate and module boundary: PASS
exact compatibility and subject binding: PASS
operator-local authority: PASS
per-record opaque redaction attestation: PASS
typed input and output classes: PASS
bounded content and policy: PASS
privacy-safe diagnostics: PASS
package-load and execution non-bypass: PASS
main.rs delta: 0
cluster_registry.rs delta: 0
largest new production module: 254 lines
decision: MERGED / VALIDATED / CLOSED
```

## Final Architecture Review

The QA evidence tree and merge tree are identical. The controlled merge has two
parents, introduces no conflict-resolution delta, and preserves every
independent package-load and execution blocker.

Post-merge validation passed on the exact merge commit:

```text
scripts/quality-gate.sh: PASS WITH WARNINGS
required failures: 0
cargo test --workspace: PASS, 1005/1005
cargo clippy -p iamine-agent-runtime --all-targets -- -D warnings: PASS
main.rs delta: 0
cluster_registry.rs delta: 0
```

The workspace warnings are historical warnings in unchanged crates. Optional
`cargo audit`, `cargo deny`, and `gitleaks` tools were unavailable and reported
as skipped. The next registered feature is
`AGENT-RUNTIME-SANDBOX-ENFORCEMENT-001`; it remains `PROPOSED` until its own
Architecture and development authorization checks complete.
