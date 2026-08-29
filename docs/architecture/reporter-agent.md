# Reporter Agent Architecture

Feature:

```text
REPORTER-AGENT-001
```

State:

```text
ARCHITECTURE APPROVED
IMPLEMENTATION COMPLETE
LOCAL VALIDATION PASSED
FIELD QA PASSED
APPROVED FOR MERGE
```

## Baseline

```text
branch: feature/reporter-agent-001
base: 65f12dc3c7b6a67489fe54e691dd30778bd6a183
base tree: 604bc770eef3374eb34858019e586653e72956a9
reconciled base: 27c4d5fb7a6f4315546a5897c5e136c3748940ad
approved candidate: 4a10d2912819592b6c5f0f7eef0b6ca6eb1a926c
approved tree: e416330a672270474bb99a55240075c72862d22d
target: develop
```

## Purpose

Implement the second functional P0 official agent as a bounded Privacy-Safe
Support Reporter. Reporter formats operator-approved, already-redacted evidence
codes into a local operator-visible report. It does not collect evidence,
diagnose hidden facts, invoke support bundles, mutate state, export reports, or
start network, model, worker, scheduler, or inference behavior.

Package identity:

```text
package_id: iamine.beta.support-reporter
task_type: support_report
scope_id: privacy_safe_support_report
mode: local_readonly
```

## Typed Input Contract

Schema:

```text
iamine.agent.reporter.input-0.1
```

The CLI accepts at most eight repeated evidence tokens:

```text
iamine-node agents reporter --package-root PATH \
  [--evidence SOURCE:STATUS:CLAIM]... [--json]
```

Allowed sources:

```text
operator_symptom_summary
redacted_diagnostic_summary
redacted_support_bundle_summary
```

Allowed statuses:

```text
observed
attention
blocked
missing
```

Allowed claim codes:

```text
node_readiness
configuration_status
model_readiness
network_readiness
runtime_health
unsupported_claim
```

Tokens are parsed into enums, bounded, deduplicated, serialized as structured
JSON, enforced as redacted input, and parsed again by the official Rust
program. Free-form evidence, paths, identifiers, logs, prompts, credentials,
host data, and unknown values fail closed. Empty evidence is valid only to
produce a missing-evidence blocked report.

## Output Contract

Schema:

```text
iamine.agent.reporter.output-0.1
```

The output contains only the schema, stable classification, typed evidence
codes, and a bounded next-step code. It never echoes CLI tokens as raw text.

Classification policy:

- complete supported evidence produces `support_report`;
- absent or explicitly missing evidence produces `blocked_action_report`;
- `unsupported_claim` produces `handoff_request`;
- invalid, broad, duplicate, oversized, or contradictory input is rejected.

The additive runtime classification `support_report` does not change existing
classification meanings.

## Package And Runtime Integration

The reviewed package lives under:

```text
agents/official/reporter/
```

Its root manifest and all seven policy-bearing references must match the
compiled canonical snapshot exactly. Package metadata keeps
`execution_authorized: false`; only the existing operator-local runtime owner
chain may establish review, compatibility, input/output, sandbox, lifecycle,
timeout, scope, permission, routing, audit, load, execution, and result
verification evidence.

The common local-readonly official-agent composition moved from Node Doctor to
a dedicated `official_agent_execution` module. Node Doctor and Reporter supply
immutable agent-specific specs and program registrars. The specs also carry an
explicit bounded execution timeout: Reporter remains at 1,000 ms and Node
Doctor uses 5,000 ms for its platform-dependent local evidence collection. No
other timeout class or authority check changed.

Reporter implementation is split under:

```text
iamine-node/src/reporter_agent/
```

`iamine-node/src/main.rs` remains wiring only. `cluster_registry.rs` remains
unchanged.

## Security And Privacy

- Reporter is pre-network and log-free.
- It never calls `iamine-node support bundle` or reads bundle files.
- A support-bundle source token means only that an operator supplied an
  approved, redacted summary code.
- No arbitrary text, filesystem access, shell, child process, persistence,
  export, upload, service mutation, hardware probe, model operation, worker,
  P2P, PubSub, scheduler, inference, credential, wallet, or host identifier is
  allowed.
- User confirmation cannot elevate blocked actions.
- Missing, unknown, contradictory, stale, broad, or privacy-invasive metadata
  fails closed.
- Runtime output must explicitly report no scheduler mutation, transport
  startup, persistence, or OS-isolation claim.

## Compatibility And Non-Regression

The feature is additive and preserves CPU-only, accelerated, macOS, Linux,
VM, container, mock-worker, cgroup, and constrained-host behavior. It does not
change Node Doctor evidence or output contracts, support-bundle behavior,
scheduler, P2P, PubSub, workers, models, inference, hardware profiling,
registry publication, installer, marketplace, reputation, rewards, settlement,
or public-beta state.

## Validation Gate

Local validation must cover:

```text
package and all referenced metadata
nine canonical boundary-eval classes
typed input parse and bounds
positive support report
missing-evidence blocked report
unsupported-claim handoff
privacy and no-echo behavior
altered package and manifest fail-closed behavior
scope and permission enforcement
runtime authorization, audit, cleanup, and no-side-effect fields
Node Doctor non-regression through the extracted shared composition
quality gate and architecture size guards
```

Field QA is required because this feature adds executable agent and CLI
behavior. Validate the exact commit on Mac, TS140, and the four
Proxmox/R5500 roles. Every role must prove pre-network local-only behavior,
structured output, package integrity, privacy, cleanup, and zero transport,
scheduler, persistence, model, worker, or inference side effects.

## Out Of Scope

```text
raw or free-form report input
support-bundle invocation or file ingestion
report persistence, export, upload, publication, or third-party contact
automatic evidence collection or diagnosis
model-backed prose generation
LAN or remote execution
installer or registry discovery
OS-level sandbox claims
v0.12.0 milestone closure
authorization of later P0 agents
```

## Architecture Review Decision

The final diff remains bounded to the official Reporter package, its typed CLI
and runtime integration, the shared local-readonly composition, Node Doctor
compatibility wiring, tests, and evidence. It does not modify `iamine-core`,
`iamine-models`, `iamine-network`, dashboard product code, scheduler, P2P,
PubSub, workers, models, inference, reputation, rewards, or settlement.

The exact candidate passed the complete local quality gate, Mac Field QA,
TS140, the Proxmox host inventory gate, and all four Proxmox guests. Privacy,
fail-closed boundaries, cleanup, no-side-effect fields, process counts, and
source cleanliness passed on every execution role. Architecture therefore
authorizes only the controlled merge of candidate `4a10d291...` into current
canonical `develop`, subject to the standard pre-push fetch/reconciliation and
post-merge validation.

## Next Candidate

`LAN-FILE-SHARE-ASSISTANT-AGENT-001` remains `PROPOSED` and requires separate
Architecture authorization.
