# AGENT-RUNTIME-COMPATIBILITY-GATE-001

## State

```text
MERGED / VALIDATED / CLOSED
branch: feature/agent-runtime-compatibility-gate-001
base: a83e08effdb5c67ec8a0ac411f7c489fb44f466e
base tree: 884b6f921094fbc4e41fad5484ae304b11437311
feature commit: 933f15fa41395fe4d18bd8cc4b4c7a3fe95dea7e
feature tree: 5182dd9faab77d4b943d1b20b18f9536b2f34c3f
QA evidence commit: b4f50b12dcc1e3030a2700f990ffa4265785c068
QA evidence tree: 5f6f15eee580875fadd1e6862116b5431e8fea18
merge commit: 40a9a8074d8138204b6f4c1dd4c787d1d97fb219
merge tree: 5f6f15eee580875fadd1e6862116b5431e8fea18
runtime behavior change: passive in-memory compatibility evidence
local focused validation: passed
quality gate: PASS WITH WARNINGS
architecture checkpoint: passed
field QA: passed on Mac, TS140, and Proxmox/R5500
final architecture review: passed
post-merge validation: passed with accepted baseline and environment exceptions
```

## Objective

Establish typed, fail-closed evidence for runtime-language and resource
compatibility after package review evidence exists. The decision is bound to
the exact reviewed manifest and resolved package references plus an immutable
operator-local compatibility authority.

This feature does not authorize package loading, sandbox startup, permissions,
network access, model access, worker startup, scheduling, or agent execution.

## Ownership

The implementation belongs to `iamine-agent-runtime`:

```text
runtime_compatibility/authority.rs
runtime_compatibility/configuration.rs
runtime_compatibility/error.rs
runtime_compatibility/evaluation.rs
runtime_compatibility/evidence.rs
```

`iamine-agents` continues to own typed resource metadata. Its
`ResourceOperatingMode::as_str` identifier is exposed for exact typed map
lookup; parsing and validation behavior do not change.

No compatibility logic is added to `iamine-node`, `main.rs`,
`cluster_registry.rs`, the hardware profiler, scheduler, worker, network,
model, inference, package-load, or execution owners.

## Trust Boundary

`RuntimeCompatibilityAuthority` is an operator-local in-memory capability. It
contains:

- one typed runtime-language decision;
- one bounded resource envelope assigned by the operator;
- one private identity used to verify emitted evidence.

The authority first requires `PackageReviewAuthority` to verify review evidence
for the exact `PackageReviewSubject`. The subject already binds the exact
manifest object and exact resolved-reference object. A different review
authority, compatibility authority, manifest object, or cloned resolution
fails verification.

Package bytes cannot deserialize or construct either authority, the runtime
decision, the resource envelope, or compatibility evidence. Later consumers
must retain operator-configured authority instances; accepting authorities
supplied alongside package content is forbidden.

## Runtime Language Decision

The typed matrix contains every mode from
`AGENT-RUNTIME-LANGUAGE-MATRIX-001`:

```text
rust_native_official
rust_metadata_validator
python_sdk_tooling
typescript_sdk_tooling
wasm_wasi_sandboxed_agent
container_sandboxed_agent
arbitrary_shell_agent
unrestricted_filesystem_agent
mainnet_wallet_agent
```

Only `rust_native_official` with availability `available` can establish
compatibility evidence. `unavailable`, `deferred`, and `blocked` fail closed.
Every other mode remains non-executable even if a caller labels it available.

`available` means only that this compatibility decision recognizes the
operator-selected mode. It does not mean an executor, sandbox, loader,
interpreter, package manager, or dependency installer exists.

## Resource Decision

The evaluator parses only the already resolved `ResourceRequirementsMetadata`
reference and verifies:

- resource package ID matches the reviewed manifest;
- manifest execution mode exists in the resource declaration;
- assigned logical cores meet the declared minimum;
- assigned memory can contain the declared maximum working set;
- assigned storage can contain package, temporary, and cache budgets;
- assigned network availability can satisfy the declared network mode.

The envelope contains normalized limits only. It does not contain hostnames,
paths, addresses, device identifiers, process data, hardware inventories, or
permanent fingerprints. It performs no hardware, filesystem, process,
environment, network, cgroup, container, model, or backend probe.

Optional accelerators and model backend declarations do not decide
compatibility here. Hardware, backend, model, permission, and scheduler gates
remain independent owners.

## Public Contract

- `RuntimeCompatibilityAuthority`: evaluates and verifies authority-bound
  compatibility evidence.
- `RuntimeLanguageDecision`: typed runtime mode and availability.
- `RuntimeResourceEnvelope`: normalized operator-assigned resource limits.
- `RuntimeCompatibilityEvidence`: opaque positive evidence for the exact
  subject.
- `RuntimeCompatibilityRequirement`: stable independent requirement labels.
- `RuntimeCompatibilityError`: privacy-safe stable failure code and owner.

Established evidence reports:

```text
load_allowed: false
execution_allowed: false
```

## Non-Bypass Rules

- Compatibility requires exact package review evidence first.
- Package content cannot select or forge trusted local capacity.
- Compatibility does not grant scope or permissions.
- Network availability is not network authorization.
- Resource compatibility is not scheduler placement.
- Optional accelerators do not imply hardware eligibility or rewards.
- Backend declarations do not imply model or backend availability.
- Compatibility does not remove package-load blockers.
- Compatibility does not install, load, persist, publish, or execute.
- No node, worker, P2P, PubSub, model, inference, reward, reputation, wallet,
  marketplace, public-beta, or mainnet behavior changes.

## Privacy

Authority and resource-envelope Debug output redact configuration. Evidence
redacts authority and subject identities. Errors expose only static codes,
requirements, and messages.

Diagnostics do not expose package identifiers, resource values, paths,
reference contents, host identifiers, credentials, secrets, hardware
inventories, prompts, outputs, or process lists.

## Integration

```text
AGENT-PACKAGE-REVIEW-EVIDENCE-001
-> AGENT-RUNTIME-COMPATIBILITY-GATE-001
-> AGENT-INPUT-OUTPUT-ENFORCEMENT-001
```

Only later evidence-integration and execution-authorization owners may combine
this result with independent gates. This feature does not modify the static
package-load report.

## Field QA

The root workflow requires field QA because this feature changes the runtime
crate and emits a platform-relevant resource decision. The exact source commit
must run on:

- Mac development host;
- TS140;
- `iamine-ctrl`;
- `iamine-wrk1`;
- `iamine-wrk2`;
- `iamine-heavy`.

Tests use explicit synthetic envelopes. Field QA must not infer envelope values
from private host identity, start `iamine-node`, probe hardware, load models, or
open network runtimes.

## Risks

- Treating a caller-created authority as operator configuration.
- Treating compatibility as package-load or execution authorization.
- Treating network availability as a permission grant.
- Trusting a resource file that is not bound to reviewed references.
- Reusing evidence after the manifest, resolution, or authority changes.
- Adding hardware probes or scheduler policy to this owner.
- Combining later sandbox or authorization logic into this module.

## Success Criteria

- Only reviewed Rust official packages with sufficient resources establish
  evidence.
- Every non-executable language mode and unavailable state fails closed.
- CPU, memory, storage, network, mode, package, metadata, authority, and subject
  mismatches fail at stable owners.
- Debug and errors remain privacy-safe.
- Package-load blockers and execution remain unchanged.
- `main.rs` and `cluster_registry.rs` do not change.
- Focused, workspace, quality, and field QA gates pass or receive an explicit
  evidence-backed classification.

## Architecture Checkpoint

```text
owner crate and module boundary: PASS
exact review and subject binding: PASS
operator-local compatibility authority: PASS
runtime language fail-closed matrix: PASS
resource dimension independence: PASS
privacy-safe diagnostics: PASS
package-load and execution non-bypass: PASS
main.rs delta: 0
cluster_registry.rs delta: 0
largest new production module: 184 lines
local quality gate: PASS WITH WARNINGS
decision: FIELD QA AUTHORIZED
```

The quality-gate warning is environmental. The optional workspace Clippy pass
exhausted the Mac temporary target volume after every required check,
including `cargo test --workspace`, had passed. Strict Clippy for
`iamine-agent-runtime` passed with `-D warnings`.

## Final Architecture Review

```text
exact executable source identity: PASS
local focused validation: PASS, 25/25
required quality gate: PASS
Mac field QA: PASS
TS140 field QA: PASS
Proxmox/R5500 field QA: PASS, 4/4 guests
canonical remote work preservation: PASS
runtime side effects: none observed
product failures: none
environment findings: Mac temporary target disk exhaustion
harness findings: Mac process inspection sandbox; TS140 Cargo shell initialization
merge conflicts evaluated: none
decision: MERGED / VALIDATED / CLOSED
```

The environmental and harness findings do not weaken the product evidence.
Mac reported no `iamine-node` process after the focused run; every remote host
reported `0 -> 0`. All six hosts ran the exact source commit and tree. No test
read local hardware, started a runtime, opened network services, loaded a
model, or changed package-load behavior.

## Closure

The controlled no-fast-forward merge landed in `develop` as `40a9a80`. Its
tree is identical to the reviewed QA evidence tree. Post-merge focused
validation passed:

```text
cargo fmt --all -- --check: PASS
cargo test -p iamine-agent-runtime: PASS, 25/25
cargo test -p iamine-agents: PASS, 109/109
cargo clippy -p iamine-agent-runtime --all-targets -- -D warnings: PASS
git diff --check: PASS
```

The broad quality-gate script reported three required command failures. They
are accepted baseline and environment exceptions, not product regressions:

- the base and merge contain identical `iamine-models` source, manifest, and
  lock data; the base suite reproduced four real Metal inference failures;
- all four affected real-inference tests passed in isolated processes on the
  merge, and the merge also produced a complete `158/158` package pass;
- the only node failure was the Codex sandbox denying creation of a Unix
  daemon socket; the exact test passed outside the sandbox.

Workspace Clippy passed with historical warnings. `cargo audit`, `cargo deny`,
and `gitleaks` remained unavailable. No production file changed after the
reviewed merge.

The next independent executable feature is
`AGENT-INPUT-OUTPUT-ENFORCEMENT-001`.
