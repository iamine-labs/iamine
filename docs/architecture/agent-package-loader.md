# AGENT-PACKAGE-LOADER-001

## State

```text
ARCHITECTURE APPROVED
DEVELOPMENT AUTHORIZED
IMPLEMENTATION COMPLETE
LOCAL VALIDATION PASSED
ARCHITECTURE CHECKPOINT PASSED
FIELD QA REQUIRED
branch: feature/agent-package-loader-001
base: 7455f30193bcb53c5362690206b3fb79aba92bbd
base tree: 84a53698274ab1d5d71b001a313f961b7ce5d8ae
runtime behavior change: bounded in-memory package loading
runtime execution change: none
```

## Objective

Implement the independent runtime owner that materializes an eligible agent
package as an immutable, bounded in-memory snapshot. Loading consumes the exact
current `PackageLoadEvidence` and preserves its already-resolved
`PackageReviewSubject`.

Loading does not execute an agent, activate a sandbox, transition lifecycle,
start a process, access the network, select a model, mutate a scheduler,
persist state, or emit an external event.

## Owner Boundary

```text
owner crate: iamine-agent-runtime
owner module: package_loader
upstream filesystem owner: PackageReferenceResolver
upstream eligibility owner: PackageLoadEvidenceAuthority
runtime executor owner: unavailable
node integration: forbidden / absent
```

`iamine-agents` remains the pure parser and policy-contract owner. The loader
does not add package behavior to `iamine-node`, nor does it change scheduler,
P2P, PubSub, model, inference, worker, controller, reputation, reward, wallet,
installer, or marketplace behavior.

## Load Contract

`PackageLoaderAuthority::load` requires:

1. the exact operator-local `PackageLoadEvidenceAuthority`;
2. the exact package-load evidence instance;
3. the exact execution-authorization authority and evidence;
4. the current authorization request and lifecycle revision.

The authority re-verifies that complete chain at load time. Foreign, stale,
cancelled, mismatched, or replayed-across-revision evidence fails closed.

Successful loading returns `LoadedAgentPackage`, bound to:

```text
operator-local loader authority identity
exact package-load evidence identity
exact reviewed package subject
exact resolved-reference snapshot
current lifecycle revision
```

## Snapshot Policy

The resolver already performed capability-relative, no-follow, race-aware,
bounded reads. Package-load evidence already parsed the exact seven resolved
references and validated their cross-file contract.

The loader retains that immutable in-memory subject. It deliberately does not
reopen package paths:

- a second path lookup could select bytes different from those reviewed and
  authorized;
- the resolved collection has no public mutation path;
- the manifest and reference snapshot remain immutably borrowed for the
  lifetime of the loaded package;
- reference count and total bytes remain available without exposing content.

Changing a source file after bounded resolution does not change the loaded
snapshot. A later execution feature must consume this loaded object and must
not reopen the original package paths.

## Public Contract

```text
PackageLoaderAuthority::new_operator_local
PackageLoaderAuthority::load
PackageLoaderAuthority::verifies
LoadedAgentPackage
LoadedAgentPackageStatus::Loaded
PackageLoaderRequirement
PackageLoaderError
```

Loaded package schema:

```text
iamine.agent.package_loader.loaded_package-0.1
```

Successful state reports:

```text
package_load_evidence_verified = true
package_loaded = true
execution_allowed = false
execution_started = false
runtime_active = false
sandbox_active = false
scheduler_mutated = false
transport_started = false
persisted = false
external_event_emitted = false
```

## Security And Privacy

- Authority and evidence identities use crate-controlled `Arc` identity.
- Caller booleans, strings, paths, or package self-claims cannot create a
  loaded package.
- Debug output redacts authority, evidence, subject, package values, and
  reference contents.
- Errors use stable codes and fixed messages without package IDs, paths, raw
  I/O errors, usernames, host identifiers, secrets, or credentials.
- Loading performs no filesystem, environment, process, network, clock,
  randomness, logging, model, inference, or persistence operation.
- Reference contents remain private and bounded by the resolver contract.

## Explicitly Out Of Scope

- reading or resolving package paths a second time;
- installing, downloading, updating, or publishing packages;
- activating a sandbox or changing lifecycle state;
- runtime execution or dispatch;
- node, worker, controller, CLI, scheduler, P2P, PubSub, model, or inference
  wiring;
- model selection, model download, model loading, or backend initialization;
- changing the static `iamine-agents` package-load assessment;
- reporting the runtime executor as available;
- closing the v0.11.2 milestone gate.

## Architecture Maintenance

```text
main.rs: 4929 lines, delta 0
cluster_registry.rs: 862 lines, delta 0
largest new production file: package_loader/loaded.rs, 147 lines
largest focused test file: package_loader.rs, 359 lines
new non-main Rust file above 750 lines: none
duplicated parser logic: none
duplicated resolver logic: none
node wiring: none
runtime executor integration: absent
```

The original `RuntimeFoundationReport` remains blocked. Its owner registry is
an integration report, so a standalone owner API does not make package access
or execution globally available.

## Validation Contract

```bash
cargo fmt --all -- --check
cargo test -p iamine-agent-runtime --test package_loader
cargo test -p iamine-agent-runtime
cargo test -p iamine-agents
cargo build -p iamine-agent-runtime
cargo clippy -p iamine-agent-runtime --all-targets -- -D warnings
./scripts/quality-gate.sh
git diff --check
git diff --cached --check
```

Field QA must validate the exact source commit on Mac, TS140, `iamine-ctrl`,
`iamine-wrk1`, `iamine-wrk2`, and `iamine-heavy`. It must run the focused
loader tests and runtime regression without starting a daemon, worker,
sandbox, transport, model, inference, or agent process.

## Risks And Decisions

- Reopening paths after authorization would create a TOCTOU gap. The loader
  retains the exact resolved snapshot instead.
- Treating loaded state as executable would bypass the next independent owner.
  Every execution and runtime flag remains false.
- Accepting evidence by value or status alone would allow provenance replay.
  Exact authority and evidence identities are required.
- Exposing package values through Debug or errors would violate privacy. All
  such fields remain redacted.
- Combining this work with the executor would create an oversized,
  unauditable integration. `AGENT-RUNTIME-EXECUTOR-001` remains separate and
  `PROPOSED`.

## Current Architecture Decision

```text
owner boundary: SATISFIED
independent gates: PRESERVED
bounded snapshot: SATISFIED
privacy contract: SATISFIED
anti-monolith guards: PASS
focused validation: PASS, 9/9
runtime regression: PASS, 137/137
agents regression: PASS, 109/109
strict crate clippy: PASS
field QA: REQUIRED / PENDING
recommendation: FIELD QA AUTHORIZED
```
