# AGENT-RUNTIME-EXECUTOR-001

## State

```text
ARCHITECTURE APPROVED
DEVELOPMENT AUTHORIZED
IMPLEMENTATION COMPLETE
LOCAL VALIDATION PASSED
ARCHITECTURE CHECKPOINT PASSED
FIELD QA AUTHORIZED
FIELD QA PASSED
FINAL ARCHITECTURE REVIEW PASSED
READY FOR MERGE REVIEW
branch: feature/agent-runtime-executor-001
base: b5aaf292f71cf7a3b243fc2780bac5f95c8223d6
base tree: a3085fafb2e9f28d26b1a0430aa5e3ffd287ce8f
source commit: df6b9037994822db3677e13175184e81a9dcff58
source tree: 4a37be4da2e42f4f8cc48004346e034377eb3856
runtime behavior change: synchronous execution of registered official Rust programs
node integration: none
```

## Objective

Implement the final independent v0.11.2 runtime owner. The executor consumes
an exact authorized and loaded package, validates the current runtime evidence
chain, and invokes one operator-registered official Rust program through
lifecycle, timeout, input/output, sandbox-adapter, and audit owners.

Package bytes are never interpreted as code. The current manifest has no
executable entrypoint contract, so this baseline does not load arbitrary
native, script, WASM, or process-based code.

## Owner Boundary

```text
owner crate: iamine-agent-runtime
owner module: runtime_executor
program mode: RustNativeOfficial
program trust owner: operator-local OfficialRustProgramRegistry
package bytes executed: no
node, worker, scheduler, or transport wiring: no
OS process sandbox: no
```

`iamine-agents` remains the pure package and policy-contract owner. The
executor does not add behavior to `iamine-node` and does not change scheduler,
P2P, PubSub, models, inference, workers, controllers, reputation, rewards,
wallets, installers, or marketplace behavior.

## Execution Chain

Preparation requires:

1. the exact `LoadedAgentPackage`;
2. its loader authority and package-load evidence;
3. the exact execution authorization and request;
4. an official Rust program registered for the exact reviewed subject;
5. the current `ScopeCheck` lifecycle revision.

`RuntimeExecutorAuthority::prepare` re-verifies the loader and authorization
chain and returns a non-cloneable, one-shot `RuntimeExecutionPermit`.

Execution then requires:

1. the same executor authority, program registry, and program identity;
2. the exact lifecycle record and permit revision;
3. the exact sandbox evidence authorized upstream;
4. the timeout/cancel control for the same execution;
5. an enforced input from the exact input/output evidence;
6. the audit authority.

The loader and authorization verifications transitively cover review,
compatibility, Scope, Permission, routing, handoff state, input/output,
sandbox, lifecycle, timeout/cancel, and audit evidence. No caller boolean,
string, path, or package self-claim replaces an owner.

## Program Contract

`OfficialRustProgramRegistry` binds one compiled function pointer to one exact
`PackageReviewSubject`. The registry is operator-local. A function registered
for another subject or through another registry cannot consume the permit.

The function receives:

```text
RuntimeExecutionContext
operator-enforced redacted input
```

The context exposes:

```text
checkpoint for cooperative timeout/cancellation observation
sandbox resource limits
network_allowed = false
shell_allowed = false
child_processes_allowed = false
persistence_allowed = false
```

The function returns a typed classification and operator-reviewed redacted
content. The input/output authority still enforces the configured output
bound and creates the authoritative output record.

This contract trusts only code compiled into the operator-controlled binary.
It is not a security boundary for untrusted in-process Rust code.

## Lifecycle And Failure Contract

The public lifecycle API continues to reject a direct `ScopeCheck -> Running`
transition. A crate-private transition is available only for the executor
integration after the exact permit has passed verification.

```text
ScopeCheck
-> Running
-> Completed
```

Program failure or rejected output records `Failed`. An expired execution
deadline records `Timeout`. A cancellation already pending before execution
blocks the transition to `Running`.

The current executor is synchronous and cooperative. It cannot preempt a
function that does not return or call `checkpoint`. A later asynchronous
runtime feature must design a terminal cancellation handoff without weakening
the existing authority-bound cancellation evidence.

## Sandbox Truthfulness

The adapter verifies the approved restriction plan, resource limits, cleanup
owner, and cleanup triggers before execution. It reports that the adapter was
active and closed for the bounded in-process call.

It does not claim operating-system isolation:

```text
sandbox_adapter_was_active = true
os_isolation_claimed = false
cleanup_completed = true
```

Running arbitrary or package-supplied Rust under this adapter would be unsafe.
Only operator-registered official handlers are supported by this feature.

## Public Contract

```text
RuntimeExecutorAuthority
RuntimeExecutionPreparation
RuntimeExecutionPermit
RuntimeExecutionRequest
RuntimeExecutionVerification
RuntimeExecutionResult
OfficialRustProgramRegistry
OfficialRustProgram
OfficialRustProgramOutput
OfficialRustProgramFailure
RuntimeExecutionContext
RuntimeExecutionInterrupt
RuntimeExecutorError
RuntimeExecutorRequirement
```

Result schema:

```text
iamine.agent.runtime_executor.result-0.1
```

## Security And Privacy

- Executor, loader, evidence, program, execution, and subject identities are
  crate-controlled and redacted in Debug output.
- Errors use stable codes and fixed messages without package IDs, paths, host
  values, input, output, credentials, or secrets.
- A permit cannot be cloned and is consumed by one execution request.
- An enforced input is bound to the exact input/output evidence and reviewed
  subject.
- The executor does not read environment variables, package paths, host
  identity, hardware, credentials, or user data.
- The executor does not persist state, start transport, mutate the scheduler,
  or emit an external event.

## Explicitly Out Of Scope

- executing package bytes or resolving package paths again;
- dynamic libraries, scripts, WASM, containers, subprocesses, or arbitrary
  native executables;
- OS-level syscall, filesystem, network, memory, or CPU isolation;
- hard preemption of a non-cooperative in-process function;
- node CLI, daemon, worker, controller, scheduler, P2P, or PubSub integration;
- model selection, download, loading, inference, or backend startup;
- functional P0 official-agent behavior;
- closing the v0.11.2 milestone QA gate.

## Architecture Maintenance

```text
main.rs: 4929 lines, delta 0
cluster_registry.rs: 862 lines, delta 0
largest new production file: runtime_executor/authority.rs, 450 lines
largest focused test file: runtime_executor.rs, 516 lines
new non-main Rust file above 750 lines: none
new production modules: 10
Cargo dependencies added: none
node wiring: none
duplicated package parser or resolver logic: none
```

The implementation adds about 2,000 Rust lines including focused tests, but
keeps each responsibility in a narrow module. No new production file reaches
the 750-line review threshold.

## Validation Contract

```bash
cargo fmt --all -- --check
cargo test -p iamine-agent-runtime --test runtime_executor
cargo test -p iamine-agent-runtime
cargo test -p iamine-agents
cargo build -p iamine-agent-runtime
cargo clippy -p iamine-agent-runtime --all-targets -- -D warnings
./scripts/quality-gate.sh
git diff --check
git diff --cached --check
```

Field QA must validate the exact source commit on Mac, TS140,
`iamine-ctrl`, `iamine-wrk1`, `iamine-wrk2`, and `iamine-heavy`. It must run
the focused executor tests and complete runtime regression without starting a
node daemon, worker, transport, model backend, inference engine, or package
process.

## Architecture Checkpoint

```text
owner boundary: SATISFIED
independent gates: PRESERVED
one-shot permit: SATISFIED
exact evidence binding: SATISFIED
public lifecycle bypass: BLOCKED
arbitrary package execution: BLOCKED
OS isolation claim: NONE
anti-monolith guards: PASS
focused validation: PASS, 12/12
runtime regression: PASS, 149/149
agents regression: PASS, 109/109
strict crate clippy: PASS
workspace clippy: PASS WITH BASELINE WARNINGS
quality gate: PASS WITH ACCEPTED BASELINE EXCEPTION
field QA roles: PASS, 6/6
field QA focused tests: PASS, 72/72
field QA runtime regression: PASS, 894/894
product failures: 0
runtime side effects observed: 0
recommendation: READY FOR MERGE REVIEW
```

## Final Architecture Review

The six-role exact-tree matrix preserved the source identity and exercised the
same deterministic Rust surface on macOS, physical Linux, and four Linux
guests. No field result contradicts the local owner-boundary, privacy,
one-shot permit, lifecycle, timeout, I/O, audit, or side-effect claims.

Environmental findings do not change the product decision:

- Mac Cargo artifacts exhausted the data volume before disposable build output
  was removed.
- The four Proxmox guest root filesystems remain between 97% and 100% full.
- Proxmox `/dev/shm` is isolated per SSH session, so QA streamed and executed
  the bundle inside one session.
- `iamine-wrk1` emitted non-fatal full-database messages from its saturated
  root environment; all required tests still passed.
- The four Metal real-inference baseline failures reproduced at the exact
  feature base and do not touch the runtime-executor ownership surface.

The feature remains limited to operator-registered official Rust handlers and
does not claim OS isolation. Those limits are intentional, visible, and
release-relevant; they are not silently treated as completed future work.
