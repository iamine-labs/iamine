# AGENT-RUNTIME-SANDBOX-ENFORCEMENT-001

## State

```text
APPROVED FOR MERGE
branch: feature/agent-runtime-sandbox-enforcement-001
base: c97dcf66047683e99937a05ebd2b63b8349a5195
base tree: c118fe9a35fc589d186d3dd1e55b9158b47b748f
source commit: 0a57870873adfef716a56904aa84e92913bc3dbb
source tree: 41f7dc8c7e5f78c91204878130dd89412325f675
runtime behavior change: passive platform-bound sandbox enforcement plan
field QA: passed on Mac, TS140, and four Proxmox/R5500 guests
QA recommendation: READY FOR ARCHITECTURE MERGE REVIEW
```

## Objective

Establish a typed, fail-closed sandbox enforcement plan for one exact reviewed
package after runtime compatibility and input/output enforcement evidence
exist. The plan defines the restrictions and cleanup obligations that a later
runtime adapter must apply before agent code can execute.

This feature does not start a sandbox, load a package, spawn a process,
register cleanup handlers, authorize execution, persist evidence, or change
node, worker, scheduler, network, model, or inference behavior.

## Dependencies

```text
AGENT-PACKAGE-REVIEW-EVIDENCE-001
-> AGENT-RUNTIME-COMPATIBILITY-GATE-001
-> AGENT-INPUT-OUTPUT-ENFORCEMENT-001
-> AGENT-RUNTIME-SANDBOX-ENFORCEMENT-001
```

The sandbox authority must verify the exact compatibility authority/evidence,
input/output authority/evidence, manifest object, and resolved reference set.
Package-controlled bytes cannot construct any authority or evidence identity.

## Ownership

Production behavior belongs to a new owner under `iamine-agent-runtime`:

```text
sandbox_enforcement/authority.rs
sandbox_enforcement/configuration.rs
sandbox_enforcement/error.rs
sandbox_enforcement/evaluation.rs
sandbox_enforcement/evidence.rs
sandbox_enforcement/mod.rs
sandbox_enforcement/restrictions.rs
```

The existing runtime-compatibility owner may expose one crate-private,
normalized resource profile so sandbox evaluation can reuse the already typed
resource parser. It must not duplicate resource parsing or move policy into
`iamine-node/src/main.rs`.

## Supported Boundary

The initial executable plan is intentionally narrow:

```text
platforms: macOS and Linux
runtime: rust_native_official
operating mode: local_readonly
filesystem: package-relative reads only
writable storage: bounded temporary workspace only
network: denied
arbitrary shell: denied
child processes: denied
privilege expansion: denied
credentials and private paths: denied
```

Windows, WASM/WASI, containers, LAN access, local-planning mutations,
interpreters, arbitrary shell, unrestricted filesystem, and unrestricted
network remain unsupported and fail closed.

## Resource Contract

The plan derives CPU, memory, and writable-storage limits from the exact
validated resource metadata for the compatible operating mode:

- logical-core limit from the recommended logical-core bound;
- background-thread limit from the declared maximum;
- memory limit from the maximum working set;
- writable-storage limit from temporary workspace plus cache budget;
- process count fixed to one with zero child processes;
- wall-clock and open-file limits from bounded operator policy.

Wall-clock and open-file configuration must be non-zero and capped. Arithmetic
overflow or contradictory metadata fails closed. Resource values describe
package limits, never host inventory.

## Platform Contract

`SandboxPlatform::current()` uses compile-time target identity only:

```text
target_os=macos -> macOS
target_os=linux -> Linux
other targets -> unsupported
```

It must not inspect hostnames, usernames, process lists, IP addresses, machine
IDs, hardware fingerprints, or private paths. Platform identity is attached to
the plan so a later adapter cannot reuse evidence for another target.

## Cleanup Contract

Cleanup ownership is assigned to the future runtime sandbox adapter. The plan
requires cleanup after:

- startup failure;
- normal exit;
- cancellation;
- timeout;
- adapter drop.

Prepared evidence reports that cleanup is required but not registered and that
the sandbox is not active. Timeout/cancel enforcement remains owned by
`AGENT-TIMEOUT-CANCEL-ENFORCEMENT-001`.

## Evidence Contract

The schema identifier is:

```text
iamine.agent.sandbox_enforcement.plan-0.1
```

Evidence status is `Prepared`. It may report normalized restrictions and
package resource limits, but:

```text
sandbox_active = false
cleanup_registered = false
load_allowed = false
execution_allowed = false
persistence_allowed = false
transport_allowed = false
```

The static `SandboxEnforcementUnavailable` package-load blocker remains
unchanged until the later package-load evidence integration owner consumes the
new evidence together with every other independent gate.

## Privacy

Debug output redacts authority identities, evidence identities, subjects,
policies, limits, and restriction details. Errors expose only static codes,
requirements, and messages. No content, package value, scope value, path,
hostname, credential, prompt, output, process list, or host resource value may
appear in diagnostics.

## Non-Bypass Rules

- A safe plan is not an active sandbox.
- Platform selection is not proof that a platform adapter ran.
- Compatibility does not grant sandbox, load, or execution authorization.
- Input/output evidence does not grant filesystem or network access.
- Cleanup requirements do not prove cleanup handlers were registered.
- Resource limits cannot expand declared package requirements.
- A sandbox plan cannot grant Scope, Permission, Audit, handoff, or lifecycle
  transitions.
- No static package-load blocker is removed in this feature.

## Validation Matrix

Local validation:

- exact evidence and subject binding;
- both supported platform variants remain typed;
- current target maps to the expected platform;
- unsupported operating modes and network access fail closed;
- unsafe security claims fail closed independently;
- zero, excessive, and overflowing limits fail closed;
- restrictions and cleanup requirements cannot be weakened;
- errors and Debug output remain private;
- package-load blockers remain unchanged.

Field QA:

- Mac validates the macOS target plan;
- TS140 validates the Linux target plan;
- Proxmox/R5500 guests validate the Linux target plan under VM and cgroup
  environments;
- each environment confirms no process, socket, file, model, worker, or
  persistent state is created by the tests.

## Risks

- Treating prepared evidence as proof that OS isolation is active.
- Claiming macOS or Linux enforcement without a later platform adapter.
- Duplicating resource parsing and allowing the two decisions to drift.
- Allowing LAN or writable-host access through a read-only plan.
- Assigning cleanup to lifecycle or timeout owners instead of the sandbox
  adapter.
- Adding process execution early and bypassing loader and authorization gates.

## Architecture Decision

```text
owner crate: iamine-agent-runtime
main.rs changes: forbidden
cluster_registry.rs changes: forbidden
new Cargo dependency: forbidden
process or sandbox startup: forbidden
package-load blocker change: forbidden
field QA: required
field QA result: PASS, 6/6 hosts
decision: APPROVED FOR MERGE
merge authority: final Architecture review complete
```
