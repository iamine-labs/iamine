# Node Doctor Agent QA

Feature:

```text
NODE-DOCTOR-AGENT-001
```

Current state:

```text
READY FOR ARCHITECTURE MERGE REVIEW
```

## Authorized Identity

```text
branch: feature/node-doctor-agent-001
base: 2d51b9532992b0857856b8d3450cc9e85cf2470c
source commit: f84b81744737313b2fca2e892cbc77563dd9880f
source tree: 2ac1e1436687724ec1a2969e3c4ba4ab6bd123e0
full bundle SHA-256: 895240043c9b4ee8da64c753485130d661f5b4d1badf063a6bdcc7135719d5a1
Linux x86_64 binary SHA-256: 18a5f89eec77d1d9102e32c17d6caf66e2c2abaae37fcd77a6878817d0a2b056
Linux focused-test binary SHA-256: 323ce79a5fda4a182d68d0675f3efef0cafffec8d92f924d421687bd8ba8fcc7
Linux guest archive SHA-256: 7dc65f99a5c0c9169d5ac8936c8825732241b1d4488640d778eee48301b0fcd5
official package aggregate SHA-256: 26910c212c271de5b8a89373303fbdb8bdb2cf14f5eb976a50e3e445af1000f9
origin: https://github.com/iamine-labs/iamine
```

## Scope

Created:

```text
agents/official/node-doctor/
iamine-node/src/node_doctor_agent/
docs/architecture/node-doctor-agent.md
docs/qa/node-doctor-agent.md
```

Updated surfaces are limited to CLI/control-plane wiring, the existing Node
Doctor evidence-provider visibility boundary, additive runtime output
classification and safe package-subject inspection, Cargo dependency wiring,
agent contracts, and roadmap state.

## Local Results

```text
focused Node Doctor tests: 7/7 PASS
log-free dispatch regression: 1/1 PASS
iamine-agent-runtime: 149/149 PASS
iamine-agents: 109/109 PASS
iamine-node: 496/496 PASS
cargo build -p iamine-node: PASS
JSON CLI smoke: PASS
fresh HOME absent before and after smoke: PASS
git diff --check: PASS
new feature warnings: 0
scripts/quality-gate.sh: PASS WITH WARNINGS
required failures: 0
quality-gate warnings: 0
optional tools skipped: 3
```

Four warning groups in `task_cache.rs`, `wallet.rs`, and `worker_pool.rs` remain
historical baseline findings. A sandboxed Mac process-inspection command could
not access `sysmond`; process-count evidence must be collected during field QA
outside that sandbox.

## Focused Assertions

```text
official package and all seven references validate
all nine required boundary classes match runtime scope decisions
only the exact compiled package snapshot executes
one-byte package changes fail closed
typed manifest changes and missing packages fail closed
missing evidence returns blocked_action_report
private owner messages and details do not appear in output
runtime completion has audit and cleanup evidence
scheduler, transport, persistence, shell, children, and network remain off
```

## Field Results

The exact committed source and hash-matched Linux artifact passed on:

```text
Mac:          PASS, processes 0 -> 0, diagnostic_report
TS140:        PASS, processes 1 -> 1, diagnostic_report
iamine-ctrl:  PASS, processes 0 -> 0, diagnostic_report
iamine-wrk1:  PASS, processes 0 -> 0, diagnostic_report
iamine-wrk2:  PASS, processes 0 -> 0, diagnostic_report
iamine-heavy: PASS, processes 0 -> 0, diagnostic_report
```

Every role verified the output schema, six categories, privacy filters, absent
fresh HOME, unchanged process count, cleanup/audit evidence, and false
scheduler/transport/persistence/OS-isolation claims. Every role also changed one
byte in capability metadata and observed fail-closed package rejection. No
guest QA root contained NDJSON output, and each command ran from a fresh
directory that remained empty after both success and tamper rejection.

Mac and TS140 reported static node/hardware/config/model readiness with passive
peer and remote-inference evidence `not_observed`. The four Proxmox guests
reported explicit attention for node/model/remote-inference readiness while
hardware/configuration remained ready. These are truthful environment results,
not execution failures.

TS140 built from the complete Git bundle, verified commit/tree/base and clean
status, then passed the seven focused tests. The four guests consumed one
compressed Linux artifact after verifying its SHA-256, exact binary SHA-256,
and aggregate package SHA-256.

## Product Finding And Correction

The initial six-role matrix verified HOME and process side effects but did not
baseline the current working directory. Its Mac run left an empty
`logs/iamine-node.ndjson` because global runtime logging was initialized before
the pre-network dispatcher. This contradicted the feature's no-persistence
contract even though the agent result itself reported `persisted: false`.

The feature returned to `CHANGES REQUIRED`. Node Doctor is now dispatched
before runtime logging through an explicit mode predicate, while all existing
control-plane modes retain their prior startup order. A regression test binds
that ordering. The branch then integrated the current `origin/develop` without
conflicts, repeated the complete local quality gate, rebuilt Linux from the
exact bundle, and repeated all six field roles with a fresh-directory check.
The corrected matrix passed with zero working-directory artifacts.

## Harness And Environment Findings

The first Mac tamper harness expected the Display code `package_mismatch`, but
Rust process termination emitted the safe typed Debug variant
`PackageMismatch`. The product had already rejected the modified package and
left zero processes; the harness accepted both stable representations and the
rerun passed.

TS140's non-interactive shell did not export Cargo in `PATH`. QA used the known
executable `/home/ts140/.cargo/bin/cargo` after verifying it as Cargo 1.94.0.
This was environmental and did not change source or the canonical TS140
checkout.

## Architecture Maintenance

```text
main.rs: 4934 -> 4935, wiring only
cluster_registry.rs: 862 -> 862
largest new Rust file: node_doctor_agent/execution.rs, 436 lines
new non-main Rust files above 750 lines: 0
```

## Recommendation

```text
READY FOR ARCHITECTURE MERGE REVIEW
```

QA does not emit merge approval or merge authorization.
