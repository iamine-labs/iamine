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
base: 3374e27f7b6b132b39c3e979af7a1a03cd5daf9b
source commit: 2349499c94209f2b82665289cc08abce84625ea5
source tree: 2656459419d0a2bb68c07395998cd06dc0da1327
full bundle SHA-256: 96aa759c3af23d113f6d29277d9427c33215c7b4cfc2b1a1e1f4d944b896397a
Linux x86_64 binary SHA-256: 1b0efc0afc0dd62ce8ccbf74710695287ec2ba563e7729d17db6cc11e9f15306
Linux focused-test binary SHA-256: f8499824eecf9b7473685ebb326d072837008bd9aaeb51673b1015c6484c69f7
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
iamine-agent-runtime: 149/149 PASS
iamine-agents: 109/109 PASS
iamine-node: 493/493 PASS
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
guest QA root contained NDJSON output.

Mac and TS140 reported static node/hardware/config/model readiness with passive
peer and remote-inference evidence `not_observed`. The four Proxmox guests
reported explicit attention for node/model/remote-inference readiness while
hardware/configuration remained ready. These are truthful environment results,
not execution failures.

TS140 built from the complete Git bundle, verified commit/tree/base and clean
status, then passed the seven focused tests. The four guests consumed one
compressed Linux artifact after verifying its SHA-256, exact binary SHA-256,
and aggregate package SHA-256.

## Findings

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
main.rs: 4934 -> 4931, wiring only
cluster_registry.rs: 862 -> 862
largest new Rust file: node_doctor_agent/execution.rs, 436 lines
new non-main Rust files above 750 lines: 0
```

## Recommendation

```text
READY FOR ARCHITECTURE MERGE REVIEW
```

QA does not emit merge approval or merge authorization.
