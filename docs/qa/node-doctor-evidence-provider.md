# Node Doctor Evidence Provider QA

Feature:

```text
NODE-DOCTOR-EVIDENCE-PROVIDER-001
```

Current state:

```text
LOCAL VALIDATION PASSED
FIELD QA REQUIRED
```

## Authorized Identity

```text
branch: feature/node-doctor-evidence-provider-001
base: e2e6a8a70a8f952bf4eb064a7fd9f70e39aac72a
base tree: bbb3a261d85717d5326a0b960381f4509f787d30
origin: https://github.com/iamine-labs/iamine
```

The exact source commit and tree will be recorded before field QA.

## Scope

Created:

```text
iamine-node/src/node_doctor_evidence_provider.rs
docs/architecture/node-doctor-evidence-provider.md
docs/qa/node-doctor-evidence-provider.md
```

Updated:

```text
iamine-node/src/main.rs
iamine-node/src/lan_node_doctor.rs
docs/agents/node-doctor-agent-skeleton.md
docs/architecture/node-doctor-agent-dependency-reconciliation.md
docs/roadmap/iamine-agent-network-roadmap.md
docs/roadmap/iamine-product-roadmap.md
```

## Local Validation

```text
focused provider tests: 6/6 PASS
iamine-node: 485/486 in Codex sandbox
daemon socket test outside sandbox: 1/1 PASS
effective iamine-node result: 486/486 PASS WITH ACCEPTED ENVIRONMENT EXCEPTION
cargo fmt --all -- --check: PASS
changed-surface Clippy with baseline lint families excluded: PASS
new feature Clippy findings: 0
```

The strict workspace-style node Clippy invocation is blocked by historical
`dead_code`, `too_many_arguments`, and `type_complexity` findings outside the
feature diff. The feature-specific `manual_contains` findings were corrected.

## Architecture Maintenance

```text
main.rs: 4929 -> 4934, wiring only
cluster_registry.rs: 862 -> 862
lan_node_doctor.rs: 687 lines
node_doctor_evidence_provider.rs: 419 lines
new non-main Rust files above 750 lines: 0
```

## Field QA Matrix

Required environments:

```text
Mac
TS140
iamine-ctrl
iamine-wrk1
iamine-wrk2
iamine-heavy
```

On each role:

1. verify exact source commit, tree, base, tracked state, staging, and untracked
   baseline;
2. run the six focused provider tests;
3. run the complete `iamine-node` suite or the approved exact-tree focused
   matrix when resource limits require it;
4. confirm serialized evidence contains only static bounded fields;
5. confirm peer/network and remote readiness remain `not_observed` without an
   active probe;
6. confirm no worker, P2P, PubSub, download, model load, inference, profile
   write, config write, or persistent evidence state is created;
7. preserve existing processes, files, services, profiles, and credentials.

## Current Recommendation

```text
FIELD QA AUTHORIZED AFTER SOURCE COMMIT
```

QA may recommend `READY FOR ARCHITECTURE MERGE REVIEW` after the six-role exact
source matrix passes. QA does not authorize merge.
