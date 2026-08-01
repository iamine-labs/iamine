# Node Doctor Evidence Provider QA

Feature:

```text
NODE-DOCTOR-EVIDENCE-PROVIDER-001
```

Current state:

```text
MERGED / VALIDATED / CLOSED
```

## Authorized Identity

```text
branch: feature/node-doctor-evidence-provider-001
base: e2e6a8a70a8f952bf4eb064a7fd9f70e39aac72a
base tree: bbb3a261d85717d5326a0b960381f4509f787d30
source commit: c66f626162ad1419977483b35249cb4fd0d80bf3
source tree: 758fa81b00a431848393ad1ab9029a7649a4ff7c
bundle SHA-256: 0f98fdefb1ab194f6c8604b03cdb1401b01143ca4eb5d01cab8920b45a052f3b
Linux x86_64 test binary SHA-256: 6fadedbab89e59376b902a29b8e6f26d976b026051ce4f375d31fe6470e09033
origin: https://github.com/iamine-labs/iamine
```

## Merge Identity

```text
PR: https://github.com/iamine-labs/iamine/pull/13
merge commit: f54851bc70d603eab10ed60b719088628dc8f482
merge tree: 18a5b0d2f2d49661d889b067e756eeee65646b94
first parent: e2e6a8a70a8f952bf4eb064a7fd9f70e39aac72a
second parent: d6d80f692b3df05bf284992b6f4a020099c8cce6
post-merge bundle SHA-256: a84e6c357cf681b5a667c79a01d6a4d934f56136a0179c29a77e4fff84deea3a
```

## Scope

Created:

```text
iamine-node/src/node_doctor_evidence_provider.rs
docs/architecture/node-doctor-evidence-provider.md
docs/qa/node-doctor-evidence-provider.md
```

Updated:

```text
iamine-models/src/model_storage.rs
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
read-only model storage test: 1/1 PASS
iamine-node: 485/486 in Codex sandbox
daemon socket test outside sandbox: 1/1 PASS
effective iamine-node result: 486/486 PASS WITH ACCEPTED ENVIRONMENT EXCEPTION
cargo fmt --all -- --check: PASS
changed-surface Clippy with baseline lint families excluded: PASS
new feature Clippy findings: 0
workspace test inventory: 1125
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
model_storage.rs: 192 lines
new non-main Rust files above 750 lines: 0
```

## Finding And Correction

Field preparation found that the existing LAN Doctor path instantiated
`ModelStorage::new()`, which can create the default models directory. That was a
product finding because the provider contract forbids writes even when no model
is installed.

The source was corrected before QA closure. LAN Doctor now uses
`ModelStorage::for_read_only_inspection()`, and the owner crate has a regression
test proving that inspection of a missing storage path does not create it. The
six-role rerun also executed the provider with a fresh nonexistent home path;
the path remained absent on every role.

## Field QA Results

Exact-source results:

```text
Mac:          6/6 PASS, fresh home not created, processes 0 -> 0
TS140:        6/6 PASS, fresh home not created, processes 1 -> 1
iamine-ctrl:  6/6 PASS, fresh home not created, processes 0 -> 0
iamine-wrk1:  6/6 PASS, fresh home not created, processes 0 -> 0
iamine-wrk2:  6/6 PASS, fresh home not created, processes 0 -> 0
iamine-heavy: 6/6 PASS, fresh home not created, processes 0 -> 0
```

TS140 full gate:

```text
scripts/quality-gate.sh: PASS WITH WARNINGS
required_failures: 0
warnings: 0
skipped optional tools: 3
iamine-models unit tests: 100/100 PASS
iamine-models integration tests: 59/59 PASS
iamine-network tests: 163/163 PASS
iamine-node tests: 486/486 PASS
workspace test inventory: 1125
changed-surface strict Clippy: PASS
```

The four Proxmox guests had 5.3 GB to 6.1 GB free, so QA used the exact Git
bundle plus the single TS140-built Linux x86_64 test binary instead of producing
four duplicate compilation trees. Each guest verified both hashes, source
commit, tree, base, clean tracked state, and zero untracked files before running
the focused matrix.

Two harness-only observations did not affect product results. Proxmox
`/dev/shm` was session-scoped, so artifacts were staged under feature-specific
`/tmp` paths. On Mac, a zsh variable-name collision occurred after a successful
test run; the command was corrected and rerun successfully.

## Post-Merge Validation

TS140 validated the exact merge commit, tree, and both parents from the bundle
recorded above. The worktree remained clean and the existing node process count
was preserved.

```text
scripts/quality-gate.sh: PASS WITH WARNINGS
required_failures: 0
warnings: 0
skipped optional tools: 3
iamine-models unit tests: 100/100 PASS
iamine-models integration tests: 59/59 PASS
iamine-network tests: 163/163 PASS
iamine-node tests: 486/486 PASS
cargo build -p iamine-node: PASS
cargo test --workspace: PASS
cargo clippy --workspace --all-targets: PASS
```

PR checks for format, diff, models, network, node, build, workspace, Clippy,
Cargo deny, and the complete quality-gate script passed. The secret-scan job did
not execute because the organization lacks the license now required by
`gitleaks-action@v2`; the same failure exists on the exact `develop` base. An
official Gitleaks CLI 8.30.1 artifact verified by release SHA-256 scanned all
three feature commits and found no leaks. Cargo audit remained an informational
baseline failure, and the feature changed no Cargo manifest or lockfile.

## Current Recommendation

```text
MERGED / VALIDATED / CLOSED
```

The provider lifecycle is closed. `NODE-DOCTOR-AGENT-001` is the next canonical
feature in `PROPOSED`; it is not development-authorized by this closure.
