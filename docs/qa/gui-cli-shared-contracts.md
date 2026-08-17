# GUI-CLI-SHARED-CONTRACTS-001 QA

## Identity

```text
branch: feature/gui-cli-shared-contracts-001
base: df6d46295eb42efe4e112758960f6061ec4ec2e2
target: develop
platform: Mac development machine
field QA: not required for contract-only behavior
```

The preserved feature branch was 10 commits behind `origin/develop` and one
commit ahead. It was reconciled through a regular merge from the current base;
there were no merge conflicts and no source branch was deleted.

## Scope

Validate the shared, typed, versioned boundary that future CLI and dashboard
adapters will consume. The contract must remain fail-closed, privacy-bounded,
non-authorizing, independent from runtime policy, and owned by `iamine-core`.

## Required Checks

```text
CHECK 1 identity, historical branch, base, and scope
CHECK 2 architecture and ownership review
CHECK 3 current develop reconciliation
CHECK 4 contract invariants and regression coverage
CHECK 5 core format, tests, and Clippy
CHECK 6 workspace quality gate
CHECK 7 size, core safety, and QA classification
CHECK 8 architecture handoff and controlled push authorization
```

## Results

```text
CHECK 1 identity, historical branch, base, and scope: PASS
CHECK 2 architecture and ownership review: PASS WITH CORRECTIONS
CHECK 3 current develop reconciliation: PASS, no conflicts
CHECK 4 contract invariants and regression coverage: PASS
CHECK 5 core format, tests, and Clippy: PASS
CHECK 6 workspace quality gate: PASS WITH WARNINGS
CHECK 7 size, core safety, and QA classification: PASS
CHECK 8 architecture handoff: READY FOR MERGE REVIEW
```

Focused evidence:

```text
cargo fmt --all: PASS
cargo test -p iamine-core: PASS, 43 unit + 10 integration tests
cargo clippy -p iamine-core --all-targets: PASS, no feature warnings
```

Repository evidence:

```text
./scripts/quality-gate.sh: PASS WITH WARNINGS
required failures: 0
new warnings: 0
workspace tests: 1148 passed
cargo clippy --workspace --all-targets: PASS with baseline warnings
optional skipped: cargo-audit, cargo-deny, gitleaks unavailable
main.rs: 4935 lines, delta 0
cluster_registry.rs: 862 lines, delta 0
interface_contracts.rs: 581 lines
interface contract integration tests: 254 lines
git diff --check: PASS
```

## Findings

1. The preserved branch had useful implementation work but was 10 commits
   behind the canonical base. A regular merge reconciled it without conflicts
   and retained the completed dashboard and core history from `develop`.
2. Unknown struct and enum-container fields were silently accepted by Serde.
   Contract boundaries now use `deny_unknown_fields`, and regression tests
   prove fail-closed deserialization.
3. An event stream could be supplied independently from its payload, allowing
   contradictory event identity. The stream is now derived during construction
   and a mismatched serialized stream is rejected.
4. Exact JSON shape and the complete operation-to-class table were not frozen.
   Integration tests now protect both contracts from accidental drift.
5. Adding those tests made the production module 833 lines. The tests were
   extracted into a 254-line integration module, leaving production at 581
   lines and avoiding a new monolith.
6. Workspace Clippy reports historical warnings in `client-rust`, models,
   network, and node code outside the feature diff. They are baseline debt, not
   new regressions. The three optional security tools were unavailable.

## Core Safety

The feature changes only the shared `iamine-core` contract, its focused tests,
and architecture, QA, and roadmap documentation. It does not change
`iamine-node`, scheduler, workers, P2P, PubSub, hardware, models, inference, or
the dashboard. Field QA on TS140 and Proxmox is therefore not required.

## Recommendation

```text
READY FOR ARCHITECTURE MERGE REVIEW
```

This recommendation applies only to the shared contract boundary. It does not
authorize a Local Control API, dashboard connectivity, runtime mutation, or
agent execution.
