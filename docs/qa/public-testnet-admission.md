# IAMINE Public Testnet Admission QA

Feature:

```text
PUBLIC-TESTNET-ADMISSION-001
```

## Objective

Validate that public-testnet admission semantics are explicit, closed by
default, bounded, privacy-safe, and separate from the existing private-testnet
runtime allowlist.

## Identity

Record before QA:

```text
Branch: feature/public-testnet-admission-001
HEAD: 43bb18397723882002cc18a5be25ae7ecd92c192
Tree: bcfe5863a2597c616340a997108854d25a02eff4
Base: 43bb18397723882002cc18a5be25ae7ecd92c192
origin/develop: 43bb18397723882002cc18a5be25ae7ecd92c192
tracked clean: no; feature delta is limited to expected tracked paths
staging clean: yes
untracked baseline:
- docs/architecture/public-testnet-admission.md
- docs/qa/public-testnet-admission.md
- iamine-network/src/public_testnet_admission.rs
- iamine-node/logs/iamine-node.ndjson
```

Untracked file hashes captured before this QA document result update:

```text
docs/architecture/public-testnet-admission.md
9348ec30267962c2ec4cf291fddccdf7fdc274d231573fe5a5a4008d2945e3b5
docs/qa/public-testnet-admission.md
1029f7594cd22f80608937438732165a50f5f04720e96eccece9f696e5912f64
iamine-network/src/public_testnet_admission.rs
b2b49a7657ed53266af8ecec71d91c34cba91577d7983a0721e1de3b119eed0e
iamine-node/logs/iamine-node.ndjson
1b7a0ff42843d142bb478b19fc21bdac60409c468db34445c7ed9b40bdfbd0b3
```

## Scope Checks

Expected changed paths:

```text
docs/architecture/public-testnet-admission.md
docs/qa/public-testnet-admission.md
docs/roadmap/iamine-product-roadmap.md
iamine-network/src/lib.rs
iamine-network/src/public_testnet_admission.rs
```

Expected runtime behavior change:

```text
none
```

This feature must not modify `iamine-node/src/main.rs`, private-testnet runtime
admission, scheduler behavior, worker behavior, model policy, remote inference,
or P2P startup.

## Required Local Validation

```bash
cargo fmt --all -- --check
cargo test -p iamine-network public_testnet_admission
cargo test -p iamine-network testnet_admission
cargo test -p iamine-node testnet_admission_runtime
cargo test -p iamine-node network_config
git diff --check
git diff --cached --check
```

Run `./scripts/quality-gate.sh` before merge review.

## Expected Results

- closed mode rejects candidates;
- controlled mode requires at least one admitted peer;
- removal overrides admission;
- operator node limit rejects extra nodes;
- identity registration is required by strict abuse controls;
- secure transport is required by strict abuse controls;
- admitted and removed peer lists are deduplicated and bounded;
- private-testnet admission tests remain green.

## Field QA Decision

Field QA is not required for the initial policy-only feature because no runtime,
P2P, worker, scheduler, or inference behavior changes. Field QA is required for
any later feature that wires public-testnet admission into node startup,
discovery, connection handling, remote inference, or operator provisioning.

## Local Results

Status:

```text
LOCAL VALIDATION PASSED
```

Executed on Mac local worktree:

```text
cargo test -p iamine-network public_testnet_admission: PASS; 9 passed
cargo test -p iamine-network testnet_admission: PASS; 15 passed
cargo test -p iamine-node testnet_admission_runtime: PASS; 2 passed
cargo test -p iamine-node network_config: PASS; 14 passed
cargo fmt --all -- --check: PASS
git diff --check: PASS
privacy scan over changed docs/module exports: PASS; no matches
./scripts/quality-gate.sh: PASS WITH WARNINGS
size guard:
- iamine-network/src/public_testnet_admission.rs: 541 lines
- iamine-node/src/main.rs: 4928 lines
- iamine-node/src/cluster_registry.rs: 862 lines
```

Warnings:

```text
iamine-node emitted existing dead_code warnings in untouched files:
- iamine-node/src/task_cache.rs
- iamine-node/src/wallet.rs
- iamine-node/src/worker_pool.rs

quality-gate clippy emitted existing warnings in untouched modules:
- client-rust/src/solana_client.rs
- client-rust/src/solana_config.rs
- iamine-models/src/distributed_inference.rs
- iamine-network/src/prompt_semantic_signals.rs
- iamine-network/src/scheduler.rs
- iamine-node/src/capability_display.rs
- iamine-node/src/cluster_registry.rs
- iamine-node/src/discovery_runtime.rs
- iamine-node/src/infer_observability.rs
- iamine-node/src/model_display_policy.rs
- iamine-node/src/pubsub_observability.rs
- iamine-node/src/result_observability.rs
- iamine-node/src/worker_startup_policy.rs
- iamine-node/src/main.rs

Classified as baseline/non-blocking for this policy-only feature. The quality
gate reported `warnings=0`, `required_failures=0`, and `skipped=3`.
```

Optional tools skipped by quality gate:

```text
cargo audit: skipped; not installed
cargo deny check: skipped; not installed
gitleaks secret scan: skipped; not installed
```

Field QA:

```text
Not required for this feature because it adds policy types and docs only.
Runtime, P2P startup, scheduler, worker, and inference behavior were not changed.
```

## QA Recommendation

QA may recommend:

```text
READY FOR ARCHITECTURE MERGE REVIEW
```

QA must not emit:

```text
MERGE APPROVED
MERGE AUTHORIZED
```
