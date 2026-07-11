# IAMINE Signed Auto-Update QA

Feature:

```text
SIGNED-AUTOUPDATE-001
```

## Objective

Validate that signed auto-update eligibility is explicit, closed by default,
bounded by rollout policy, rollback-aware, and separate from runtime update
execution.

## Identity

Record before QA:

```text
Branch: feature/signed-autoupdate-001
HEAD: 86066e4d58a7fa6f042da0ba27d686eef31599bf
Tree: a2c4bed46c9fa8565c1cf2cafe4ed9d415e08624
Base: 86066e4d58a7fa6f042da0ba27d686eef31599bf
origin/develop: 86066e4d58a7fa6f042da0ba27d686eef31599bf
tracked clean: no; feature delta is limited to expected tracked paths
staging clean: yes
untracked baseline:
- docs/architecture/signed-autoupdate.md
- docs/qa/signed-autoupdate.md
- iamine-core/src/signed_autoupdate.rs
```

Implementation file hashes captured after final clippy correction and before
staging:

```text
docs/architecture/signed-autoupdate.md
22206c6e209b71383304d2d61ee2c777611f9c0d14bf36a9d0d6dd1ec9dbbe11
docs/roadmap/iamine-product-roadmap.md
4aa81f4acd6752a991c248e35b8c3c251a249663b550ce01fabda879831c7c3e
iamine-core/src/lib.rs
ad109896c28ee6ece896e6fb946c538cb54556793c23d15782d62743b0f3e2b7
iamine-core/src/signed_autoupdate.rs
309e7242edff3039bf25d60a62875136b0c3716bf3ac38a6145bec2e3912c588
```

## Scope Checks

Expected changed paths:

```text
docs/architecture/signed-autoupdate.md
docs/qa/signed-autoupdate.md
docs/roadmap/iamine-product-roadmap.md
iamine-core/src/lib.rs
iamine-core/src/signed_autoupdate.rs
```

Expected runtime behavior change:

```text
none
```

This feature must not modify `iamine-node/src/main.rs`, private/public testnet
admission runtime, P2P startup, worker behavior, scheduler behavior, model
policy, inference execution, packaging scripts, service templates, or install
helpers.

## Required Local Validation

```bash
cargo fmt --all -- --check
cargo test -p iamine-core signed_autoupdate
cargo test -p iamine-core
git diff --check
git diff --cached --check
```

Run `./scripts/quality-gate.sh` before merge review.

## Expected Results

- default policy rejects update candidates;
- controlled rollout accepts only verified artifacts signed by trusted keys;
- missing signatures reject;
- invalid signatures reject;
- untrusted signing keys reject;
- missing or invalid SHA-256 digests reject;
- requested rollout percentage must be between 1 and the policy maximum;
- rollback artifact is required by default;
- too many artifacts reject with a stable reason code.

## Field QA Decision

Field QA is not required for this initial policy-only feature because no
runtime, installer, P2P, worker, scheduler, inference, model, service-manager,
or packaging behavior changes.

Field QA is required for any later feature that wires this policy into updater
execution, package installation, node startup, service management, release
fetching, or runtime artifact replacement.

## Local Results

Status:

```text
LOCAL VALIDATION PASSED
```

Executed on Mac local worktree:

```text
cargo test -p iamine-core signed_autoupdate: PASS; 8 passed
cargo test -p iamine-core: PASS; 8 passed; doc tests 0 passed
cargo fmt --all -- --check: PASS
git diff --check: PASS
privacy scan over changed docs/module exports: PASS; no matches
targeted clippy correction:
- cargo clippy -p iamine-core --all-targets: PASS
./scripts/quality-gate.sh: PASS WITH WARNINGS
size guard:
- iamine-core/src/signed_autoupdate.rs: 423 lines
- iamine-node/src/main.rs: 4928 lines; unchanged
- iamine-node/src/cluster_registry.rs: 862 lines; unchanged
```

Warnings:

```text
No warnings remain in the new iamine-core signed auto-update module.

quality-gate emitted existing warnings in untouched modules:
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
- iamine-node/src/task_cache.rs
- iamine-node/src/task_protocol.rs
- iamine-node/src/wallet.rs
- iamine-node/src/worker_pool.rs
- iamine-node/src/worker_startup_policy.rs
- iamine-node/src/main.rs

Classified as baseline/non-blocking for this policy-only feature. The quality
gate reported `required_failures=0`, `warnings=0`, and `skipped=3`.
Optional tools skipped:
- cargo audit
- cargo deny
- gitleaks
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
