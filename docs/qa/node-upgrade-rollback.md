# IAMINE Node Upgrade Rollback QA

Feature:

```text
NODE-UPGRADE-ROLLBACK-001
```

## Objective

Validate that node upgrade rollback eligibility is explicit, closed by default,
bounded, rollback-artifact aware, and separate from runtime rollback execution.

## Identity

Record before QA:

```text
Branch: feature/node-upgrade-rollback-001
HEAD: f560595eb2c998127bc044a5a0109f8c54614eb3
Tree: be8bace24b736d9d2449e04bc90fc5b8f6d39f4f
Base: origin/develop
origin/develop: f560595eb2c998127bc044a5a0109f8c54614eb3
tracked clean: no; feature delta is limited to expected tracked paths
staging clean: yes before final staging
untracked baseline: none expected in feature worktree
```

## Scope Checks

Expected changed paths:

```text
docs/architecture/node-upgrade-rollback.md
docs/qa/node-upgrade-rollback.md
docs/roadmap/iamine-product-roadmap.md
iamine-core/src/lib.rs
iamine-core/src/node_upgrade_rollback.rs
iamine-core/src/release_validation.rs
iamine-core/src/signed_autoupdate.rs
iamine-core/src/supply_chain_security.rs
```

Expected runtime behavior change:

```text
none
```

This feature must not modify `iamine-node/src/main.rs`, service templates,
packaging scripts, node startup, private/public testnet admission runtime, P2P
startup, worker behavior, scheduler behavior, model policy, or inference
execution.

## Required Local Validation

```bash
cargo fmt --all -- --check
cargo test -p iamine-core node_upgrade_rollback
cargo test -p iamine-core signed_autoupdate
cargo test -p iamine-core supply_chain_security
cargo test -p iamine-core
cargo clippy -p iamine-core --all-targets
git diff --check
git diff --cached --check
```

Run `./scripts/quality-gate.sh` before merge review.

## Expected Results

- default policy rejects rollback candidates;
- controlled recovery accepts failed or incompatible upgrades only;
- healthy or unknown upgrade states reject;
- operator confirmation is required by default;
- active tasks must be drained;
- pre-upgrade snapshot must be available;
- config backup must be available;
- current version must match the failed upgrade version;
- rollback version must differ from the current failed version;
- rollback version must be explicitly allowed by policy;
- trusted signing keys and allowed rollback versions are bounded;
- rollback artifact list is bounded;
- artifact versions must match the rollback version;
- artifact digests must be valid SHA-256 hex values;
- artifact signatures must be verified with trusted keys;
- manifest-only artifact sets reject because at least one restorable artifact is
  required.

## Field QA Decision

Field QA is not required for this initial policy-only feature because no
runtime, installer, updater, P2P, worker, scheduler, inference, model,
service-manager, package generation, or release-publishing behavior changes.

Field QA is required for any later feature that wires this policy into updater
execution, package installation, node startup, service management, release
fetching, active task draining, or runtime artifact replacement.

## Local Results

Status:

```text
LOCAL VALIDATION PASSED
```

Executed on Mac local worktree:

```text
cargo fmt --all -- --check: PASS
cargo test -p iamine-core node_upgrade_rollback: PASS; 20 passed
cargo test -p iamine-core signed_autoupdate: PASS; 9 passed
cargo test -p iamine-core supply_chain_security: PASS; 14 passed
cargo test -p iamine-core: PASS; 43 passed; doc tests 0 passed
cargo clippy -p iamine-core --all-targets: PASS
git diff --check: PASS
./scripts/quality-gate.sh: PASS WITH WARNINGS
```

Scope and size guard:

```text
iamine-core/src/node_upgrade_rollback.rs: 703 lines
iamine-core/src/release_validation.rs: 3 lines
iamine-node/src/main.rs: 4929 lines; unchanged
iamine-node/src/cluster_registry.rs: 862 lines; unchanged
runtime behavior changed: none
field QA required: no
```

Privacy scan:

```text
changed rollback docs and iamine-core files: PASS
matches were limited to privacy prohibition text and serde Serialize/Deserialize
identifiers; no secret values or host identifiers were introduced.
```

Quality gate summary:

```text
required_failures=0
warnings=1
skipped=3
cargo fmt --all -- --check: PASS
cargo test -p iamine-models: PASS
cargo test -p iamine-network: PASS
cargo test -p iamine-node: PASS
cargo build -p iamine-node: PASS
cargo test --workspace: PASS
git diff --check: PASS
git diff --cached --check: PASS
cargo clippy --workspace --all-targets: WARN; environment no space left on device
cargo audit: SKIPPED; not installed
cargo deny check: SKIPPED; not installed
gitleaks secret scan: SKIPPED; not installed
```

Environmental warning:

```text
df -h /private/tmp during gate: 116Mi available; 100% capacity
target/ before cleanup: 4.8G
classification: environment
blocking product issue: no
maintenance action: removed generated target/ from this disposable worktree
```
