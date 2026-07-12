# IAMINE Supply-Chain Security QA

Feature:

```text
V1-SUPPLY-CHAIN-SECURITY-001
```

## Objective

Validate that release provenance eligibility is explicit, fail-closed, bounded,
privacy-safe, and separate from signed auto-update execution, package
installation, node runtime, and release publishing.

## Scope Checks

Expected changed paths:

```text
docs/architecture/v1-supply-chain-security.md
docs/qa/v1-supply-chain-security.md
docs/roadmap/iamine-product-roadmap.md
iamine-core/src/lib.rs
iamine-core/src/supply_chain_security.rs
```

Expected runtime behavior change:

```text
none
```

This feature must not modify `iamine-node/src/main.rs`, `iamine-node`
runtime, P2P startup, worker behavior, scheduler behavior, model policy,
inference execution, packaging scripts, service templates, update execution, or
release publication.

## Required Local Validation

```bash
cargo fmt --all -- --check
cargo test -p iamine-core supply_chain_security
cargo test -p iamine-core
cargo clippy -p iamine-core --all-targets
git diff --check
git diff --cached --check
```

Run `./scripts/quality-gate.sh` before merge review.

## Expected Results

- default policy rejects release candidates;
- controlled release accepts only verified source, dependency, build,
  artifact, and provenance evidence;
- trusted builder allowlist is bounded;
- dirty tracked worktree evidence rejects;
- dirty staging evidence rejects;
- invalid source commit or tree SHA rejects;
- invalid Cargo.lock digest rejects;
- failed cargo-audit rejects;
- skipped cargo-audit without accepted baseline rejects;
- skipped cargo-audit with accepted baseline is allowed;
- failed cargo-deny rejects;
- skipped cargo-deny without accepted baseline rejects;
- failed secret scan rejects;
- skipped secret scan rejects even with an accepted baseline marker;
- untrusted builder rejects;
- build source mismatch rejects;
- non-isolated build rejects;
- non-reproducible build rejects;
- build without passing tests rejects;
- missing or invalid build provenance rejects;
- missing artifacts reject;
- too many artifacts reject;
- invalid artifact digest rejects;
- artifact source mismatch rejects;
- artifact from untrusted builder rejects;
- missing or invalid artifact provenance rejects.

## Field QA Decision

Field QA is not required for this initial policy-only feature because no
runtime, installer, updater, P2P, worker, scheduler, inference, model,
service-manager, package generation, or release-publishing behavior changes.

Field QA is required for any later feature that wires this gate into installer
execution, updater execution, release packaging, service management, node
startup, network distribution, or runtime artifact replacement.

## Local Results

Status:

```text
LOCAL VALIDATION PASSED
```

Executed on Mac local worktree:

```text
cargo fmt --all -- --check: PASS
cargo test -p iamine-core supply_chain_security: PASS; 14 passed
cargo test -p iamine-core: PASS; 23 passed
cargo clippy -p iamine-core --all-targets: PASS
git diff --check: PASS
git diff --cached --check: PASS
./scripts/quality-gate.sh: PASS WITH WARNINGS
```

Quality gate summary:

```text
required_failures=0
warnings=0
skipped=3
cargo audit: SKIPPED; not installed
cargo deny check: SKIPPED; not installed
gitleaks secret scan: SKIPPED; not installed
```

Scope and size guard:

```text
iamine-core/src/supply_chain_security.rs: 653 lines
iamine-node/src/main.rs: 4929 lines; unchanged
iamine-node/src/cluster_registry.rs: 862 lines; unchanged
runtime behavior changed: none
field QA required: no
```

Warnings:

```text
No warnings were emitted by the new supply-chain module.

Quality gate emitted existing baseline warnings in untouched modules:
- client-rust/src/solana_client.rs
- client-rust/src/solana_config.rs
- iamine-models/src/distributed_inference.rs
- iamine-network/src/prompt_semantic_signals.rs
- iamine-network/src/scheduler.rs
- iamine-node baseline dead_code and clippy::too_many_arguments surfaces
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
