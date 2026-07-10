# IAMINE Private Testnet Release Gate QA

Feature:

```text
PRIVATE-TESTNET-RELEASE-001
```

## Objective

Validate that the v0.9 private-testnet release-gate package is coherent,
evidence-backed, privacy-safe, and executable across Mac, TS140, and
Proxmox/R5500 without adding runtime behavior.

## Identity

Record before QA:

```text
Branch:
HEAD:
Tree:
Base:
origin/develop:
tracked clean:
staging clean:
untracked baseline:
```

Mac local QA identity recorded on 2026-07-10 before the pre-push local gate:

```text
Branch: feature/private-testnet-release-001
HEAD: de1b35e50455fa9b639d9dac01b22c3a239b6bc6
Tree: d8489d7da444bed5a4abe85a64d3cdff76647c32
Base: de1b35e50455fa9b639d9dac01b22c3a239b6bc6
origin/develop: de1b35e50455fa9b639d9dac01b22c3a239b6bc6
tracked clean: no, documentation delta for this feature only
staging clean: yes before staging
untracked baseline:
- docs/architecture/private-testnet-release.md
- docs/qa/private-testnet-release.md
- docs/roadmap/v0.9-private-testnet-release-gate.md
- logs/iamine-node.ndjson (sha256 527d1ad6167a27be7db91206490b361457485bc803aff5e4b54b7f560e1af322)
- iamine-node/logs/iamine-node.ndjson (sha256 d10887a5260b0fbcb60a4f71dcdbe9da94a5ed0d7945b5d44cc7b7c220622e8a)
```

The two log artifacts are generated local QA output and must remain untracked.

## Local Validation

Required local checks:

```bash
cargo fmt --all -- --check
cargo test -p iamine-network protocol_version secure_transport testnet_admission
cargo test -p iamine-node remote_inference_api testnet_observability cluster_stress
cargo build -p iamine-node
./target/debug/iamine-node --help
./target/debug/iamine-node cluster status --json
./target/debug/iamine-node cluster stress --requests 0 --profile testnet-load-resilience --json
./scripts/quality-gate.sh
git diff --check
git diff --cached --check
```

Expected:

- no runtime code changes are required;
- help exposes private-testnet relevant CLI surfaces;
- status JSON remains parseable;
- zero-request testnet load-resilience smoke passes without starting workers;
- quality gate required checks pass;
- optional tools are skipped if unavailable.

Mac local results on 2026-07-10:

```text
cargo fmt --all -- --check: PASS
cargo test -p iamine-network protocol_version: PASS (5 passed)
cargo test -p iamine-network secure_transport: PASS (4 passed)
cargo test -p iamine-network testnet_admission: PASS (6 passed)
cargo test -p iamine-node remote_inference_api: PASS (6 passed)
cargo test -p iamine-node testnet_observability: PASS (2 passed)
cargo test -p iamine-node cluster_stress: PASS (43 passed)
cargo build -p iamine-node: PASS
./target/debug/iamine-node --help: PASS
./target/debug/iamine-node cluster status --json: PASS, JSON parseable
./target/debug/iamine-node cluster stress --requests 0 --profile testnet-load-resilience --json: PASS, passed=true, resilience.passed=true
./scripts/quality-gate.sh: PASS WITH WARNINGS
git diff --check: PASS
git diff --cached --check: PASS
```

Quality-gate required checks passed:

```text
cargo fmt --all -- --check: PASS
cargo test -p iamine-models: PASS (99 unit, 59 integration)
cargo test -p iamine-network: PASS (154 unit, 4 routing)
cargo test -p iamine-node: PASS (471 unit)
cargo build -p iamine-node: PASS
cargo test --workspace: PASS
git diff --check: PASS
git diff --cached --check: PASS
required_failures=0
```

Quality-gate optional checks:

```text
cargo clippy --workspace --all-targets: PASS WITH BASELINE WARNINGS
cargo audit: SKIPPED, command not available
cargo deny check: SKIPPED, command not available
gitleaks: SKIPPED, command not available
```

Warnings observed were existing baseline categories: dead code in
`iamine-node`, unused/deprecated Solana client items in `iamine-client`, and
`too_many_arguments` / `type_complexity` clippy warnings in existing modules.
No new runtime code was added by this feature.

## TS140 Field QA

Use a disposable QA worktree if `/home/ts140/iamine` is dirty or on another
feature.

Required:

```bash
git fetch origin +refs/heads/feature/private-testnet-release-001:refs/remotes/origin/feature/private-testnet-release-001
git worktree add --detach /tmp/iamine-qa-private-testnet-release-<short-head> origin/feature/private-testnet-release-001
```

Then validate:

```bash
cargo fmt --all -- --check
cargo test -p iamine-network protocol_version secure_transport testnet_admission
cargo test -p iamine-node remote_inference_api testnet_observability cluster_stress
cargo build -p iamine-node
./target/debug/iamine-node --help
./target/debug/iamine-node cluster status --json
./target/debug/iamine-node cluster stress --requests 0 --profile testnet-load-resilience --json
git diff --check
git diff --cached --check
```

Expected:

- exact branch HEAD, tree, and base match local QA;
- tracked and staged state remain clean;
- generated logs remain untracked;
- no real model load is triggered;
- no worker, P2P, or inference runtime starts outside explicit smoke commands.

Status:

```text
PENDING: requires feature branch push before TS140 can fetch exact QA identity.
```

## Proxmox/R5500 Field QA

Use disposable QA worktrees on:

```text
iamine-ctrl
iamine-wrk1
iamine-wrk2
iamine-heavy
```

Required identity checks on every guest:

```bash
git rev-parse HEAD
git rev-parse 'HEAD^{tree}'
git merge-base HEAD origin/develop
git status --short
```

Required targeted validation on every guest:

```bash
cargo fmt --all -- --check
cargo test -p iamine-network protocol_version secure_transport testnet_admission
cargo test -p iamine-node remote_inference_api testnet_observability cluster_stress
cargo build -p iamine-node
./target/debug/iamine-node --help
./target/debug/iamine-node cluster status --json
./target/debug/iamine-node cluster stress --requests 0 --profile testnet-load-resilience --json
```

Required field stress from `iamine-ctrl` when workers are available:

```bash
./target/debug/iamine-node cluster stress \
  --requests 30 \
  --concurrency 6 \
  --task reverse_string \
  --profile testnet-load-resilience \
  --prefix qa-private-testnet-release-<timestamp> \
  --json
```

Expected:

- stress command exits 0;
- `passed=true`;
- `resilience.passed=true`;
- `resilience.blocking_failures=[]`;
- zero failures, timeouts, duplicate results, duplicate executions, duplicate
  request IDs, duplicate task IDs, and incompatible assignments;
- no `SIGILL`;
- workers remain in mock/skip mode unless Architecture explicitly authorizes a
  real backend run;
- QA-owned workers are stopped after the run and logs are preserved.

Status:

```text
PENDING: requires feature branch push before Proxmox/R5500 guests can fetch exact QA identity.
```

## Release Gate Decision

The feature can move to `READY FOR MERGE REVIEW` when:

- local validation passes;
- TS140 validation passes or any blocker is classified as environment/harness;
- Proxmox/R5500 validation passes or any blocker is classified as
  environment/harness;
- release-gate docs accurately preserve the distinction between launch-package
  readiness and the future 2-4 week operational soak.

QA must not emit `MERGE APPROVED`.
