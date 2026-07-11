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
PASS: executed from disposable worktree after feature branch push.
```

TS140 identity and validation on 2026-07-10 / 2026-07-11:

```text
canonical path: /home/ts140/iamine
canonical branch: feature/wan-peer-discovery-001
canonical state: dirty, preserved
QA path: /tmp/iamine-qa-private-testnet-release-fb73154-20260710233101
HEAD: fb73154144f84d8aed96c0cb3bb4b9894e6d5a4a
Tree: 9876ccc42a41d2d947477c94c194826130175c92
Base: de1b35e50455fa9b639d9dac01b22c3a239b6bc6
tracked clean: yes
staging clean: yes
```

TS140 sync note:

```text
Initial merge-base check reported stale origin/develop on TS140.
Resolution used fetch-only remote-tracking sync:
git fetch origin +refs/heads/develop:refs/remotes/origin/develop
No source reset, clean, checkout -f, switch -f, or canonical worktree edit was used.
Classification: environment sync precondition, not product regression.
```

TS140 results:

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
./target/debug/iamine-node cluster status --json: PASS, JSON payload parseable
./target/debug/iamine-node cluster stress --requests 0 --profile testnet-load-resilience --json: PASS
git diff --check: PASS
git diff --cached --check: PASS
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
PASS: executed from disposable worktrees on all four Proxmox/R5500 guests.
```

Proxmox/R5500 identity on 2026-07-10 / 2026-07-11:

```text
canonical path on every guest: /home/iamine/work/iamine
canonical branch on every guest: feature/wan-peer-discovery-001
canonical state on every guest: dirty, preserved

iamine-ctrl QA path: /tmp/iamine-qa-private-testnet-release-fb73154-20260710234032
iamine-wrk1 QA path: /tmp/iamine-qa-private-testnet-release-fb73154-20260710234046
iamine-wrk2 QA path: /tmp/iamine-qa-private-testnet-release-fb73154-20260710234101
iamine-heavy QA path: /tmp/iamine-qa-private-testnet-release-fb73154-20260710234117

HEAD on every QA worktree: fb73154144f84d8aed96c0cb3bb4b9894e6d5a4a
Tree on every QA worktree: 9876ccc42a41d2d947477c94c194826130175c92
Base on every QA worktree: de1b35e50455fa9b639d9dac01b22c3a239b6bc6
tracked clean on every QA worktree: yes
staging clean on every QA worktree: yes
```

Targeted validation passed on all four Proxmox/R5500 guests:

```text
cargo fmt --all -- --check: PASS
cargo test -p iamine-network protocol_version: PASS
cargo test -p iamine-network secure_transport: PASS
cargo test -p iamine-network testnet_admission: PASS
cargo test -p iamine-node remote_inference_api: PASS
cargo test -p iamine-node testnet_observability: PASS
cargo test -p iamine-node cluster_stress: PASS
cargo build -p iamine-node: PASS
./target/debug/iamine-node --help: PASS
./target/debug/iamine-node cluster status --json: PASS, JSON payload parseable
./target/debug/iamine-node cluster stress --requests 0 --profile testnet-load-resilience --json: PASS
git diff --check: PASS
git diff --cached --check: PASS
```

Proxmox/R5500 QA log roots:

```text
iamine-ctrl: /tmp/iamine-private-testnet-release-qa-iamine-ctrl-20260710234306
iamine-wrk1: /tmp/iamine-private-testnet-release-qa-iamine-wrk1-20260710235821
iamine-wrk2: /tmp/iamine-private-testnet-release-qa-iamine-wrk2-20260711001252
iamine-heavy: /tmp/iamine-private-testnet-release-qa-iamine-heavy-20260711002744
```

Field stress finding:

```text
First stress attempt from iamine-ctrl without active QA workers:
- total_requests=30
- observed_requests=30
- completed=0
- failed=30
- timed_out=30
- resilience.blocking_failures=["failed_requests", "timed_out_requests"]

Classification: harness/environment precondition. The stress profile requires
active workers for non-zero request execution; the zero-request smoke had
already validated the local profile shape.
```

Field stress with QA workers:

```text
workers:
- iamine-wrk1, mock backend, skip model load, QA pid stopped after run
- iamine-wrk2, mock backend, skip model load, QA pid stopped after run
- iamine-heavy, mock backend, skip model load, QA pid stopped after run

worker log roots:
- /tmp/iamine-private-testnet-release-worker-wrk1-20260711004556
- /tmp/iamine-private-testnet-release-worker-wrk2-20260711004556
- /tmp/iamine-private-testnet-release-worker-heavy-20260711004556

controller stress log root:
- /tmp/iamine-private-testnet-release-fieldstress-with-workers-20260711004629

command:
./target/debug/iamine-node cluster stress \
  --requests 30 \
  --concurrency 6 \
  --task reverse_string \
  --profile testnet-load-resilience \
  --prefix qa-private-testnet-release-workers-20260711 \
  --json

result:
- passed=true
- resilience.passed=true
- resilience.blocking_failures=[]
- total_requests=30
- observed_requests=30
- completed=30
- failed=0
- timed_out=0
- duplicate_results=0
- duplicate_executions=0
- duplicate_request_ids=0
- duplicate_task_ids=0
- incompatible_assignments=0
- p95_latency_ms=1168
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

QA recommendation:

```text
READY FOR ARCHITECTURE MERGE REVIEW
```
