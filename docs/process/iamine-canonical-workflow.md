# IAMINE Canonical Iteration Workflow

This workflow consolidates the current Development/Codex, Architecture, and QA
process for IAMINE. It is intentionally separate from `AGENTS.md`: keep
`AGENTS.md` as stable agent guidance, and keep this document as the detailed
operating procedure.

Product milestone meaning, feature order, and roadmap state are owned by
`docs/roadmap/iamine-product-roadmap.md`. This workflow controls how an
authorized feature moves through delivery; it does not independently select
the next product feature.

## Objective

Every feature must advance through a reproducible, auditable, incremental,
evidence-based process with separated responsibilities.

A feature is not complete merely because code exists or local tests pass.

Canonical lifecycle:

```text
Planning
-> Architecture
-> Development authorization
-> Implementation
-> Local validation
-> Architecture checkpoint review
-> Field QA
-> Final Architecture review
-> Controlled merge
-> Post-merge validation
-> Closure
-> Next feature
```

## Roles

Architecture:

- defines the problem, scope, contracts, ownership, restrictions, and out of
  scope items;
- authorizes Development;
- reviews checkpoints;
- authorizes QA;
- decides required changes or accepted exceptions;
- authorizes merge;
- closes the feature.

Development/Codex:

- inspects the existing code;
- implements only the authorized scope;
- keeps modular ownership clear;
- adds tests;
- runs local validation;
- reports changes, limitations, and evidence;
- does not merge without authorization.

QA:

- validates the exact authorized commit;
- runs local and field checks;
- tests Mac, TS140, and Proxmox when required;
- collects evidence;
- classifies defects, blocks, and test gaps;
- does not modify code;
- does not authorize merge.

Merge owner:

- verifies commit and tree;
- performs only the authorized merge;
- stops on conflicts;
- publishes post-merge evidence.

## Canonical States

```text
PROPOSED
ARCHITECTURE IN PROGRESS
ARCHITECTURE APPROVED
DEVELOPMENT AUTHORIZED
IMPLEMENTATION IN PROGRESS
IMPLEMENTATION COMPLETE
LOCAL VALIDATION PASSED
ARCHITECTURE REVIEW REQUIRED
FIELD QA AUTHORIZED
FIELD QA IN PROGRESS
QA BLOCKED
CHANGES REQUIRED
READY FOR MERGE REVIEW
APPROVED FOR MERGE
MERGED
POST-MERGE VALIDATION
MERGED / VALIDATED / CLOSED
```

Do not use only `done`, `finished`, or `complete`.

## Phase 1 - Planning

Define:

- problem;
- objective;
- affected users or components;
- scope;
- restrictions;
- risks;
- dependencies;
- architecture impact;
- success criteria.

Result:

```text
FEATURE: PROPOSED
```

Do not create a branch until scope is clear enough.

## Phase 2 - Architecture

Architecture defines:

- functional contract;
- affected crates and files;
- interfaces;
- invariants;
- security restrictions;
- compatibility strategy;
- test matrix;
- behavior that must not change.

Result:

```text
DEVELOPMENT AUTHORIZED
```

## Phase 3 - Git Preparation

Start from the authorized `develop` base:

```bash
git fetch origin --prune
git checkout develop
git pull origin develop
git checkout -b <branch-name>
```

Before implementing:

```bash
git branch --show-current
git rev-parse HEAD
git rev-parse 'HEAD^{tree}'
git status --short
```

Confirm:

- exact base;
- known worktree;
- empty staging area;
- historical untracked files recorded;
- no unrelated modifications.

Do not use:

```text
git clean
git reset --hard
git checkout -f
git switch -f
cargo clean
```

## Phase 4 - Implementation

Rules:

- work only on the authorized branch;
- do not mix features;
- keep scope intact;
- preserve compatibility;
- add tests with the implementation;
- do not add unrelated refactors;
- avoid growth in critical files;
- document real limitations;
- do not hide incomplete behavior.

IAMINE prefers:

```text
owner crate/module > new logic in main.rs
explicit types > magic strings
deterministic tests > runner-hardware-dependent tests
```

## Phase 5 - Local Development Validation

Base command set:

```bash
./scripts/quality-gate.sh
cargo fmt --all -- --check
cargo test -p <main-feature-crate> --all-targets
cargo test -p iamine-models
cargo test -p iamine-network
cargo test -p iamine-node
cargo build -p iamine-node
cargo test --workspace
cargo clippy --workspace --all-targets
git diff --check
git diff --cached --check
```

Distinguish:

```text
historical warnings
new warnings caused by the feature
```

If optional tools are unavailable, report them as skipped:

```text
cargo audit
cargo deny
gitleaks
```

## Phase 6 - Development Checkpoint

Development reports:

```text
Project:
Feature:
Branch:
Base:
Commit:
Tree:
PR:

Goal completed:
Production behavior changed:

Files created:
Files updated:
What changed:

Architecture maintenance:
main.rs before/after:
cluster_registry.rs before/after:
largest file:
duplicated logic:
deferred extractions:

Validation executed:
Test counts:
Warnings:
Optional tools skipped:

Field QA required:
Field QA executed:

Known limitations:
Recommendation:
```

Use full SHAs. A `/pull/new/<branch>` URL does not prove a PR exists.

## Phase 7 - Architecture Checkpoint Review

Architecture verifies:

- design was respected;
- scope did not expand;
- integrations are additive;
- invariants are preserved;
- QA can begin.

Possible result:

```text
FIELD QA AUTHORIZED
```

This is not merge authorization.

## Phase 8 - QA Handoff

Architecture gives QA:

- feature;
- branch;
- exact commit;
- base;
- scope;
- criteria;
- environments;
- forbidden regressions;
- validation matrix;
- report format.

QA must validate only the authorized commit:

```text
Expected commit:
<FULL_SHA>
```

If the SHA differs:

```text
RERUN - WRONG COMMIT
```

## Phase 9 - QA Execution

Recurring environments:

```text
MacBook Air
TS140
Proxmox/R5500
```

QA must not change code to force a pass.

### CHECK 0 - Identity, Scope, and Cleanliness

```bash
git branch --show-current
git rev-parse HEAD
git rev-parse 'HEAD^{tree}'
git merge-base HEAD <BASE>
git log --oneline <BASE>..HEAD
git diff --name-status
git diff --cached --name-status
git diff --name-only <BASE> HEAD
git ls-files --others --exclude-standard | sort
```

Validate:

- exact branch;
- exact HEAD;
- exact tree;
- exact base;
- commit count;
- file scope;
- clean tracked worktree;
- clean staging;
- preserved untracked baseline.

Stop if this fails.

### CHECK 1 - Build and Local Tests

```bash
cargo fmt --all -- --check
cargo test -p <feature-crate>
cargo test -p iamine-node
cargo test --workspace
cargo build -p iamine-node
```

Also confirm:

- no panic;
- no SIGILL;
- no compilation errors;
- no unintended IAMINE process;
- no unintended profile or persistent state;
- repository intact.

### CHECK 2 - Mac Field QA

For hardware profiler:

```bash
IAMINE_HARDWARE_PROFILE_PATH=/isolated/path/profile.json \
  ./target/debug/iamine-node hardware inspect

IAMINE_HARDWARE_PROFILE_PATH=/isolated/path/profile.json \
  ./target/debug/iamine-node hardware inspect --json

IAMINE_HARDWARE_PROFILE_PATH=/isolated/path/profile.json \
  ./target/debug/iamine-node hardware inspect --dynamic

IAMINE_HARDWARE_PROFILE_PATH=/isolated/path/profile.json \
  ./target/debug/iamine-node hardware inspect --dynamic --json

IAMINE_HARDWARE_PROFILE_PATH=/isolated/path/profile.json \
  ./target/debug/iamine-node hardware refresh --yes --json

IAMINE_HARDWARE_PROFILE_PATH=/isolated/path/profile.json \
  ./target/debug/iamine-node hardware show --json
```

Validate human output, JSON, schema, CPU, memory, accelerator, dynamic profile,
persistence, privacy, no unintended runtime, and no model store mutation.

### CHECK 3A - TS140 Field QA

Sync branch safely:

```bash
git fetch origin \
  +refs/heads/<BRANCH>:refs/remotes/origin/<BRANCH>

git switch <BRANCH>
git merge --ff-only origin/<BRANCH>
```

If the local branch does not exist:

```bash
git switch \
  --create <BRANCH> \
  --track origin/<BRANCH>
```

Run the same functional checks with an isolated profile path.

Validate specifically:

```text
linux/x86_64
CPU-only
AVX2 detected
FMA detected
NEON not active
Metal not active
non-unified memory
restrictive permissions
no SIGILL
```

Preserve existing worker, model store, real profile, untracked files, and Git
identity.

### CHECK 3B - TS140 Worker Regression

Verify the feature did not alter worker startup.

Rules:

- never stop the preexisting worker;
- use a free port;
- use isolated `HOME`;
- use mock backend;
- avoid real model loads;
- stop only the QA-created process.

Variables:

```bash
IAMINE_SKIP_MODEL_LOAD_ON_STARTUP=1
IAMINE_INFERENCE_BACKEND=mock
```

Validate executable, args, environment, open port, stable startup, no downloads,
no GGUF load, no panic/SIGILL, and cleanup.

### CHECK 4A - Proxmox/R5500 Preflight

Before sync or build:

```bash
ssh pve-r5500 'pveversion'
ssh pve-r5500 'qm list'
ssh pve-r5500 'pct list'
```

Expected guests:

```text
iamine-ctrl
iamine-wrk1
iamine-wrk2
iamine-heavy
```

Check each guest for SSH, virtualization, cgroups, visible CPU, visible memory,
guest limits, repo, branch, HEAD, toolchain, IAMINE processes, mock/skip
backend where relevant, model store, profiles, and untracked baseline.

This check is read-only.

### CHECK 4B - Proxmox Field QA

After preflight:

1. Sync only required guests.
2. Build the feature.
3. Run profiler with isolated path.
4. Validate JSON and schema.
5. Confirm it reports guest-visible resources.
6. Confirm it does not leak full physical host resources.
7. Validate cgroups.
8. Preserve workers, models, and profiles.
9. Confirm no real model load or download.

### Persistence and Safety Matrix

Cover:

- happy path: `refresh` creates profile, `show` returns it,
  `refresh == show == persisted file`;
- permissions: `0600` or equivalent, no group/other access;
- corrupt JSON: controlled error, no panic, no accidental overwrite;
- write failure: previous profile intact, no partial file;
- concurrency: simultaneous refreshes produce valid JSON and no corruption;
- cancellation: no residual process, stale lock, or partial file;
- limits: bounded dynamic benchmark, memory, duration, and temp storage.

### Quality Gate

Run after Field QA:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets
cargo test --workspace
cargo build -p iamine-node
git diff --check
git diff --cached --check
```

Compare Clippy against the base in an isolated worktree:

```bash
git worktree add /tmp/iamine-base-quality <BASE>
cd /tmp/iamine-base-quality
cargo clippy --workspace --all-targets
```

Then run feature Clippy and classify each warning:

```text
preexisting
new by feature
blocking
non-blocking
```

## QA Result Classifications

Allowed:

```text
PASS COMPLETO
PASS WITH ACCEPTED BASELINE EXCEPTION
FAIL
RERUN
BLOCKED
TEST GAP
```

Positive QA recommendation:

```text
READY FOR ARCHITECTURE MERGE REVIEW
```

QA never emits:

```text
MERGE AUTHORIZED
MERGE APPROVED
```

## Defects, Gaps, and Reruns

For a test gap, QA reports expected behavior, observed behavior, evidence,
uncovered scenarios, testability limitation, risk, and decision options.

Architecture may answer:

```text
ARCHITECTURE DECISION: REQUIRE TARGETED TESTS
ARCHITECTURE DECISION: COVERAGE EXCEPTION ACCEPTED
ARCHITECTURE DECISION: CHANGES REQUIRED
```

If Development changes production code, rerun all affected checks. Do not reuse
evidence automatically. All evidence belongs to the exact SHA it validated.

## Phase 10 - Final QA Report

QA reports:

```text
Feature:
Branch:
Expected commit:
Base:

Overall result:
Recommendation:

Checks:
Mac:
TS140:
Proxmox:
Quality gates:
Warnings:
Test gaps:
Accepted exceptions:

Blocking defect:
Merge authorized by QA:
NO

Architecture review required:
YES
```

## Phase 11 - Final Architecture Decision

Architecture may issue:

```text
APPROVED FOR MERGE
CHANGES REQUIRED
```

Merge authorization must identify the exact commit and tree.

## Phase 12 - Merge Precheck

Before merge:

```text
fetch origin
checkout feature
verify HEAD
verify tree
verify worktree
verify staging
verify diff
checkout develop
pull --ff-only
```

Confirm exact HEAD, exact tree, current `develop`, clean tracked worktree, and
clean staging. Preserve local untracked files.

## Phase 13 - Controlled Merge

Merge only into `develop`:

```bash
git checkout develop
git pull origin develop
git merge --no-ff origin/<source-branch> -m "Merge pull request: <summary>"
git push origin develop
```

Rule:

```text
validated commit -> merge -> no additional changes
```

If conflicts occur:

```text
STOP
MERGE NOT COMPLETED
ARCHITECTURE REVALIDATION REQUIRED
```

Do not resolve conflicts automatically and continue with a tree different from
the validated tree.

## Phase 14 - Post-Merge Validation

Verify:

- merge commit;
- `develop`;
- `origin/develop`;
- approved commit ancestry;
- worktree and staging;
- format;
- main tests;
- workspace;
- build;
- diff check;
- CLI smoke;
- primary feature contract.

Suggested command set:

```bash
git checkout develop
git pull origin develop
./scripts/quality-gate.sh
cargo fmt --all -- --check
cargo test -p iamine-models
cargo test -p iamine-network
cargo test -p iamine-node
cargo build -p iamine-node
cargo test --workspace
git diff --check
```

Run minimum CLI smoke afterward.

## Phase 15 - Closure

Only Architecture declares:

```text
FEATURE:
MERGED / VALIDATED / CLOSED
```

Closure records feature, approved commit, merge commit, resulting base, QA,
exceptions, incorporated contract, pending debt, and next authorized feature.

If post-merge validation fails:

```text
POST-MERGE FAILURE
NEXT FEATURES BLOCKED
```

## Phase 16 - Operational Normalization

When a feature changed or stopped real processes:

- sync TS140;
- rebuild binary;
- restart worker;
- confirm readiness;
- confirm cluster status;
- preserve model store;
- confirm mocks when relevant.

This belongs to QA or operations, not Architecture chat.

## Phase 17 - Next Feature

Do not start the next feature until the prior feature is:

```text
MERGED / VALIDATED / CLOSED
```

Exception: Architecture explicitly authorizes parallel work.

When starting the next feature, fix:

```text
Feature
Branch
Full base SHA
Dependencies
Scope
Out of scope
```

## Permanent Scope Rules

Each feature needs:

```text
one main responsibility
one clear owner
one exact base
one testable contract
```

Do not mix these in one feature without authorization:

- hardware profiler;
- model classification;
- certified benchmark;
- reputation;
- rewards;
- trusted registry.

When a new requirement appears, register it, identify dependencies, assign a
future feature, and do not add it to the active feature unless authorized.

## Current Applied State

This workflow does not duplicate current feature status. Read
`docs/roadmap/iamine-product-roadmap.md` for the reconciled product sequence.
Before opening a new feature branch, Architecture must select the next feature
from that roadmap or explicitly register an enabling feature, then record its
exact base, scope, dependencies, and out-of-scope items.
