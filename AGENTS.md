# IAMINE Agent Instructions

Stable operating guidance for agents working in the IAMINE repository.

Detailed procedures live in:

- `docs/process/iamine-canonical-workflow.md`
- `docs/process/iamine-context-consolidation.md`

Product milestones, feature ordering, and current roadmap state are owned by:

- `docs/roadmap/iamine-product-roadmap.md`

Do not duplicate temporary feature state or roadmap status in this file.

## Git Workflow

- Branch from `develop`, never from `main`.
- Integrate feature work into `develop`, never directly into `main`.
- Start from the exact authorized `origin/develop`.
- Keep each branch limited to its authorized feature scope.
- Do not mix unrelated refactors or fixes.
- Preserve existing untracked and user artifacts unless explicitly authorized to remove them.
- Do not delete source branches before required post-merge validation is complete.
- Stop a merge if conflicts or unexpected repository state appear.

Typical feature preparation commands:

- `git fetch origin --prune`
- `git switch develop`
- `git pull --ff-only origin develop`
- `git switch -c <branch-name>`

Before implementation, QA, or merge work, record:

- `git branch --show-current`
- `git rev-parse HEAD`
- `git rev-parse 'HEAD^{tree}'`
- `git status --short`

## Responsibilities

Architecture:

- defines problem, scope, contracts, ownership, restrictions, and non-regression rules;
- authorizes Development;
- determines required QA;
- reviews evidence;
- authorizes merge and closure.

Development/Codex:

- implements only the authorized scope;
- keeps domain ownership modular;
- adds appropriate tests;
- performs local validation;
- reports limitations and evidence;
- does not authorize its own merge.

QA:

- validates the exact authorized commit;
- collects reproducible evidence;
- classifies failures and environmental blockers;
- does not modify product code during QA;
- does not authorize merge.

Merge owner:

- verifies branch, base, commit, tree, staging, and repository state;
- performs only the authorized merge;
- stops on conflicts;
- performs post-merge validation.

## Canonical States

Use explicit lifecycle states where applicable:

- `PROPOSED`
- `ARCHITECTURE IN PROGRESS`
- `ARCHITECTURE APPROVED`
- `DEVELOPMENT AUTHORIZED`
- `IMPLEMENTATION IN PROGRESS`
- `IMPLEMENTATION COMPLETE`
- `LOCAL VALIDATION PASSED`
- `ARCHITECTURE REVIEW REQUIRED`
- `FIELD QA AUTHORIZED`
- `FIELD QA IN PROGRESS`
- `QA BLOCKED`
- `CHANGES REQUIRED`
- `READY FOR MERGE REVIEW`
- `APPROVED FOR MERGE`
- `MERGED`
- `POST-MERGE VALIDATION`
- `MERGED / VALIDATED / CLOSED`

Do not report work merely as `done`, `finished`, or `complete`.

Do not declare `MERGED`, `CLOSED`, `QA PASS`, `MERGE APPROVED`, or an equivalent
state without the required evidence and authority.

## Architecture

- Domain behavior belongs in its owning crate or module.
- Keep `iamine-node/src/main.rs` primarily as wiring.
- Avoid unnecessary growth in `iamine-node/src/cluster_registry.rs`.
- Do not modify scheduler, P2P, PubSub, model selection, worker lifecycle,
  inference behavior, model storage, reputation, rewards, or unrelated
  subsystems as a side effect.
- Prefer additive compatibility.
- Preserve supported CPU-only, accelerated, macOS, Linux, VM, container,
  mock-worker, and constrained-host behavior where applicable.
- Prefer explicit types and stable enums over magic strings.
- Avoid duplicated parsing, detection, and policy logic.
- Use bounded resources and explicit cleanup paths.

## Privacy and Security

Do not intentionally commit or expose:

- passwords or credentials;
- tokens;
- private keys;
- wallet secrets;
- personal filesystem paths;
- unnecessary machine identifiers;
- permanent hardware fingerprints;
- unnecessary IP, MAC, hostname, serial, or disk-identifying information.

Remote credentials belong in local SSH or operator configuration, not repository
files.

Unknown, absent, or contradictory security or policy metadata must not silently
authorize new behavior.

## Validation

Use focused checks during implementation and the broader repository gate before
handoff or merge review.

Canonical baseline commands:

- `./scripts/quality-gate.sh`
- `cargo fmt --all -- --check`
- `cargo test -p iamine-models`
- `cargo test -p iamine-network`
- `cargo test -p iamine-node`
- `cargo build -p iamine-node`
- `cargo test --workspace`
- `cargo clippy --workspace --all-targets`
- `git diff --check`
- `git diff --cached --check`

Optional tools such as `cargo audit`, `cargo deny`, and `gitleaks` must be
reported as skipped when unavailable.

Do not report a check as executed when it was not executed.

Historical warnings are not automatically new regressions. Compare them against
the authorized base.

## CLI Smoke Tests

Where relevant:

- `./target/debug/iamine-node --help`
- `./target/debug/iamine-node cluster status`
- `./target/debug/iamine-node cluster status --json`
- `./target/debug/iamine-node tasks stats`

For feature-specific commands, run the corresponding command with `--help`
before operational testing.

## Field QA

Field QA is required when changes affect platform-dependent or operational
behavior such as:

- hardware profiling;
- runtime behavior;
- scheduler behavior;
- worker behavior or lifecycle;
- cluster or capability reporting;
- networking or broadcast behavior;
- inference or model execution.

When required, use the platform matrix defined by the canonical workflow,
including Mac, TS140, and Proxmox/R5500 environments where applicable.

Do not claim remote Field QA was executed when those systems were unavailable.

On the first meaningful QA failure:

- stop the affected sequence;
- classify the failure;
- preserve evidence;
- do not modify product code during QA;
- do not repeat successful checks unless tested identity, tree, scope, or
  Architecture direction changed.

## Git Safety

Do not run destructive cleanup commands unless explicitly authorized and truly
required:

- `git clean`
- `git reset --hard`
- `git checkout -f`
- `git switch -f`
- `cargo clean`

Never remove preserved local or user artifacts merely to obtain a clean test
environment.

Use a disposable checkout or worktree when isolation is required.

Stage only files belonging to the authorized feature.

## Artifact Policy

Do not commit local execution artifacts such as:

- logs;
- `.DS_Store`;
- `target/`;
- model files such as `*.gguf`;
- local QA output;
- generated runtime artifacts;
- secrets or credentials.

Repository-quality evidence intended for project history belongs in the
appropriate `docs/qa/` documentation.

## Handoff

A Development or QA handoff should identify enough state to reproduce what was
validated, including when applicable:

- Project
- Feature
- Branch
- Base
- Commit
- Tree
- Goal
- Production behavior changed
- Files created
- Files updated
- Validation executed
- Test results
- Warnings
- Optional tools skipped
- Field QA required
- Field QA executed
- Known limitations
- Recommendation

Development or QA may recommend:

`READY FOR ARCHITECTURE MERGE REVIEW`

They must not self-authorize merge or closure.

## Current Work

Do not record the currently active feature in this file.

Read the current roadmap and Architecture authorization to determine active
work.

The existence of an old branch on `origin` is not evidence that it remains the
current feature.
