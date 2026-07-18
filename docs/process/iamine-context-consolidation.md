# IAMINE Context Consolidation

This consolidation was produced from the Development/Codex, Architecture, QA,
and canonical workflow handoffs.

## What Should Go In `AGENTS.md`

Use `AGENTS.md` for stable operating rules:

- branch from `develop`, never `main`;
- merge into `develop`, never `main`;
- preserve untracked/user artifacts unless explicitly told otherwise;
- keep domain logic out of `iamine-node/src/main.rs`;
- avoid growth in `iamine-node/src/cluster_registry.rs`;
- require field QA for runtime, scheduler, capabilities, cluster status, worker,
  and hardware profiling changes;
- keep privacy constraints explicit;
- list canonical validation commands;
- list CLI smoke commands;
- define size guards;
- define canonical feature states;
- require a named exhaustive milestone QA gate before any non-historical
  milestone transitions to `CLOSED`;
- define Git precheck and merge-control checks;
- define completion report format;
- forbid declaring `MERGED`, `CLOSED`, or `QA PASS` without evidence.

## What Should Become Scripts

Create scripts for repeatable checks:

- `scripts/qa-local-gate.sh`: implemented by
  `PROCESS-QA-LOCAL-GATE-001`; local identity, scope, quality gate, tests,
  build, and diff checks.
- `scripts/qa-hardware-smoke.sh`: isolated `IAMINE_HARDWARE_PROFILE_PATH`,
  hardware inspect/refresh/show, JSON parse, schema check, persistence equality.
- `scripts/qa-size-guard.sh`: file-size thresholds for `main.rs`,
  `cluster_registry.rs`, and large Rust files.
- `scripts/qa-remote-proxmox-preflight.sh`: SSH alias checks only, no guest sync.
- `scripts/qa-remote-ts140-smoke.sh`: TS140 worker/profile checks when explicitly
  requested.
- `scripts/merge-precheck.sh`: branch, HEAD, tree, staging, untracked baseline,
  and `develop`/`main` relationship checks.
- `scripts/post-merge-validation.sh`: post-merge quality gate, targeted tests,
  build, diff check, and CLI smoke.

Keep scripts non-secret. Use SSH aliases and local config.

## What Should Stay In Docs

Keep long procedure and evidence in documentation:

- `docs/roadmap/iamine-product-roadmap.md`
- `docs/qa/node-hardware-profiler.md`
- `docs/qa/remote-field-qa.md`
- `docs/architecture/hardware-profiler.md`
- `docs/architecture/model-gates.md`
- `docs/qa/templates/checkpoint-report.md`
- `docs/qa/agent-milestone-qa-gates.md`
- `docs/process/iamine-canonical-workflow.md`

Do not put full logs or run evidence in `AGENTS.md`.

## Current Project State Verified From Git

- Canonical integration branch: `develop`.
- Product sequence and reconciled feature states live only in
  `docs/roadmap/iamine-product-roadmap.md`.
- Agent Network milestone closure gate IDs and states live in
  `docs/roadmap/iamine-agent-network-roadmap.md`.
- This context document must not duplicate roadmap state or select the next
  product feature.
- Before opening a feature branch, Architecture must still record the exact
  feature, base, scope, dependencies, and out-of-scope items.

## Parallel Work Model

Use parallel agents only when file ownership is clear:

- Architecture thread: reviews contracts, gates, and non-regression risks.
- QA thread: runs field checks and produces evidence, without modifying code.
- Development thread: implements scoped changes and tests them.
- Integration thread: merges only after Architecture/QA evidence is complete.

Avoid two agents editing the same files. Use worktrees or separate branches for
parallel implementation work.

Canonical roles:

- Architecture defines scope, contracts, restrictions, and merge authorization.
- Development/Codex implements the authorized scope and reports checkpoints.
- QA validates the exact authorized commit and emits evidence, without editing
  code or authorizing merge.
- Merge owner performs only the Architecture-authorized merge and post-merge
  validation.

## Immediate Next Action

Read `docs/roadmap/iamine-product-roadmap.md`, then select the next feature
through Architecture. Do not reuse a stale feature branch as active work merely
because it still exists on `origin`.
