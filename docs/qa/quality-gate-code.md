# IAMINE Code Quality Gate

## Purpose

`QUALITY-GATE-CODE-001` defines the first formal local and CI quality gate for IAMINE. It protects `develop` before scheduler, task lifecycle, model registry safety, and future milestones continue.

The gate is intentionally focused on infrastructure and code health. It does not change runtime behavior.

## Local Command

Run:

```bash
./scripts/quality-gate.sh
```

The script exits non-zero when required checks fail. Optional checks are reported as `PASS`, `WARN`, or `SKIPPED`.

The local script and CI test jobs run package tests with `RUST_TEST_THREADS=1` by default. This keeps the gate deterministic while existing tests still share process-wide environment variables and temporary log locations.

## Required Blocking Checks

These checks block merge when they fail:

```bash
cargo fmt --all -- --check
cargo test -p iamine-node
cargo test -p iamine-network
cargo build -p iamine-node
git diff --check
git diff --cached --check
```

## Informational Checks

These checks are useful but non-blocking in the first iteration:

```bash
cargo clippy --workspace --all-targets
cargo audit
cargo deny check
gitleaks detect --source . --no-git --redact
```

If a tool is not installed locally, the script reports `SKIPPED` and prints the install hint.

## CI Expectations

GitHub Actions runs required jobs on pull requests to `develop` and `main`, and on pushes to `develop`:

- format
- iamine-node tests
- iamine-network tests
- iamine-node build
- diff whitespace check

Informational CI jobs may warn without blocking:

- clippy
- cargo audit
- cargo deny, when configured
- secret scan status

## Architecture Guard

The quality gate records `iamine-node/src/main.rs` line count and compares it to `QUALITY_GATE_BASE_REF`, defaulting to `origin/develop` when available.

Current policy:

- warn if `main.rs` grows by more than 250 lines relative to the base ref
- warn if any non-main Rust file exceeds 1000 lines
- keep new feature logic in cohesive modules
- keep runtime side effects at module boundaries

These checks are warnings in the first gate iteration. They are intended to guide review without destabilizing active development.

## Merge Blocking Policy

Block merge when:

- formatting fails
- required tests fail
- iamine-node build fails
- diff whitespace checks fail
- the quality gate script cannot run required checks

Do not block merge solely because:

- optional tools are missing
- informational clippy/audit/deny/secret checks warn
- known baseline warnings are unchanged

## Known Non-Blockers

These are documented as non-blocking unless they regress:

- existing Rust `dead_code` warnings
- `worker_startup_invalid_math` for worker ports below the metrics base
- metrics fallback for ports below the metrics base
- `backend_cpu_feature_incompatible` on legacy Proxmox CPU without AVX2
- cluster assignment log spam
- Proxmox/R5500 field smoke pending due Dell availability
- `QA-CLI-UNKNOWN-MODE-EXIT-CODE-007`
- `QA-MODEL-REGISTRY-SAFETY-METADATA-001`

## Branch Policy

- `main` remains the stable branch.
- `develop` is the active integration branch.
- Feature, refactor, fix, and chore branches target `develop`.
- Do not merge directly to `main` from quality-gate or feature branches.
