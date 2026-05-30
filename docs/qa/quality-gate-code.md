# IAMINE Code Quality Gate

## Purpose

`QUALITY-GATE-STRICTNESS-002` hardens the local and CI quality gate without turning
historical debt or missing optional tools into immediate merge blockers. It protects
`develop` while architecture cleanup continues and does not change runtime behavior.

## Local Command

Run:

```bash
./scripts/quality-gate.sh
```

The script prints required checks, optional checks, architecture guards, repository
guards, counters, and one final result:

- `PASS`: all required checks and guards pass, with no warnings or skips.
- `PASS WITH WARNINGS`: required checks pass, but optional tools are missing or
  informational warnings remain. Exit code is `0`.
- `FAIL`: a required check or blocking guard fails. Exit code is non-zero.

Package and workspace tests run with `RUST_TEST_THREADS=1` by default. This keeps the
gate deterministic while existing tests still share process-wide environment
variables and temporary paths.

For fast, non-destructive guard validation without builds or tests:

```bash
QUALITY_GATE_GUARDS_ONLY=1 ./scripts/quality-gate.sh
```

## Required Blocking Checks

These checks block merge when they fail:

```bash
cargo fmt --all -- --check
cargo test -p iamine-models
cargo test -p iamine-network
cargo test -p iamine-node
cargo build -p iamine-node
cargo test --workspace
git diff --check
git diff --cached --check
```

`cargo test --workspace` is required because it completes in an acceptable time on
the current repository and catches cross-crate regressions.

## Optional Checks

These tools are valuable but remain optional while their baselines mature:

```bash
cargo clippy --workspace --all-targets
cargo audit
cargo deny check
gitleaks detect --source . --no-git --redact
```

When an optional tool is absent locally, the script reports `SKIPPED`, increments the
skip count, prints an install hint, and keeps the gate non-blocking.

When installed:

- Clippy failures are warnings until historical lint debt is classified.
- `cargo audit` and `cargo deny` failures are warnings until dependency-policy
  baselines are classified.
- A GitLeaks finding is blocking. Missing GitLeaks remains a local skip.

Install optional tools with:

```bash
rustup component add clippy
cargo install cargo-audit
cargo install cargo-deny
# Install GitLeaks from https://github.com/gitleaks/gitleaks
```

## Architecture Guards

### main.rs

Current baseline:

- `iamine-node/src/main.rs`: `4830` lines
- warn above `5000` lines
- fail above `5500` lines
- fail when a branch grows `main.rs` by more than `150` lines relative to its
  merge-base

The merge-base reference defaults to `origin/develop`. Override it when needed:

```bash
QUALITY_GATE_BASE_REF=origin/main ./scripts/quality-gate.sh
```

### Rust File Size

For tracked Rust files other than `main.rs`:

- warn above `900` lines
- fail above `1500` lines

The gate also reports focused regression checks for:

- `iamine-node/src/broadcast_runtime.rs`
- `iamine-network/src/prompt_analyzer.rs`

At this checkpoint no non-main Rust file exceeds `900` lines. The largest current
files are expected to remain below that threshold.

### New Debt Markers

The gate inspects added Rust lines relative to merge-base and warns when they add:

- `TODO`
- `FIXME`
- `.unwrap()`
- `.expect()`
- `panic!()`

This guard is intentionally warning-only while historical debt is addressed.

## Repository Guards

Repository guards inspect tracked files only. Existing untracked local logs and
artifacts do not produce false positives.

The gate fails when tracked files include generated artifacts or local model binaries:

- `target/`
- `iamine-logs/`
- `.DS_Store`
- `*.log`
- `*.ndjson`
- `*.gguf`
- `*.safetensors`

The gate also fails when tracked files include sensitive material:

- `.env` files, except documented `.env.example`, `.env.sample`, or `.env.template`
- `*.pem`
- `*.key`
- `id_rsa`
- `id_ed25519`
- `secrets.*`
- wallet or private-key material paths

GitLeaks provides an additional secret scan when installed locally and runs as a
blocking GitHub Actions job.

## CI Expectations

GitHub Actions runs required jobs on pull requests to `develop` and `main`, and on
pushes to `develop`:

- format
- iamine-models tests
- iamine-network tests
- iamine-node tests
- iamine-node build
- workspace tests
- diff whitespace checks
- complete quality-gate script

Informational CI jobs may warn without blocking:

- Clippy
- cargo audit
- cargo deny, when configured

The GitLeaks job blocks when it detects a secret.

## Failure Handling

When a required check fails:

1. Read the failing command in the summary.
2. Re-run that command directly.
3. Fix the underlying issue instead of weakening the gate.
4. Re-run `./scripts/quality-gate.sh`.

When an optional tool is absent, install it when practical or record the skip in the
checkpoint. Absence alone does not block the branch.

## Known Non-Blockers

These remain documented as non-blocking unless they regress:

- existing Rust `dead_code` warnings
- existing Clippy warnings
- metrics fallback for worker ports below the metrics base
- optional cargo-audit and cargo-deny baselines not yet classified
- optional local GitLeaks installation

## Future Strictness

Tighten the gate incrementally after debt is measured:

1. Classify Clippy warnings and block new lint debt.
2. Add and classify cargo-deny policy.
3. Classify dependency advisories from cargo-audit.
4. Reduce file-size thresholds when architecture cleanup makes that practical.

## Branch Policy

- `main` remains the stable branch.
- `develop` is the active integration branch.
- Feature, refactor, fix, and chore branches target `develop`.
- Do not merge directly to `main` from quality-gate or feature branches.
