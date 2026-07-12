# IAMINE Public Testnet Documentation QA

Feature:

```text
PUBLIC-TESTNET-DOCUMENTATION-001
```

## Objective

Validate that the public-testnet documentation baseline is explicit,
privacy-safe, aligned with the canonical roadmap, and does not launch public
beta or change runtime behavior.

## Identity

Record before QA:

```text
Branch: feature/public-testnet-documentation-001
HEAD: 7dc1f11b1fb244029e75765cb18a55336285e587
Tree: 8c001f98545bac33d11e21996ed2fd840d289e7b
Base: origin/develop
origin/develop: 7dc1f11b1fb244029e75765cb18a55336285e587
tracked clean: no; feature delta is limited to expected tracked paths
staging clean: yes before final staging
untracked baseline: none expected in feature worktree
post-gate untracked: iamine-node/logs/ generated locally; preserved and not staged
```

## Scope Checks

Expected changed paths:

```text
README.md
docs/architecture/public-testnet-documentation.md
docs/public-testnet/README.md
docs/qa/public-testnet-documentation.md
docs/roadmap/iamine-product-roadmap.md
```

Expected runtime behavior change:

```text
none
```

This feature must not modify Rust source, tests, packaging scripts, service
templates, runtime startup, P2P, PubSub, worker behavior, scheduler behavior,
model policy, inference execution, updater execution, or rollback execution.

## Required Local Validation

```bash
git diff --check
rg -n "Q2 2026|Quick Start \\(Testnet\\)|ganas|recibe .*token" README.md
rg -n "inference-only public beta" docs/public-testnet/README.md docs/architecture/public-testnet-documentation.md
rg -n "public beta launch|public-testnet participation as closed|not open yet" README.md docs/public-testnet/README.md docs/architecture/public-testnet-documentation.md
rg -n "private key|token|credential|IP address|MAC address|hostname|home director" docs/public-testnet/README.md docs/architecture/public-testnet-documentation.md
```

Run `./scripts/quality-gate.sh` before merge review when local disk capacity
allows it. If unavailable due to environment, classify the limitation and run
the narrow docs checks above.

## Observed Local Results

```text
git diff --check: PASS
README stale/public reward claims scan: PASS; no matches
public beta wording scan: PASS; matches are only in explicit rejected-meaning context
pre-public/not-open wording scan: PASS
privacy boundary scan: PASS; matches are only in explicit prohibition context
```

Quality gate:

```text
./scripts/quality-gate.sh: PASS WITH WARNINGS
required_failures=0
warnings=0
skipped=3
```

Required checks:

```text
cargo fmt --all -- --check: PASS
cargo test -p iamine-models: PASS
cargo test -p iamine-network: PASS
cargo test -p iamine-node: PASS
cargo build -p iamine-node: PASS
cargo test --workspace: PASS
git diff --check: PASS
git diff --cached --check: PASS
```

Optional checks:

```text
cargo clippy --workspace --all-targets: PASS
cargo audit: SKIPPED; not available
cargo deny check: SKIPPED; not available
gitleaks secret scan: SKIPPED; not available
```

Warnings observed during clippy/build are baseline Rust warnings in existing
code paths, including `client-rust`, `iamine-models`, `iamine-network`, and
`iamine-node`. They are not introduced by this documentation-only feature.

## Post-Merge Validation

Merge:

```text
origin/develop: 2a3bd6d34af10c0d0cd17e4d228ef89c12732b74
feature commit: 963890d1ffa0f0550c0ce441d6e1cc3e320951fa
tree: 145e581a7277307377d01cb50b629169bb0cc36e
```

Focused post-merge checks:

```text
git diff --check origin/develop~1..origin/develop: PASS
README stale/public reward claims scan: PASS; no matches
public beta wording scan: PASS; matches are only in explicit rejected-meaning context
pre-public/not-open wording scan: PASS
privacy boundary scan: PASS; matches are only in explicit prohibition context
```

Post-merge quality gate:

```text
./scripts/quality-gate.sh: PASS WITH WARNINGS
required_failures=0
warnings=1
skipped=3
```

Required post-merge checks:

```text
cargo fmt --all -- --check: PASS
cargo test -p iamine-models: PASS
cargo test -p iamine-network: PASS
cargo test -p iamine-node: PASS
cargo build -p iamine-node: PASS
cargo test --workspace: PASS
git diff --check: PASS
git diff --cached --check: PASS
```

Post-merge warning:

```text
cargo clippy --workspace --all-targets: WARN
```

The warning is classified as environmental. `/private/tmp` was at 100%
capacity with 116 MiB available during `clippy`, and `clippy` failed while
creating files under `target/debug/.fingerprint`. Required checks had already
passed on the published merge commit. Generated `target/` artifacts were
removed after validation to restore local disk capacity; source files and logs
were preserved.

Skipped optional tools:

```text
cargo audit: SKIPPED; not available
cargo deny check: SKIPPED; not available
gitleaks secret scan: SKIPPED; not available
```

## Expected Results

- README does not imply that public testnet is currently open;
- README does not promise public rewards or token receipts;
- public docs state pre-public status;
- public docs state no public beta launch;
- public docs do not publish bootnodes, release instructions, support intake, or
  public reward claims;
- public docs preserve IAMINE Agent Network Public Beta wording;
- privacy boundary is explicit;
- `NODE-UPGRADE-ROLLBACK-001` roadmap state is normalized to `CLOSED`;
- `PUBLIC-TESTNET-DOCUMENTATION-001` roadmap state is `CLOSED`.

## Field QA Decision

Field QA is not required for this documentation-only feature because no runtime,
installer, updater, P2P, worker, scheduler, inference, model, service-manager,
package generation, or release-publishing behavior changes.

Proxmox/R5500 may remain available for the next readiness gate feature, where
runtime or operational validation may be required.
