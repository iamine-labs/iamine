# Agent Manifest Parser Validator QA

Feature:

```text
AGENT-MANIFEST-PARSER-VALIDATOR-001
```

## Objective

Validate that the new `iamine-agents` crate parses and validates only the root
`agent.yaml` contract, fails closed, bounds resource use, preserves privacy,
and introduces no agent runtime or node side effects.

## Expected Scope

```text
Cargo.toml
Cargo.lock
iamine-agents/
docs/agents/agent-package-manifest.md
docs/agents/agent-manifest-schema-source-of-truth.md
docs/agents/agent-skeleton-standard.md
docs/agents/*-agent-skeleton.md
docs/agents/agent-audit-log.md
docs/agents/agent-capability-metadata.md
docs/agents/agent-dependency-policy.md
docs/agents/agent-expertise-metadata.md
docs/agents/agent-permission-model.md
docs/agents/agent-registry-local.md
docs/agents/agent-resource-requirements.md
docs/agents/agent-scope-boundary-evals.md
docs/architecture/agent-manifest-parser-validator.md
docs/architecture/agent-manifest-schema-source-of-truth.md
docs/architecture/agent-package-manifest.md
docs/architecture/agent-skeleton-standard.md
docs/architecture/node-doctor-agent-dependency-reconciliation.md
docs/qa/agent-manifest-parser-validator.md
docs/qa/agent-package-manifest.md
docs/qa/agent-skeleton-standard.md
docs/roadmap/iamine-agent-network-roadmap.md
docs/roadmap/iamine-product-roadmap.md
```

Expected runtime behavior change:

```text
none
```

## Required Local Validation

```bash
cargo fmt --all -- --check
cargo test -p iamine-agents
cargo clippy -p iamine-agents --all-targets
cargo run -p iamine-agents --example print_manifest_schema
cargo test --workspace
cargo clippy --workspace --all-targets
./scripts/quality-gate.sh
git diff --check
git diff --cached --check
wc -l iamine-node/src/main.rs
wc -l iamine-node/src/cluster_registry.rs
rg --files -g '*.rs' -g '!target/**' | xargs wc -l | sort -nr | head -20
```

## Required Behavior

- valid canonical YAML parses successfully;
- generated JSON Schema comes from the canonical Rust types;
- unknown top-level and nested fields fail;
- required fields and enum values fail closed;
- inputs above 64 KiB fail before parsing;
- execution authorization remains false;
- remote execution modes remain unavailable;
- public distribution and unsafe security claims fail;
- missing review gates fail;
- absolute, platform-absolute, and traversal references fail;
- reference contents and formats remain outside root-parser ownership;
- duplicate personas and references fail;
- invalid IDs and semantic versions fail;
- syntax, schema, and semantic errors do not echo private values;
- no filesystem, package loading, runtime, worker, scheduler, network, model,
  inference, hardware, or CLI side effect exists.

## Field QA Decision

Field QA is not required for this feature. The crate exposes an in-memory
parser and schema API only; it is not wired into runtime, CLI, package loading,
capability/status reporting, scheduler, workers, networking, or inference.

Mac local validation and the full workspace gate are required. TS140 and
Proxmox become relevant when a later feature wires package loading or runtime
behavior.

## Observed Local Results

Execution date:

```text
2026-07-17
```

Identity and scope:

```text
branch=feature/agent-manifest-parser-validator-001
base=825df894e59fe6413e79544564a392ee35be8bee
runtime_behavior_changed=false
field_qa_required=false
```

Results:

- `cargo test -p iamine-agents`: PASS, 22 passed, 0 failed;
- `cargo clippy -p iamine-agents --all-targets`: PASS, no feature warnings;
- generated JSON Schema example: PASS and emitted parseable draft-07 JSON;
- `cargo fmt --all -- --check`: PASS;
- `cargo test --workspace`: PASS, 885 passed, 0 failed;
- `cargo clippy --workspace --all-targets`: PASS;
- `./scripts/quality-gate.sh`: PASS WITH WARNINGS, with
  `required_failures=0`, `warnings=0`, and `skipped=3`;
- `git diff --check` and `git diff --cached --check`: PASS;
- repository size and sensitive-file guards: PASS;
- `main.rs`: 4929 lines, delta 0;
- `cluster_registry.rs`: 862 lines, delta 0.

The workspace emitted existing dead-code, unused-import, deprecation,
`too_many_arguments`, and `type_complexity` warnings from untouched crates.
The feature crate emitted none. Optional `cargo audit`, `cargo deny`, and
`gitleaks` checks were reported as skipped because the tools are unavailable.

Known dependency note: the canonical dependency policy currently selects
`serde_yaml` 0.9, whose package release is marked deprecated upstream. Replacing
it requires a separate parser-policy decision and is not hidden by this QA
result.

## Recommendation Boundary

Passing QA can recommend this parser feature for Architecture merge review. It
must not authorize package loading, agent execution, or functional Node Doctor
development.

## Post-Merge Validation and Closure

Merge identity:

```text
source branch: feature/agent-manifest-parser-validator-001
source commit: 67fbf35ef1784f99235c71c393d92bc741f5ae7c
target branch: develop
merge commit: c849d98c6861d0f8a9821608e84a779b9c857d3f
tree: d3aa677cf8733b3cf6bf22289857aacc55938929
origin/develop: c849d98c6861d0f8a9821608e84a779b9c857d3f
```

The merge tree exactly matches the locally validated feature tree. The
post-merge quality gate stopped in the existing `iamine-models` real-inference
integration suite, as required by the first-failure policy. Four tests returned
`success=false` after loading the installed TinyLlama model through Metal:

```text
test_concurrency_limit
test_inference_queue
test_real_inference
test_token_streaming
```

`test_real_inference` reproduced with the same result on the exact pre-feature
base `825df894e59fe6413e79544564a392ee35be8bee`. The feature does not modify
`iamine-models`, inference code, model storage, Metal selection, or runtime
wiring. The failure family is therefore classified as a baseline/environment
condition and is accepted only for this parser closeout; the quality gate itself
is not reported as PASS for the post-merge run.

Focused post-merge evidence:

```text
cargo test -p iamine-agents: PASS, 22 passed, 0 failed
cargo clippy -p iamine-agents --all-targets: PASS, no warnings
cargo fmt --all -- --check: PASS
git diff --check: PASS
main.rs delta: 0
cluster_registry.rs delta: 0
field QA: not required
```

Pre-merge full-workspace evidence remains valid for the identical tree: 885
tests passed, workspace clippy passed, and the quality gate completed with zero
required failures before merge. Optional `cargo audit`, `cargo deny`, and
`gitleaks` were skipped because they were unavailable.

Closure:

```text
MERGED / VALIDATED WITH ACCEPTED BASELINE EXCEPTION / CLOSED
```

The next package lifecycle feature remains:

```text
AGENT-PACKAGE-LOAD-GATE-001
```
