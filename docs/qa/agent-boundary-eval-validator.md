# AGENT-BOUNDARY-EVAL-VALIDATOR-001 QA

## Identity

```text
source branch: feature/agent-boundary-eval-validator-001
base: 20c070c75dadc80febc2494c85eadd03138f0c59
base tree: 3c2d0e04423c63c796af6163eb6fc58a1742fdf6
feature commits: 3a3db1724ce5a7ed20dc55b50741b8e2e4562a89, 6b31746b135366bc486464ee86b9296190a63328
merge commit: 329d1dafd4c28ea6a6914a0d31303befba79e864
tree: 0164e22629f5897743511005798ebef4098d220a
tracked clean before implementation: yes
staging clean before implementation: yes
untracked baseline before implementation: empty
expected runtime behavior change: none
field QA: not required
```

## Expected Scope

```text
iamine-agents/src/lib.rs
iamine-agents/src/boundary_eval/mod.rs
iamine-agents/src/boundary_eval/schema.rs
iamine-agents/src/boundary_eval/validation.rs
iamine-agents/src/boundary_eval/error.rs
iamine-agents/tests/boundary_eval.rs
iamine-agents/tests/fixtures/boundary_eval/valid/agent-boundary-tests.yaml
docs/architecture/agent-boundary-eval-validator.md
docs/qa/agent-boundary-eval-validator.md
```

No package resolver, package-load status, runtime crate, node wiring, worker,
scheduler, P2P, hardware profiler, model, inference, installer, service,
reward, wallet, marketplace, public beta, or mainnet behavior may change.

## Required Assertions

- The parser accepts caller-supplied YAML content only.
- The generated JSON Schema rejects unknown fields.
- Input is bounded to `64 KiB`.
- All nine required classes must be declared and covered by cases.
- Case identifiers and action declarations must be unique.
- Positive cases cannot expect refusal or handoff.
- Negative and unsafe cases cannot expect an allowed response.
- Actions and routes must remain coherent.
- Redaction, failure, human-review, and QA-review policies fail closed.
- Recognizable private-data shapes and unsafe references fail without echoing
  supplied values.
- Parsing a valid declaration does not prove eval execution or success.
- `BoundaryEvalValidatorUnavailable` remains in the always-blocked
  package-load report.
- `main.rs` and `cluster_registry.rs` remain unchanged.

## Required Validation

```bash
cargo fmt --all -- --check
cargo test -p iamine-agents --test boundary_eval
cargo test -p iamine-agents
cargo clippy -p iamine-agents --all-targets -- -D warnings
./scripts/quality-gate.sh
git diff --check
git diff --cached --check
wc -l iamine-agents/src/boundary_eval/*.rs
wc -l iamine-node/src/main.rs iamine-node/src/cluster_registry.rs
rg -n "BoundaryEvalValidatorUnavailable" iamine-agents/src/package_load.rs
```

## Observed Results

```text
cargo fmt --all -- --check: PASS
boundary eval tests: PASS, 13/13
iamine-agents regression: PASS, 109/109
iamine-agents clippy with -D warnings: PASS
feature pre-merge quality gate: PASS WITH WARNINGS, required_failures 0
feature pre-merge cargo test -p iamine-models: PASS, 158/158
cargo test -p iamine-network: PASS, 167/167
cargo test -p iamine-node: PASS, 480/480
cargo build -p iamine-node: PASS
cargo test --workspace: PASS
cargo clippy --workspace --all-targets: PASS with historical warnings
git diff --check: PASS
post-merge cargo test -p iamine-models: BASELINE EXCEPTION, 55/59
base cargo test -p iamine-models: identical result, 55/59
post-merge daemon socket test outside sandbox: PASS, 1/1
post-merge focused feature validation: PASS
quality gate result: PASS WITH ACCEPTED BASELINE EXCEPTIONS
largest new Rust owner module: validation.rs, 385 lines
main.rs: 4929 lines, delta 0
cluster_registry.rs: 862 lines, delta 0
package-load behavior: unchanged, still blocked
field QA: not required
```

The workspace compiler and Clippy reproduced existing `dead_code`, unused
import, deprecation, argument-count, and type-complexity warnings in
`client-rust`, `iamine-models`, `iamine-network`, and `iamine-node`. None is in
the feature diff. The focused `iamine-agents` Clippy run with warnings denied
passed, so these warnings are classified as historical baseline and not as a
new regression.

A post-merge quality-gate run reproduced four TinyLlama/Metal integration
failures in `test_concurrency_limit`, `test_inference_queue`,
`test_real_inference`, and `test_token_streaming`. The exact base commit,
executed with the same model and target environment, produced the same `55/59`
result and the same four failures. The feature does not modify
`iamine-models`, `iamine-node`, model loading, or inference, so these failures
are accepted as a baseline environment/model-output exception.

The same gate encountered an `EPERM` socket-creation failure in
`daemon_runtime::tests::test_daemon_start_stop` under the restricted sandbox.
The exact test passed `1/1` outside that sandbox on the merged tree. This is
classified as a harness environment exception, not a product regression.

A first pre-push integration gate exposed a harness visibility gap: while the
test file was untracked, the new-marker guard did not include it in its Git
diff. Once committed, the guard correctly reported test-only `expect` and
`panic` markers. The test suite was refactored to propagate typed test errors,
then passed `13/13`, focused Clippy with warnings denied, and a direct marker
scan with no matches. The first local integration merge was not pushed and was
superseded by a fresh merge from `origin/develop`.

Optional tools:

```text
cargo audit: SKIPPED, unavailable
cargo deny: SKIPPED, unavailable
gitleaks: SKIPPED, unavailable
```

## Test Coverage

The focused suite verifies:

- valid typed parsing and generated schema availability;
- structural rejection of unknown fields;
- required class declaration and case coverage;
- positive and unsafe class/action contradictions;
- action/route contradictions;
- complete action vocabulary and unique case IDs;
- required redaction and independent review;
- private path, IP, email, MAC, and assigned-secret rejection;
- redacted unsafe-reference failures;
- unsupported schema and oversized input failures;
- preservation of the static package-load blocker.

The fixture is synthetic and contains no real host, user, network, credential,
secret, or personal-path values.

## Field QA Decision

Field QA is not required. The parser receives in-memory content and performs no
package I/O, eval execution, model call, process startup, hardware inspection,
runtime mutation, network operation, or persistence. Mac, TS140, and
Proxmox/R5500 field QA becomes mandatory when a later feature resolves package
paths, executes evals, integrates trusted review evidence, or starts runtime
behavior.

## Post-Merge Result

```text
MERGED / VALIDATED / CLOSED
develop merge: 329d1dafd4c28ea6a6914a0d31303befba79e864
tree: 0164e22629f5897743511005798ebef4098d220a
next feature: AGENT-RUNTIME-CORE-001 (PROPOSED)
```
