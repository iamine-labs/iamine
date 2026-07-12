# USER-DIAGNOSTICS-SUPPORT-001 QA

## Scope

Validate the privacy-safe support bundle command:

```text
iamine-node support bundle [--output PATH] [--json]
```

The feature is local diagnostic support only. It must not start workers, P2P,
PubSub, model downloads, model loading, inference, dynamic hardware probes,
installers, updaters, or rollback flows.

## Required Local Validation

```text
cargo fmt --all -- --check
cargo test -p iamine-node user_diagnostics_support
cargo test -p iamine-node cli_detects_support_bundle_json_output
cargo test -p iamine-node cli_valid_commands_do_not_show_unknown_mode
cargo test -p iamine-node cli_preserves_existing_help_text
cargo build -p iamine-node
git diff --check
git diff --cached --check
```

After build:

```text
./target/debug/iamine-node support bundle --json
./target/debug/iamine-node support bundle --output /tmp/iamine-support/support.json --json
```

Validate:

- JSON parses;
- `schema_version` is `1.0.0`;
- `feature` is `USER-DIAGNOSTICS-SUPPORT-001`;
- privacy flags for usernames, home directories, full hostnames, MAC addresses,
  IP addresses, serial numbers, disk UUIDs, machine IDs, user process lists,
  personal paths, raw logs, and secrets are all false;
- output metadata contains only the output file label;
- bundle file permissions are private on Unix;
- action items include next commands for non-pass diagnostics;
- runtime side-effect flags are all false.

## Local Results

Status:

```text
LOCAL VALIDATION PASSED
```

Executed on Mac local worktree:

```text
cargo fmt --all -- --check: PASS
cargo test -p iamine-node user_diagnostics_support: PASS; 8 passed
cargo test -p iamine-node cli_detects_support_bundle_json_output: PASS
cargo test -p iamine-node cli_: PASS; 47 passed
cargo test -p iamine-node: PASS; 480 passed
cargo build -p iamine-node: PASS
cargo clippy -p iamine-node --all-targets: PASS with baseline warnings only
./scripts/quality-gate.sh: PASS WITH WARNINGS
git diff --check: PASS
git diff --cached --check: PASS
support bundle JSON smoke: PASS
support bundle output file smoke: PASS; Unix permissions 0600
privacy and runtime side-effect checks: PASS
```

Observed non-blockers:

```text
One full iamine-node test run initially reported
cluster_stress_batch::tests::batch_file_rejects_empty_requests as failed.
The test passed in isolation and passed in the full rerun. It was classified
as non-reproducible and not a feature regression.

Optional quality-gate tools skipped because they were unavailable locally:
cargo audit, cargo deny, gitleaks.
```

Size guard:

```text
iamine-node/src/user_diagnostics_support.rs: 521 lines
iamine-node/src/main.rs: 4929 lines; +1 module registration only
iamine-node/src/cluster_registry.rs: 862 lines; unchanged
```

## Merge and Post-Merge Validation

Controlled merge:

```text
Source branch: feature/user-diagnostics-support-001
Feature commit: ab5b0a8206c6e0b1668e6e79bdaa062d660a79d5
Target branch: develop
Merge commit: 80709636a36e518786207dbbdd887ebd68cd3368
Merge tree: 4a7b4a5544eb2afeccbb05edd726f60711cdaba3
```

Post-merge validation on the merge commit:

```text
./scripts/quality-gate.sh: PASS WITH WARNINGS
required_failures=0
warnings=0
skipped=3
origin/develop: 80709636a36e518786207dbbdd887ebd68cd3368
origin/develop tree: 4a7b4a5544eb2afeccbb05edd726f60711cdaba3
origin/develop..origin/main: 0
```

## Field QA Decision

Field QA is not required for this implementation unless Architecture expands the
scope to runtime, P2P, worker behavior, scheduler behavior, inference behavior,
hardware profiling, installer behavior, update behavior, or remote support
upload behavior.

If field QA becomes required, execute the canonical matrix:

- Mac development machine;
- TS140;
- Proxmox/R5500 guests.

## QA Recommendation

QA may recommend:

```text
READY FOR ARCHITECTURE MERGE REVIEW
```

QA must not emit:

```text
MERGE APPROVED
MERGE AUTHORIZED
```
