# Agent Audit Events QA

Feature:

```text
AGENT-AUDIT-EVENTS-001
```

## Objective

Validate that lifecycle, Scope, Permission, refusal, and handoff projections
are deterministic, fixed-size, privacy-safe, and incapable of authorizing
package loading or runtime execution.

## Expected Scope

```text
iamine-agents/src/audit_events/
iamine-agents/src/lib.rs
iamine-agents/tests/audit_events.rs
iamine-agents/README.md
docs/architecture/agent-audit-events.md
docs/qa/agent-audit-events.md
docs/roadmap/iamine-agent-network-roadmap.md
```

Expected runtime integration change:

```text
none
```

## Required Behavior

- event schema, classes, sources, outcomes, reasons, and lifecycle states expose
  stable codes;
- every projection returns one or two events only;
- Scope and Permission refusals emit the check before refusal evidence;
- Scope and Permission handoffs emit the check before handoff evidence;
- clarification and confirmation remain distinct non-allowing outcomes;
- lifecycle observation does not validate transitions or execute behavior;
- debug output contains no package, task, operation, input, prompt, output,
  path, host, network, credential, wallet, process, or model values;
- events cannot remove package-load blockers or authorize execution;
- no filesystem, persistence, logging, process, network, runtime, worker,
  scheduler, model, inference, hardware, service, or CLI side effect occurs.

## Local Validation

```bash
cargo fmt --all -- --check
cargo test -p iamine-agents --test audit_events
cargo test -p iamine-agents
cargo clippy -p iamine-agents --all-targets -- -D warnings
cargo test --workspace
cargo clippy --workspace --all-targets
./scripts/quality-gate.sh
git diff --check
git diff --cached --check
wc -l iamine-agents/src/audit_events/*.rs
wc -l iamine-node/src/main.rs
wc -l iamine-node/src/cluster_registry.rs
rg -n "std::fs|std::process|std::net|File::|read_to_string|Command::|SystemTime|Instant|serde" \
  iamine-agents/src/audit_events
```

The final `rg` command must return no matches.

## Field QA

Run the exact implementation commit on Mac, Dell TS140, and Proxmox guests
`iamine-ctrl`, `iamine-wrk1`, `iamine-wrk2`, and `iamine-heavy`:

```bash
cargo test -p iamine-agents --test audit_events
cargo clippy -p iamine-agents --all-targets -- -D warnings
```

Field QA must use clean disposable copies and preserve historical staged and
untracked artifacts. It must not start an agent runtime, package loader,
worker, P2P, model, inference, or service.

## Evidence Status

Baseline:

```text
base: 247cbd08ea329c8f031ab9a898f1ca37f1468ad8
tree: 8e48e8eab0c3daf73e41acfcc2506697b7ef97c8
cargo test -p iamine-agents: PASS, 63 tests
main.rs: 4929 lines
cluster_registry.rs: 862 lines
```

Local implementation evidence:

```text
cargo test -p iamine-agents --test audit_events: PASS, 10/10
cargo test -p iamine-agents: PASS, 73/73
cargo clippy -p iamine-agents --all-targets -- -D warnings: PASS
cargo fmt --all -- --check: PASS
side-effect API scan: PASS, no matches
secret and tracked-artifact scans: PASS
git diff --check: PASS
scripts/quality-gate.sh: PASS WITH WARNINGS, required_failures=0
```

Quality gate detail:

- `iamine-models`: PASS, 158/158 including real Metal inference;
- `iamine-network`: PASS, 167/167;
- `iamine-node`: PASS, 480/480 and build PASS;
- workspace: PASS, 936/936;
- workspace Clippy: PASS with historical warnings in unchanged crates;
- repository and architecture guards: PASS;
- `main.rs`: 4,929 lines, delta 0;
- `cluster_registry.rs`: 862 lines, delta 0;
- largest new production file: `event.rs`, 187 lines;
- `cargo audit`, `cargo deny`, and `gitleaks`: skipped, unavailable.

Exact implementation identity:

```text
branch: feature/agent-audit-events-001
commit: df80dad96a8eb540a4dce67fc2a6402bef2977b1
tree: 048d56d7081b0a8aa2fcf4fe47e56c1012c211fe
merge base: 247cbd08ea329c8f031ab9a898f1ca37f1468ad8
tracked clean: yes
staging clean: yes
untracked feature artifacts: none
```

Field QA evidence:

```text
Mac: PASS, 10/10 and Clippy -D warnings
TS140: PASS, 10/10 and Clippy -D warnings
iamine-ctrl: PASS, 10/10 and Clippy -D warnings
iamine-wrk1: PASS, 10/10 and Clippy -D warnings
iamine-wrk2: PASS, 10/10 and Clippy -D warnings
iamine-heavy: PASS, 10/10 and Clippy -D warnings
field total: PASS, 60/60 focused test executions
```

All remote runs retained the exact implementation commit and tree and finished
with clean disposable QA copies. The TS140 canonical working copy and all four
historical Proxmox copies retained their preflight branch, tree, staging state,
untracked inventory, and artifact hashes. No runtime, package loader, worker,
P2P, model, inference, or service was started.

Final Architecture review, merge, and post-merge evidence remain pending.

## Recommendation Boundary

QA may recommend:

```text
READY FOR ARCHITECTURE MERGE REVIEW
```

QA must not emit execution authorization or treat an audit event as proof of a
runtime side effect.
