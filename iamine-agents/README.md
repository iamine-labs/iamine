# iamine-agents

`iamine-agents` owns IAMINE agent package contracts and validation code.

The current surface validates the root `agent.yaml` manifest, assesses it
against the package-load gate, evaluates already typed scope and permission
policies, and projects typed decisions into bounded in-memory audit evidence:

```text
Rust types
-> generated JSON Schema
-> bounded YAML parsing
-> semantic deny-by-default validation
-> typed BLOCKED package-load report

typed scope declaration
-> deny-by-default policy validation
-> exact request boundary checks
-> typed allow, clarify, refuse, or orchestrator handoff

trusted reviewed permission policy
+ scope Allow evidence
-> deny-by-default action and category checks
-> typed allow, confirmation request, refusal, or orchestrator handoff

typed lifecycle state, Scope evaluation, or Permission evaluation
-> fixed-size redacted event projection
-> lifecycle, check, refusal, or handoff evidence
```

The package-load report has no positive status while downstream validators and
enforcement gates are unavailable. Scope and permission `Allow` decisions are
not execution authorization. The crate does not read files, follow references,
load packages, start a runtime, grant operating-system permissions, emit audit
logs, persist or publish audit events, start a sandbox, or execute an agent.

Focused validation:

```bash
cargo test -p iamine-agents
cargo clippy -p iamine-agents --all-targets
cargo run -p iamine-agents --example print_manifest_schema
```
