# iamine-agents

`iamine-agents` owns IAMINE agent package contracts and validation code.

The current surface validates the root `agent.yaml` manifest, assesses it
against the package-load gate, and evaluates already typed scope policies:

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
```

The package-load report has no positive status while downstream validators and
enforcement gates are unavailable. A scope `Allow` decision is not execution
authorization. The crate does not read files, follow references, load packages,
start a runtime, grant permissions, emit audit events, start a sandbox, or
execute an agent.

Focused validation:

```bash
cargo test -p iamine-agents
cargo clippy -p iamine-agents --all-targets
cargo run -p iamine-agents --example print_manifest_schema
```
