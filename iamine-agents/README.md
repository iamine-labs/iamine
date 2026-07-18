# iamine-agents

`iamine-agents` owns IAMINE agent package contracts and validation code.

The current surface is intentionally limited to the root `agent.yaml`
manifest:

```text
Rust types
-> generated JSON Schema
-> bounded YAML parsing
-> semantic deny-by-default validation
```

The crate does not read files, follow references, load packages, start a
runtime, grant permissions, emit audit events, start a sandbox, or execute an
agent.

Focused validation:

```bash
cargo test -p iamine-agents
cargo clippy -p iamine-agents --all-targets
cargo run -p iamine-agents --example print_manifest_schema
```
