# NODE-DOCTOR-AGENT-001-DEPENDENCY-RECONCILIATION-001

## Decision

Accept the Architecture checkpoint and keep functional
`NODE-DOCTOR-AGENT-001` blocked before implementation.

The closed `NODE-DOCTOR-AGENT-001-SKELETON` is a planning contract. It is
non-executable, not user available, and cannot be presented as IAMINE's first
functional agent.

## Evidence Boundary

The repository currently defines architecture contracts for package shape,
runtime states, sandbox policy, lifecycle, scope, permissions, audit, handoff,
and out-of-scope behavior. Those contracts explicitly do not implement an
agent package loader, runtime execution, permission enforcement, scope
enforcement, audit emission, sandbox startup, or an execution lifecycle.

Contract closure is not executable implementation evidence. Functional Node
Doctor development cannot use a static package, add ad hoc agent behavior to
`iamine-node`, or wrap `iamine-node lan doctor` to bypass those gates.

## Required Chain

Functional Node Doctor development requires implementation and validation
evidence for:

```text
AGENT-MANIFEST-PARSER-VALIDATOR-001
AGENT-PACKAGE-LOAD-GATE-001
AGENT-RUNTIME-BASELINE-001
AGENT-EXECUTION-LIFECYCLE-001
AGENT-SCOPE-ENFORCEMENT-001
AGENT-PERMISSION-ENFORCEMENT-001
AGENT-AUDIT-EVENTS-001
AGENT-RUNTIME-SANDBOX-001
AGENT-OUT-OF-SCOPE-RESPONSE-001
AGENT-HANDOFF-POLICY-001
NODE-DOCTOR-EVIDENCE-PROVIDER-001
```

Existing `CLOSED` labels for architecture-only contracts remain historically
valid. They do not satisfy this executable gate by themselves.

## Evidence Provider Boundary

`NODE-DOCTOR-EVIDENCE-PROVIDER-001` is an owner-module data interface, not an
agent. It may expose bounded, structured, redacted, read-only evidence for:

```text
node status
hardware profile
configuration status
model readiness
peer and network status
remote-inference readiness
```

It must not execute arbitrary commands, invoke a shell, return raw logs,
collect private host identifiers, modify node state, or produce direct
user-facing agent responses.

The provider may later reuse owner-module data that also feeds
`iamine-node lan doctor`, but the agent package must never invoke or wrap that
CLI command.

## Manifest Format Risk

The newer schema source-of-truth contract specifies YAML authoring, Rust types
as the source of truth, generated JSON Schema for validation, and JSON runtime
payloads. Earlier package and skeleton planning contracts named TOML files.

`AGENT-MANIFEST-PARSER-VALIDATOR-001` resolves the root surface as YAML-only
`agent.yaml` and does not create a competing TOML parser. Referenced child
metadata remains separately owned and is not opened by the root parser.

## Integration Boundary

This reconciliation is documentation-only. It does not modify Rust source,
Cargo manifests, runtime startup, CLI behavior, workers, schedulers, P2P,
PubSub, model policy, inference, hardware profiling, persistence, installer,
updater, registry storage, marketplace behavior, rewards, wallet, settlement,
mainnet, or distributed model MoE behavior.

## Next Feature

```text
AGENT-PACKAGE-LOAD-GATE-001
```

`AGENT-MANIFEST-PARSER-VALIDATOR-001` adds the root Rust types, YAML parser,
generated JSON Schema, validators, fixtures, and tests without package loading
or execution. The load gate must consume that parser and remain fail-closed
until every referenced metadata and enforcement prerequisite is available.
