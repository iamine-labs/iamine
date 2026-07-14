# IAMINE Agent Execution Lifecycle

Feature:

```text
AGENT-EXECUTION-LIFECYCLE-001
```

## Purpose

Define valid lifecycle transitions for future IAMINE agent execution without
implementing a runtime state machine, task queue, worker side effects, sandbox
startup, model loading, scheduler integration, or inference behavior.

This document does not authorize executable agents, runtime execution, package
installation, dependency installation, filesystem mutation, network access,
shell execution, registry publication, marketplace publication, public beta,
wallet, reward, settlement, mainnet behavior, or distributed model MoE.

## Lifecycle Contract

Lifecycle policy answers:

```text
Which state transitions are allowed for future agent execution records?
```

It does not answer whether execution records or transitions are implemented.

## Draft Schema

```text
iamine.agent.execution_lifecycle.draft-0.1
```

This feature does not implement parsers, persistence, event emission, workers,
queues, state transitions, retries, cancellation, timeout handling, or cleanup.

## Allowed Transition Shape

Future lifecycle transitions must be explicit:

```text
queued -> permission_pending
queued -> blocked
permission_pending -> scope_check
permission_pending -> blocked
scope_check -> handoff_required
scope_check -> running
scope_check -> blocked
handoff_required -> cancelled
running -> completed
running -> failed
running -> timeout
running -> cancelled
timeout -> failed
cancelled -> failed
```

`running` is not available until sandbox, permission, input/output, timeout,
and lifecycle implementation gates exist.

## Terminal States

```text
completed
failed
cancelled
timeout
blocked
```

Future implementations must make terminal-state behavior explicit. This
feature does not define retries.

## Blocked Claims

Lifecycle metadata must not claim runtime execution, sandbox availability,
package installation, dependency installation, permission enforcement,
scheduler priority, worker startup, model admission, public registry
availability, marketplace publication, trust, reputation, rewards, wallet,
settlement, mainnet behavior, or distributed model MoE.

## Privacy Rules

Lifecycle metadata must not include credentials, private keys, wallet keys,
usernames, full hostnames, IP addresses, MAC addresses, serial numbers, disk
UUIDs, machine IDs, private paths, raw user prompts, raw outputs, raw process
lists, unredacted logs, or permanent hardware fingerprints.

## Review Requirements

Human review must confirm:

- every transition is explicit;
- `blocked` is reachable from unsafe or missing gates;
- `handoff_required` cannot silently continue to `running`;
- terminal states are explicit;
- retry behavior remains undefined until a later feature;
- lifecycle policy cannot bypass sandbox, scope, permission, audit, boundary
  eval, local registry, timeout, cancellation, or handoff gates.

## Next Roadmap Step

```text
AGENT-INPUT-OUTPUT-CONTRACT-001
```
