# IAMINE Agent Handoff Policy

Feature:

```text
AGENT-HANDOFF-POLICY-001
```

## Purpose

Define future handoff behavior for IAMINE agents without implementing
orchestrator routing, human approval flows, workers, queues, persistence,
network transfer, sandbox behavior, package installation, model loading, or
inference behavior.

This document does not authorize executable agents, runtime execution,
filesystem mutation, network access, shell execution, registry publication,
marketplace publication, public beta, wallet, reward, settlement, mainnet
behavior, or distributed model MoE.

## Policy Question

Handoff policy answers:

```text
When must a future agent stop and transfer responsibility?
```

It does not answer whether orchestrator routing, approval UI, persistence,
audit logs, execution queues, or transports exist.

## Draft Schema

```text
iamine.agent.handoff_policy.draft-0.1
```

This feature does not implement parsers, runtime enforcement, human approval,
orchestrator routing, storage, event emission, retries, cancellation, timeout
handling, or cleanup.

## Handoff Targets

Future handoff must name a target class:

```text
operator
orchestrator
specialized_agent
architecture_review
security_review
qa_review
blocked_state
```

Unknown, missing, contradictory, broad, unsafe, stale, or unverifiable handoff
targets must block execution by default.

## Handoff Reasons

Future handoff must classify the reason:

```text
out_of_scope
permission_missing
risk_too_high
input_ambiguous
output_requires_review
sandbox_unavailable
timeout_or_cancelled
policy_conflict
```

Handoff must not be reported as task completion unless the delegated target
explicitly completes its own task under its own scope and evidence.

## Required Evidence

Future handoff records must include:

```text
schema_version
agent_id
task_type
scope_id
handoff_target
handoff_reason
operator_visible_summary
blocked_action
```

The summary must be safe to display to the operator by default.

## Privacy Rules

Handoff metadata must not include credentials, private keys, wallet keys,
usernames, full hostnames, IP addresses, MAC addresses, serial numbers, disk
UUIDs, machine IDs, private paths, raw user prompts, raw outputs, raw process
lists, unredacted logs, or permanent hardware fingerprints.

## Boundary Rules

- Handoff policy cannot authorize runtime execution.
- Handoff policy cannot silently continue to `running`.
- Handoff policy cannot grant permissions or broaden scope.
- Handoff policy cannot select an agent without routing policy.
- Handoff policy cannot expose private data as evidence.
- Human or architecture review must be explicit when risk exceeds the
  declared agent scope.

## Next Roadmap Step

```text
AGENT-OUT-OF-SCOPE-RESPONSE-001
```
