# IAMINE Agent Out-of-Scope Response Policy

Feature:

```text
AGENT-OUT-OF-SCOPE-RESPONSE-001
```

## Purpose

Define safe response behavior when a future IAMINE agent receives work outside
its declared scope, permissions, resources, risk envelope, or execution mode.

This feature does not implement runtime execution, routing, workers, queues,
persistence, sandbox behavior, package installation, network transfer, model
loading, inference, registry publication, marketplace publication, public beta,
wallet, reward, settlement, mainnet behavior, or distributed model MoE.

## Policy Question

Out-of-scope policy answers:

```text
How must a future agent respond when it must not execute?
```

It does not answer whether execution, routing, persistence, audit logs,
approval UI, or transports exist.

## Draft Schema

```text
iamine.agent.out_of_scope_response.draft-0.1
```

This feature does not implement parsers, runtime enforcement, refusal
generation, routing, storage, event emission, retries, cancellation, timeout
handling, or cleanup.

## Response Classes

Future out-of-scope responses must use one class:

```text
refuse
clarify
handoff
blocked
```

The response must never silently continue to `running`.

## Reason Classes

Future out-of-scope responses must classify the reason:

```text
scope_mismatch
permission_missing
input_unsafe
input_ambiguous
risk_too_high
resource_unavailable
sandbox_unavailable
policy_conflict
```

Unknown, missing, contradictory, broad, unsafe, stale, or unverifiable reasons
must block execution by default.

## Operator-Visible Shape

Future responses must be concise and safe:

```text
schema_version
agent_id
task_type
scope_id
response_class
reason_class
operator_visible_summary
handoff_target
blocked_action
```

Summaries must not include private data, raw prompts, raw outputs, host
identifiers, secrets, or personal paths.

## Privacy Rules

Out-of-scope metadata must not include credentials, private keys, wallet keys,
usernames, full hostnames, IP addresses, MAC addresses, serial numbers, disk
UUIDs, machine IDs, private paths, raw user prompts, raw outputs, raw process
lists, unredacted logs, or permanent hardware fingerprints.

## Boundary Rules

- Refusal cannot be treated as task success.
- Clarification cannot grant permissions or broaden scope.
- Handoff cannot bypass the handoff policy.
- Blocked responses cannot start workers, load models, install packages, or
  access files.
- Out-of-scope response policy cannot authorize runtime execution.

## Next Roadmap Step

```text
AGENT-ROUTING-CANDIDATE-SELECTION-001
```
