# IAMINE Agent Routing Candidate Selection

Feature:

```text
AGENT-ROUTING-CANDIDATE-SELECTION-001
```

## Purpose

Define future candidate-selection inputs for IAMINE agents without
implementing scheduler behavior, routing runtime, worker startup, queues,
persistence, network transfer, package installation, model loading, inference,
marketplace behavior, public beta, or distributed model MoE.

This document does not authorize executable agents, runtime execution,
filesystem mutation, network access, shell execution, registry publication,
marketplace publication, public beta, wallet, reward, settlement, mainnet
behavior, or distributed model MoE.

## Policy Question

Candidate-selection policy answers:

```text
Which declared metadata may be used to choose future candidate agents?
```

It does not answer whether routing, scheduling, execution, persistence, audit
logs, approval UI, or transports exist.

## Draft Schema

```text
iamine.agent.routing_candidate_selection.draft-0.1
```

This feature does not implement parsers, routing, scheduling, scoring,
persistence, event emission, model selection, task execution, retries,
cancellation, timeout handling, or cleanup.

## Candidate Inputs

Future candidate selection may consider only declared metadata:

```text
agent_id
task_type
declared_scope
permission_requirements
resource_requirements
risk_class
execution_mode
node_compatibility
availability_state
handoff_policy
out_of_scope_policy
```

Candidate selection must not inspect private prompts, raw outputs, host
fingerprints, secrets, wallets, process lists, personal paths, or private
files.

## Selection Outcomes

Future selection must produce one outcome:

```text
candidate_selected
multiple_candidates
no_candidate
handoff_required
blocked
```

Selection outcomes do not imply execution success, scheduler priority,
reputation, rewards, marketplace ranking, settlement, or model eligibility.

## Hard Exclusions

Candidates must be excluded when:

```text
scope_mismatch
permission_mismatch
resource_mismatch
risk_too_high
node_incompatible
sandbox_unavailable
policy_conflict
metadata_unknown
```

Unknown, missing, contradictory, broad, unsafe, stale, or unverifiable metadata
must block candidate selection by default.

## Privacy Rules

Routing candidate metadata must not include credentials, private keys, wallet
keys, usernames, full hostnames, IP addresses, MAC addresses, serial numbers,
disk UUIDs, machine IDs, private paths, raw user prompts, raw outputs, raw
process lists, unredacted logs, or permanent hardware fingerprints.

## Boundary Rules

- Candidate selection cannot authorize runtime execution.
- Candidate selection cannot implement scheduler policy.
- Candidate selection cannot perform model selection or distributed model MoE.
- Candidate selection cannot grant permissions or broaden scope.
- Candidate selection cannot replace handoff, out-of-scope, lifecycle, sandbox,
  audit, or input/output gates.

## Next Roadmap Step

```text
AGENT-SKELETON-GENERATOR-001
```
