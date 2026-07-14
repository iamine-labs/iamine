# IAMINE Agent Runtime Baseline

Feature:

```text
AGENT-RUNTIME-BASELINE-001
```

## Purpose

Define the minimum IAMINE agent runtime contract and execution states before
any real agent execution, sandbox startup, package installation, scheduler
integration, worker startup, model loading, reputation, reward, or distributed
model MoE behavior exists.

This document is an architecture artifact. It does not authorize executable
agents, runtime execution, package installation, package manager execution,
dependency installation, sandboxing, registry publication, marketplace
publication, third-party agents, public beta launch, or public agent discovery.

## Runtime Contract

The runtime baseline answers one narrow review question:

```text
What states and gates must exist before an agent execution can be considered?
```

It does not answer:

- whether execution is implemented;
- whether sandboxing exists;
- whether permissions are enforced;
- whether the scheduler can route tasks to agents;
- whether a worker should start;
- whether a model backend is available;
- whether an agent is trusted, reputable, certified, or rewarded;
- whether a package may be published publicly.

## Draft Schema

The first draft runtime baseline identifier is:

```text
iamine.agent.runtime_baseline.draft-0.1
```

This is a runtime planning contract. It is not a package skeleton file and is
not executable in this phase.

## Minimum Execution States

The minimum state vocabulary is:

```text
queued
permission_pending
scope_check
handoff_required
running
completed
failed
cancelled
timeout
blocked
```

These are state labels only. This feature does not implement state machines,
runtime transitions, persistence, workers, task queues, or process execution.

## Required Gates

Future runtime eligibility must require:

- valid package manifest;
- valid scope manifest;
- valid capability metadata;
- valid expertise metadata;
- valid resource requirements;
- deny-by-default permissions;
- privacy-safe audit policy;
- boundary evals;
- local registry review;
- language policy;
- dependency policy;
- runtime language matrix;
- schema source-of-truth alignment;
- sandbox policy before code execution;
- human review and QA evidence.

## Blocked Runtime Claims

Runtime baseline metadata must not claim:

- runtime execution authorization;
- sandbox availability;
- package installation authorization;
- dependency installation authorization;
- package manager execution authorization;
- permission enforcement;
- scheduler priority;
- node compatibility;
- backend availability;
- model admission;
- worker startup authorization;
- public registry availability;
- public marketplace publication;
- third-party publication;
- public beta launch;
- trust, reputation, certification, or reward eligibility;
- wallet, settlement, token, or mainnet behavior;
- distributed model MoE.

## Privacy Rules

Runtime baseline metadata must not include credentials, private keys, wallet
keys, usernames, full hostnames, IP addresses, MAC addresses, serial numbers,
disk UUIDs, machine IDs, private paths, raw user prompts, raw outputs, raw
process lists, unredacted logs, or permanent hardware fingerprints.

## Review Requirements

Human review must confirm:

- all required state labels are present;
- no runtime transition is implemented in this feature;
- sandbox remains a separate gate;
- handoff remains explicit;
- blocked and timeout states are terminal unless a later lifecycle feature
  defines safe retry behavior;
- runtime baseline cannot bypass scope, permissions, audit, boundary evals,
  local registry review, language, dependency, runtime matrix, or schema
  source-of-truth gates.

## Next Roadmap Step

The next runtime feature after this contract is:

```text
AGENT-RUNTIME-SANDBOX-001
```
