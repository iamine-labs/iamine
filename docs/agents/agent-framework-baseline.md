# IAMINE Agent Framework Baseline

Feature:

```text
AGENT-FRAMEWORK-BASELINE-001
```

## Purpose

Define the internal baseline shared by future official IAMINE agent templates
without implementing a runtime framework, SDK, generator, validator, package
manager, registry publication, marketplace publication, or agent execution.

This document does not authorize executable agents, filesystem mutation, shell
execution, unrestricted network access, dependency installation, auto
publication, wallet, reward, settlement, mainnet behavior, or distributed model
MoE.

## Baseline Question

Framework baseline policy answers:

```text
What common non-runtime contract must official templates share?
```

It does not answer whether runtime libraries, SDKs, CLIs, audit logs,
packaging, publication, or execution exists.

## Draft Schema

```text
iamine.agent.framework_baseline.draft-0.1
```

## Required Baseline Sections

Future official templates must declare:

```text
manifest_contract
scope_contract
permission_contract
input_contract
output_contract
timeout_contract
handoff_contract
out_of_scope_contract
boundary_test_contract
qa_contract
manual_review_contract
```

Templates without these sections must be blocked by default.

## Common Package Shape

Future official templates must preserve:

```text
README.md
manifest.iamine.json
src/
tests/
qa/
docs/
```

The baseline does not authorize writing, executing, building, installing, or
publishing those files.

## Blocked Framework Claims

The framework baseline must not claim:

```text
runtime_available
permissions_granted
scope_approved
publication_ready
marketplace_ready
trusted_agent
reward_eligible
mainnet_ready
```

## Privacy Rules

Framework baseline metadata must not include credentials, private keys, wallet
keys, usernames, full hostnames, IP addresses, MAC addresses, serial numbers,
disk UUIDs, machine IDs, private paths, raw user prompts, raw outputs, raw
process lists, unredacted logs, or permanent hardware fingerprints.

## Boundary Rules

- Framework baseline cannot authorize runtime execution.
- Framework baseline cannot implement an SDK or runtime.
- Framework baseline cannot grant permissions or approve scope.
- Framework baseline cannot publish to registry or marketplace.
- Framework baseline cannot bypass template validation, boundary tests, manual
  review, audit, or local registry review.

## Next Roadmap Step

```text
AGENT-TEMPLATE-DIAGNOSTIC-001
```
