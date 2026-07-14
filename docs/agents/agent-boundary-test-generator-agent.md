# IAMINE Internal Agent Boundary-Test Generator Assistant

Feature:

```text
AGENT-BOUNDARY-TEST-GENERATOR-AGENT-001-INTERNAL
```

## Purpose

Define the future internal boundary-test generator assistant boundary without
implementing test execution, file writes, manifest mutation, permission grants,
scope approval, runtime authorization, publication, marketplace behavior, or
model inference.

The boundary-test generator may draft operator-visible boundary test plans from
provided scope and permission review material. It does not run tests, create
test files, approve agents, or publish agents by itself.

## Assistant Question

Internal boundary-test generator assistant policy answers:

```text
What boundaries must a future IAMINE boundary-test generator assistant preserve?
```

It does not answer whether test runners, file writers, harnesses, approval UI,
runtime enforcement, audit logs, or registry adapters exist.

## Draft Schema

```text
iamine.agent.internal.boundary_test_generator.draft-0.1
```

## Allowed Scope

Future internal boundary-test generator assistants may request only:

```text
summarize_reviewed_scope_and_permissions
draft_boundary_test_matrix
draft_negative_test_cases
identify_missing_boundary_coverage
request_clarification
handoff_for_operator_approved_test_file_generation
handoff_for_operator_approved_test_execution
handoff_to_manual_review
```

They must not run tests, write files, mutate manifests, approve scope, approve
permissions, execute commands, publish agents, mutate registries, or claim
validation without source evidence.

## Required Guards

Future assistants must declare:

```text
boundary_input_source_policy
negative_test_policy
no_execution_policy
file_generation_handoff_policy
permission_coverage_policy
scope_coverage_policy
operator_visible_summary
```

## Privacy Rules

Boundary-test generator metadata must not include credentials, private keys,
wallet keys, tokens, usernames, full hostnames, IP addresses, MAC addresses,
serial numbers, disk UUIDs, machine IDs, private paths, raw user prompts, raw
outputs, raw process lists, unredacted logs, personal communications, or
permanent hardware fingerprints.

## Boundary Rules

- Boundary-test generator assistants cannot authorize runtime execution.
- Boundary-test generator assistants cannot run tests or execute commands.
- Boundary-test generator assistants cannot write test files by default.
- Boundary-test generator assistants cannot approve scope or permissions.
- Boundary-test generator assistants cannot publish to registry or marketplace.
- Boundary-test generator assistants cannot claim validation without executed
  evidence from an authorized harness.
- Boundary-test generator assistants cannot bypass validation, permission
  review, scope review, manual review, audit, or local registry review.

## Next Roadmap Step

```text
v0.11.3 milestone QA closeout
```
