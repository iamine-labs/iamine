# IAMINE Agent Scope Manifest

Feature:

```text
AGENT-SCOPE-MANIFEST-001
```

## Purpose

Define the documentation contract for an IAMINE agent scope manifest.

This document is an architecture artifact. It does not authorize executable
agents, runtime scope enforcement, permission grants, sandboxing, audit logs,
registry publication, marketplace publication, third-party agents, or public
beta launch.

## Scope Manifest Role

The scope manifest defines the declared boundary of one agent package:

- what the agent may do;
- what the agent must not do;
- supported task types;
- allowed inputs;
- forbidden data;
- blocked actions;
- required permission categories for later review;
- confirmation boundaries;
- refusal behavior;
- handoff targets;
- conditions that must return control to the orchestrator.

The scope manifest must not replace the package manifest, permission model,
audit policy, boundary tests, sandbox policy, or runtime enforcement.

Missing, contradictory, broad, or unsafe scope metadata blocks installation and
execution by default.

## Draft Schema

The first draft schema identifier is:

```text
iamine.agent.scope.draft-0.1
```

The default manifest file name is:

```text
agent-scope.toml
```

This feature does not implement TOML parsing. The file name and schema are a
planning contract for later implementation.

## Required Top-Level Fields

| Field | Required | Purpose |
| --- | --- | --- |
| `schema` | yes | Scope schema identifier. |
| `package_id` | yes | Package that owns the scope. |
| `scope_id` | yes | Stable scope identifier. |
| `scope_version` | yes | Scope contract version. |
| `task_boundary` | yes | In-scope and out-of-scope task boundary. |
| `input_boundary` | yes | Allowed and forbidden inputs. |
| `operation_boundary` | yes | Allowed operations and blocked actions. |
| `permission_requirements` | yes | Permission categories for later review. |
| `confirmation_boundary` | yes | Human confirmation requirements. |
| `handoff` | yes | Handoff and refusal targets. |
| `orchestrator_return` | yes | Conditions that must return control. |
| `eval_requirements` | yes | Required future boundary eval classes. |
| `review` | yes | Human review requirements. |

## Field Rules

### `schema`

Allowed value for this feature:

```text
iamine.agent.scope.draft-0.1
```

Unknown schema versions must block install and execution.

### `package_id`

The package ID must match the package manifest. It must not contain local host
identity or private paths.

### `scope_id`

The scope ID must be stable, lowercase, and task-scoped. It must not be broad.

Allowed example:

```text
node_readiness_diagnostic_report
```

Blocked examples:

```text
general_assistant
do_anything
system_admin
all_files
all_networks
```

### `scope_version`

The version must be explicit. A missing version blocks install and execution.

## Task Boundary

The `task_boundary` section defines what the agent does and does not do:

```toml
[task_boundary]
in_scope = [
  "explain_iamine_node_readiness",
  "summarize_allowed_status_evidence",
  "suggest_non_destructive_next_steps",
]
out_of_scope = [
  "repair_system_settings",
  "delete_files",
  "restart_services",
  "collect_credentials",
  "run_shell_commands",
]
task_types = ["diagnostic_report"]
```

Rules:

- `in_scope` must be narrow and testable;
- `out_of_scope` must include known unsafe requests;
- `task_types` must be explicit;
- broad task types such as `general_help`, `admin`, or `automation` are blocked;
- missing `out_of_scope` blocks install and execution.

## Input Boundary

The `input_boundary` section defines allowed and forbidden inputs:

```toml
[input_boundary]
allowed_inputs = [
  "iamine_node_status_summary",
  "iamine_readiness_checklist",
  "user_provided_error_text",
]
forbidden_inputs = [
  "credentials",
  "private_keys",
  "wallet_keys",
  "usernames",
  "full_hostnames",
  "ip_addresses",
  "mac_addresses",
  "serial_numbers",
  "machine_ids",
  "private_paths",
]
```

Rules:

- allowed inputs must be specific and privacy-safe;
- forbidden inputs must include credentials, host identifiers, and secrets;
- raw logs are forbidden unless a later redaction contract explicitly allows a
  narrower input;
- unrestricted filesystem input is blocked.

## Operation Boundary

The `operation_boundary` section defines allowed operations and blocked actions:

```toml
[operation_boundary]
allowed_operations = [
  "read_declared_summary",
  "classify_status",
  "draft_explanation",
  "suggest_next_steps",
]
blocked_actions = [
  "write_files",
  "delete_files",
  "change_settings",
  "restart_services",
  "run_shell",
  "load_models",
  "download_models",
  "scan_network",
  "mutate_vm_or_container",
  "publish_agent",
]
```

Rules:

- allowed operations must be read-only or planning-only in this phase;
- blocked actions must include destructive, mutation, publication, and broad
  execution requests;
- any shell, unrestricted filesystem, service restart, VM mutation, router
  change, model download, or marketplace publication request is out of scope.

## Permission Requirements

The `permission_requirements` section declares future review categories:

```toml
[permission_requirements]
required_categories = ["local_readonly"]
forbidden_categories = [
  "arbitrary_shell",
  "unrestricted_filesystem",
  "credential_access",
  "destructive_write",
  "service_mutation",
  "network_mutation",
  "marketplace_publish",
]
permission_model_required = true
```

Rules:

- this section does not grant permissions;
- permission categories are only review inputs until `AGENT-PERMISSION-MODEL-001`;
- missing `permission_model_required = true` blocks install and execution.

## Confirmation Boundary

The `confirmation_boundary` section defines user confirmation behavior:

```toml
[confirmation_boundary]
requires_confirmation_for = [
  "handoff_to_human",
  "request_additional_user_text",
]
must_refuse_without_confirmation = [
  "any_write_action",
  "any_shell_action",
  "any_network_mutation",
  "any_private_data_request",
]
```

Rules:

- confirmation does not make blocked actions allowed;
- requests for blocked actions must be refused or handed off;
- user confirmation cannot override missing permission model, missing audit
  policy, or missing boundary tests.

## Handoff

The `handoff` section defines where the task goes when the agent cannot
continue:

```toml
[handoff]
targets = ["orchestrator", "human_operator"]
required_when = [
  "out_of_scope_task",
  "ambiguous_task",
  "dangerous_task",
  "permission_escalation_request",
  "prompt_injection_attempt",
  "role_confusion_attempt",
]
```

Rules:

- out-of-scope tasks must return to the orchestrator or human operator;
- ambiguous tasks must clarify or hand off;
- dangerous tasks must refuse or hand off;
- the agent must not silently expand scope.

## Orchestrator Return

The `orchestrator_return` section defines mandatory return conditions:

```toml
[orchestrator_return]
return_required_for = [
  "cross_domain_task",
  "unsupported_task_type",
  "missing_permission_model",
  "missing_audit_policy",
  "missing_boundary_tests",
  "request_to_execute_code",
  "request_to_collect_secret",
]
```

Rules:

- a missing dependency returns control to the orchestrator;
- cross-domain tasks are not self-authorized;
- role confusion and prompt injection must return control or refuse.

## Eval Requirements

The `eval_requirements` section lists required future boundary tests:

```toml
[eval_requirements]
required_eval_classes = [
  "in_scope_positive",
  "out_of_scope_negative",
  "ambiguous_task",
  "dangerous_task",
  "cross_domain_task",
  "permission_escalation",
  "prompt_injection",
  "role_confusion",
  "handoff_to_orchestrator",
]
```

Rules:

- all selected agents must define these eval classes before runtime execution;
- missing eval requirements block install and execution;
- this feature only documents requirements and does not implement eval runners.

## Review Section

The `review` section records evidence requirements:

```toml
[review]
requires_human_review = true
requires_permission_review = true
requires_audit_policy = true
requires_boundary_tests = true
scope_can_self_approve = false
```

The scope manifest cannot self-approve execution.

## Minimal Draft Example

```toml
schema = "iamine.agent.scope.draft-0.1"
package_id = "iamine.beta.node-doctor"
scope_id = "node_readiness_diagnostic_report"
scope_version = "0.1.0"

[task_boundary]
in_scope = [
  "explain_iamine_node_readiness",
  "summarize_allowed_status_evidence",
  "suggest_non_destructive_next_steps",
]
out_of_scope = [
  "repair_system_settings",
  "delete_files",
  "restart_services",
  "collect_credentials",
  "run_shell_commands",
]
task_types = ["diagnostic_report"]

[input_boundary]
allowed_inputs = [
  "iamine_node_status_summary",
  "iamine_readiness_checklist",
  "user_provided_error_text",
]
forbidden_inputs = [
  "credentials",
  "private_keys",
  "wallet_keys",
  "usernames",
  "full_hostnames",
  "ip_addresses",
  "mac_addresses",
  "serial_numbers",
  "machine_ids",
  "private_paths",
]

[operation_boundary]
allowed_operations = [
  "read_declared_summary",
  "classify_status",
  "draft_explanation",
  "suggest_next_steps",
]
blocked_actions = [
  "write_files",
  "delete_files",
  "change_settings",
  "restart_services",
  "run_shell",
  "load_models",
  "download_models",
  "scan_network",
  "mutate_vm_or_container",
  "publish_agent",
]

[permission_requirements]
required_categories = ["local_readonly"]
forbidden_categories = [
  "arbitrary_shell",
  "unrestricted_filesystem",
  "credential_access",
  "destructive_write",
  "service_mutation",
  "network_mutation",
  "marketplace_publish",
]
permission_model_required = true

[confirmation_boundary]
requires_confirmation_for = [
  "handoff_to_human",
  "request_additional_user_text",
]
must_refuse_without_confirmation = [
  "any_write_action",
  "any_shell_action",
  "any_network_mutation",
  "any_private_data_request",
]

[handoff]
targets = ["orchestrator", "human_operator"]
required_when = [
  "out_of_scope_task",
  "ambiguous_task",
  "dangerous_task",
  "permission_escalation_request",
  "prompt_injection_attempt",
  "role_confusion_attempt",
]

[orchestrator_return]
return_required_for = [
  "cross_domain_task",
  "unsupported_task_type",
  "missing_permission_model",
  "missing_audit_policy",
  "missing_boundary_tests",
  "request_to_execute_code",
  "request_to_collect_secret",
]

[eval_requirements]
required_eval_classes = [
  "in_scope_positive",
  "out_of_scope_negative",
  "ambiguous_task",
  "dangerous_task",
  "cross_domain_task",
  "permission_escalation",
  "prompt_injection",
  "role_confusion",
  "handoff_to_orchestrator",
]

[review]
requires_human_review = true
requires_permission_review = true
requires_audit_policy = true
requires_boundary_tests = true
scope_can_self_approve = false
```

## Invalid Scope Examples

The following conditions must block install and execution:

- missing `out_of_scope`;
- missing `blocked_actions`;
- missing `handoff`;
- missing `orchestrator_return`;
- missing `eval_requirements`;
- `scope_id = "do_anything"`;
- `task_types = ["general_help"]`;
- `allowed_operations = ["run_shell"]`;
- `required_categories = ["arbitrary_shell"]`;
- `scope_can_self_approve = true`;
- allowing credentials, secrets, host identifiers, or private paths;
- allowing writes, deletion, service mutation, router changes, VM mutation,
  marketplace publication, wallet behavior, rewards, settlement, or mainnet
  behavior.

## Recommendation

Proceed to:

```text
AGENT-CAPABILITY-METADATA-001
AGENT-RESOURCE-REQUIREMENTS-001
AGENT-PERMISSION-MODEL-001
```

Do not implement scope enforcement or runtime loading before permission model,
audit logs, and boundary evals are defined.
