# IAMINE Agent Dependency Policy

Feature:

```text
AGENT-DEPENDENCY-POLICY-001
```

## Purpose

Define which dependency classes are allowed, optional, deferred, or blocked for
IAMINE agent work before runtime execution, package manager integration,
dependency installation, sandboxing, registry readiness, marketplace
publication, worker startup, model loading, reputation, reward, or distributed
model MoE behavior exists.

This document is an architecture artifact. It does not authorize executable
agents, dependency installation, package manager execution, runtime execution,
sandboxing, registry publication, marketplace publication, third-party agents,
public beta launch, or public agent discovery.

## Policy Contract

The dependency policy answers one narrow review question:

```text
Which dependency classes may be considered for a given IAMINE agent layer?
```

It does not answer:

- whether dependencies may be installed;
- whether a package manager may run;
- whether an agent can execute;
- whether a runtime exists;
- whether sandboxing exists;
- whether a package is safe;
- whether a scheduler should route work to an agent;
- whether a worker should start;
- whether a model backend is available;
- whether an agent is trusted, reputable, certified, or rewarded;
- whether a package may be published publicly.

## Draft Schema

The first draft policy identifier is:

```text
iamine.agent.dependency_policy.draft-0.1
```

This is a repository-level policy contract. It is not a required file inside
the agent package skeleton in this phase.

This feature does not implement parsing, lockfile validation, vulnerability
scanning, license scanning, package manager execution, dependency installation,
sandbox startup, registry advancement, publication, or runtime loading.

## Dependency Classes

| Class | Status | Notes |
| --- | --- | --- |
| `rust_core_metadata` | allowed | `serde`, `serde_json`, `serde_yaml`, `schemars`, `jsonschema`, `thiserror`, and `semver` for IAMINE-owned schema and version-validation work. |
| `rust_cli_support` | optional | `clap`, `anyhow`, `tracing` only when an implementation feature needs them. |
| `python_sdk` | deferred | Public SDK work belongs to v1.2.x developer-platform gates. |
| `typescript_sdk` | deferred | Public SDK and dashboard/tooling work belongs to later gates. |
| `wasm_wasi_runtime` | deferred | Future sandbox direction, not current runtime availability. |
| `container_runtime` | deferred | Heavy agents require later registry, sandbox, permission, dependency, and runtime matrix gates. |
| `llm_framework` | blocked | Do not add external LLM agent frameworks in this phase. |
| `ocr_framework` | blocked | OCR/classification stays future and sandbox-gated. |
| `social_api_client` | blocked | Social publishing APIs are deferred advanced automation. |
| `router_api_client` | blocked | Network/router mutation is blocked for current agents. |
| `os_automation_advanced` | blocked | Advanced OS automation remains deferred and review-gated. |

Allowed or optional does not imply installation availability.

## Required Review Fields

Future dependency policy metadata must make these fields explicit:

| Field | Required | Purpose |
| --- | --- | --- |
| `schema` | yes | Dependency policy schema identifier. |
| `dependency_class` | yes | Dependency class being reviewed. |
| `allowed_status` | yes | Allowed, optional, deferred, or blocked. |
| `owner_layer` | yes | IAMINE layer that may own the dependency later. |
| `earliest_phase` | yes | Earliest roadmap phase where it may be considered. |
| `install_available` | yes | Must be false until installer/runtime gates authorize it. |
| `package_manager_available` | yes | Must be false until tooling gates authorize it. |
| `sandbox_required` | yes | Whether sandbox policy is required first. |
| `license_review_required` | yes | Whether license review is required first. |
| `security_review_required` | yes | Whether security review is required first. |
| `failure_policy` | yes | Behavior when metadata is missing or unsafe. |
| `review` | yes | Human review requirements and evidence links. |

## Blocked Dependency Claims

Dependency policy metadata must not claim:

- dependency installation authorization;
- package manager execution authorization;
- runtime execution authorization;
- runtime language availability;
- sandbox availability;
- permission enforcement;
- scheduler priority;
- node compatibility;
- backend availability;
- model admission;
- worker startup authorization;
- local registry readiness;
- public registry availability;
- public marketplace publication;
- third-party publication;
- public beta launch;
- trust, reputation, certification, or reward eligibility;
- wallet, settlement, token, or mainnet behavior;
- distributed model MoE.

## Privacy Rules

Dependency policy metadata must not include:

- credentials;
- private keys;
- wallet keys;
- usernames;
- full hostnames;
- IP addresses;
- MAC addresses;
- serial numbers;
- disk UUIDs;
- machine IDs;
- private paths;
- raw user prompts;
- raw outputs;
- raw process lists;
- unredacted logs;
- permanent hardware fingerprints.

## Review Requirements

Human review must confirm:

- allowed Rust metadata dependencies remain limited to IAMINE-owned schema and
  validator work;
- optional Rust support dependencies are justified by a later implementation
  feature;
- Python, TypeScript, WASM/WASI, and containers remain deferred;
- external LLM frameworks, OCR frameworks, social APIs, router APIs, and
  advanced OS automation dependencies remain blocked;
- package managers cannot run in this phase;
- dependency allowance cannot authorize install or runtime execution;
- dependency allowance cannot bypass package manifest, scope, permission,
  audit, boundary eval, local registry, language, sandbox, or runtime matrix
  gates;
- privacy-sensitive identifiers and secrets are absent.

## Next Roadmap Step

The next architecture feature after this contract is:

```text
AGENT-RUNTIME-LANGUAGE-MATRIX-001
```
