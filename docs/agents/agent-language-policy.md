# IAMINE Agent Language Policy

Feature:

```text
AGENT-LANGUAGE-POLICY-001
```

## Purpose

Define which implementation languages are allowed by IAMINE agent layer and
release phase before runtime execution, dependency installation, sandboxing,
registry readiness, marketplace publication, worker startup, model loading,
reputation, reward, or distributed model MoE behavior exists.

This document is an architecture artifact. It does not authorize executable
agents, runtime language execution, package manager installation, dependency
installation, sandboxing, registry publication, marketplace publication,
third-party agents, public beta launch, or public agent discovery.

## Policy Contract

The language policy answers one narrow review question:

```text
Which languages are allowed for a given IAMINE layer and roadmap phase?
```

It does not answer:

- whether an agent can execute;
- whether a runtime exists for that language;
- whether dependencies may be installed;
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
iamine.agent.language_policy.draft-0.1
```

This is a repository-level policy contract. It is not a required file inside
the agent package skeleton in this phase.

This feature does not implement schema parsing, runtime selection, package
manager integration, dependency resolution, sandbox startup, registry
advancement, installation, publication, or runtime loading.

## Language Placement

Allowed language placement by layer:

| Language | Allowed placement | Current phase status |
| --- | --- | --- |
| Rust | IAMINE core, node, runtime, CLI, contracts, validators, official P0 agent code, audit, registry, file/network/system agents | allowed for IAMINE-owned implementation only |
| Python | public SDK later, AI/dev tooling later, prototypes, OCR/classification future, heavy model integrations under sandbox | deferred |
| TypeScript | public SDK later, web/API integrations, dashboard/tooling, content connectors | deferred |
| WASM/WASI | preferred future sandbox for third-party lightweight agents | deferred |
| Containers | future heavy agents after registry, sandbox, permission, and dependency policy mature | deferred |

Allowed placement does not imply execution availability.

## Release Phase Policy

| Phase | Policy |
| --- | --- |
| v0.11.x Agent Architecture Foundation | Documentation contracts only; no executable agents. |
| v0.12.x P0 Official Agents | Rust is the default implementation language for IAMINE-owned official agent work once later runtime gates authorize it. |
| v0.13.x P1/P2 Agents and Beta Productization | Additional languages remain subject to dependency, sandbox, runtime matrix, and registry gates. |
| v1.0.0 Agent Network Public Beta | Runtime availability must still come from explicit runtime and registry features. |
| v1.2.x Public Agent Developer Platform | Python and TypeScript SDK surfaces may become eligible as developer tooling, not automatic runtime execution. |
| v1.3.x Curated Agent Registry | Registry admission must validate language, dependency, sandbox, and review policy. |
| v1.4.x Curated Marketplace | Marketplace publication remains separate from language support. |
| v2.0.x Mainnet | Wallet, settlement, token, and open marketplace behavior remain separate gates. |

## Manifest Format Policy

Agent metadata format policy remains:

```text
Authoring: YAML
Internal representation: Rust structs
Validation: generated JSON Schema
Runtime/API payloads: JSON
Source of truth: Rust types
```

This policy does not add dependencies. Future implementation features may add
dependencies only through the later dependency and schema source-of-truth gates.

## Required Review Fields

Future language policy metadata must make these fields explicit:

| Field | Required | Purpose |
| --- | --- | --- |
| `schema` | yes | Language policy schema identifier. |
| `language` | yes | Language family being reviewed. |
| `layer` | yes | IAMINE layer where the language may be used. |
| `roadmap_phase` | yes | Earliest phase where the language may be considered. |
| `allowed_status` | yes | Allowed, deferred, blocked, or experimental. |
| `runtime_available` | yes | Must be false until runtime matrix gates authorize it. |
| `dependency_policy_required` | yes | Whether dependency policy is required first. |
| `sandbox_policy_required` | yes | Whether sandbox policy is required first. |
| `registry_review_required` | yes | Whether local or public registry review is required. |
| `failure_policy` | yes | Behavior when language metadata is missing or unsafe. |
| `review` | yes | Human review requirements and evidence links. |

## Blocked Language Claims

Language policy metadata must not claim:

- runtime language execution;
- package installation authorization;
- package manager availability;
- dependency installation authorization;
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

Language policy metadata must not include:

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

- Rust remains the only allowed language for IAMINE-owned core, node, runtime,
  CLI, contract, validator, audit, registry, and file/network/system agent
  implementation in the current architecture phase;
- Python and TypeScript are deferred to SDK, tooling, connector, or later
  sandboxed use cases;
- WASM/WASI remains a future sandbox direction, not a current runtime;
- containers remain blocked until registry, sandbox, permission, dependency,
  and runtime matrix gates mature;
- language allowance cannot install dependencies;
- language allowance cannot authorize runtime execution;
- language allowance cannot bypass package manifest, scope, permission, audit,
  boundary eval, local registry, dependency, sandbox, or runtime matrix gates;
- privacy-sensitive identifiers and secrets are absent.

## Next Roadmap Step

The next architecture feature after this contract is:

```text
AGENT-DEPENDENCY-POLICY-001
```
