# IAMINE Agent Skeleton Standard

Feature:

```text
AGENT-SKELETON-STANDARD-001
```

## Purpose

Define the canonical file layout for IAMINE agent packages. The root
`agent.yaml` parser now exists in `iamine-agents`; skeleton generation,
referenced-metadata parsing, package loading, runtime, sandbox, and registry
behavior remain unavailable.

This document is an architecture artifact. It does not authorize executable
agents, runtime execution, permission enforcement, sandboxing, registry
publication, marketplace publication, third-party agents, or public beta
launch.

## Skeleton Contract

An IAMINE agent skeleton is a reviewable package shape. It tells reviewers
where package metadata, scope metadata, permissions, audit policy, boundary
tests, and future implementation files belong.

It is not:

- an executable agent;
- a generated package;
- a permission grant;
- a scope approval;
- a registry approval;
- a sandbox;
- a public marketplace listing.

## Required Layout

Future packages must use this layout:

```text
<agent-package>/
  agent.yaml
  agent-scope.yaml
  README.md
  metadata/
    agent-capabilities.yaml
    agent-expertise.yaml
    agent-resources.yaml
    agent-permissions.yaml
    agent-audit.yaml
  evals/
    agent-boundary-tests.yaml
    README.md
  src/
    README.md
  review/
    human-review.md
    qa-evidence.md
```

All paths are package-relative. Absolute local paths are blocked.

## File Responsibilities

| Path | Responsibility |
| --- | --- |
| `agent.yaml` | Package identity and references. |
| `agent-scope.yaml` | In-scope, out-of-scope, handoff, and blocked action boundary. |
| `metadata/agent-capabilities.yaml` | Future capability metadata. |
| `metadata/agent-expertise.yaml` | Future expertise metadata. |
| `metadata/agent-resources.yaml` | Future resource requirements. |
| `metadata/agent-permissions.yaml` | Future permission categories and denial behavior. |
| `metadata/agent-audit.yaml` | Future privacy-safe audit policy. |
| `evals/agent-boundary-tests.yaml` | Future positive and negative boundary evals. |
| `src/` | Future implementation area only. |
| `review/` | Human review and QA evidence. |

Each file remains owned by its roadmap feature. The skeleton only defines
placement.

## Required Safety Defaults

Skeleton packages are blocked by default if they:

- omit the package manifest;
- omit the scope manifest;
- omit blocked actions;
- omit permission denial behavior;
- omit boundary eval references;
- include credentials, private keys, wallet keys, host identifiers, or private
  paths;
- use broad package names such as `general-assistant` or `do-anything`;
- include absolute filesystem paths;
- request arbitrary shell;
- request unrestricted filesystem access;
- request unrestricted network access;
- request service mutation;
- request public marketplace publication;
- request wallet, reward, settlement, token, or mainnet behavior.

## Source Directory Policy

The `src/` directory is reserved. It must not run code in this phase.

Until later runtime and language-policy features exist:

- files under `src/` are non-executable placeholders;
- no package manager install step is implied;
- no script entrypoint is recognized;
- no background service is installed;
- no model is loaded or downloaded;
- no network, filesystem, shell, VM, container, wallet, or settlement action is
  available.

## Review Requirements

Before a future skeleton instance can move past review, it must include:

- package identity review;
- scope review;
- capability metadata review;
- expertise metadata review;
- resource review;
- permission review;
- audit review;
- boundary eval review;
- human review evidence;
- QA evidence.

Missing evidence blocks install, registry admission, and execution by default.

## Relationship To Existing Contracts

This standard consumes the closed package and scope contracts:

```text
AGENT-PACKAGE-MANIFEST-001
AGENT-SCOPE-MANIFEST-001
```

The skeleton must not inline broad scope, permissions, runtime states, audit
behavior, or boundary eval results. Those remain separate contracts.

## Next Roadmap Step

The next architecture feature after this contract is:

```text
AGENT-CAPABILITY-METADATA-001
```
