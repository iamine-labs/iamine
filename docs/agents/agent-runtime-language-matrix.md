# IAMINE Agent Runtime Language Matrix

Feature:

```text
AGENT-RUNTIME-LANGUAGE-MATRIX-001
```

## Purpose

Define which agent runtime language modes are available, deferred, or blocked
before execution features implement real agent runtime behavior.

This document is an architecture artifact. It does not authorize executable
agents, runtime execution, package installation, package manager execution,
dependency installation, sandboxing, registry publication, marketplace
publication, third-party agents, public beta launch, or public agent discovery.

## Matrix Contract

The runtime language matrix answers one narrow review question:

```text
Which language runtime modes are eligible for future runtime design?
```

It does not answer:

- whether the runtime exists;
- whether an agent can execute;
- whether dependencies may be installed;
- whether package managers may run;
- whether sandboxing exists;
- whether permissions are enforced;
- whether a worker should start;
- whether a model backend is available;
- whether an agent is trusted, reputable, certified, or rewarded;
- whether a package may be published publicly.

## Draft Schema

The first draft matrix identifier is:

```text
iamine.agent.runtime_language_matrix.draft-0.1
```

This is a repository-level runtime planning contract. It is not a required
file inside the agent package skeleton in this phase.

This feature does not implement parsing, runtime selection, process spawning,
interpreter startup, WASM execution, container execution, package manager
integration, dependency installation, sandbox startup, registry advancement,
publication, or runtime loading.

## Runtime Modes

| Runtime mode | Status | Earliest eligible phase | Notes |
| --- | --- | --- | --- |
| `rust_native_official` | planned | v0.12.x | IAMINE-owned official agents only after runtime baseline and sandbox gates. |
| `rust_metadata_validator` | planned | v0.11.x implementation gates | Schema and validator work only; not agent execution. |
| `python_sdk_tooling` | deferred | v1.2.x | Public SDK and developer tooling, not runtime execution. |
| `typescript_sdk_tooling` | deferred | v1.2.x | Public SDK, dashboards, and tooling, not runtime execution. |
| `wasm_wasi_sandboxed_agent` | deferred | v1.3.x or later | Future sandbox direction for lightweight third-party agents. |
| `container_sandboxed_agent` | deferred | v1.3.x or later | Heavy agents only after registry, sandbox, dependency, and permission gates. |
| `arbitrary_shell_agent` | blocked | none | Arbitrary shell execution is not an agent runtime mode. |
| `unrestricted_filesystem_agent` | blocked | none | Unrestricted filesystem access is blocked. |
| `mainnet_wallet_agent` | blocked | none in v0.x/v1.0 | Wallet, settlement, token, and mainnet behavior are separate gates. |

`planned` does not mean executable. It means a later feature may design the
runtime mode after prerequisite gates close.

## Required Review Fields

Future runtime matrix metadata must make these fields explicit:

| Field | Required | Purpose |
| --- | --- | --- |
| `schema` | yes | Runtime matrix schema identifier. |
| `runtime_mode` | yes | Runtime mode being reviewed. |
| `language` | yes | Language family. |
| `status` | yes | Planned, deferred, or blocked. |
| `earliest_phase` | yes | Earliest roadmap phase for consideration. |
| `execution_available` | yes | Must be false until runtime implementation gates authorize it. |
| `sandbox_required` | yes | Whether sandbox policy is required first. |
| `dependency_policy_required` | yes | Whether dependency policy is required first. |
| `permission_policy_required` | yes | Whether permission policy is required first. |
| `registry_review_required` | yes | Whether registry review is required first. |
| `failure_policy` | yes | Behavior when metadata is missing or unsafe. |
| `review` | yes | Human review requirements and evidence links. |

## Blocked Runtime Claims

Runtime matrix metadata must not claim:

- runtime execution authorization;
- package installation authorization;
- package manager execution authorization;
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

Runtime matrix metadata must not include credentials, private keys, wallet keys,
usernames, full hostnames, IP addresses, MAC addresses, serial numbers, disk
UUIDs, machine IDs, private paths, raw user prompts, raw outputs, raw process
lists, unredacted logs, or permanent hardware fingerprints.

## Review Requirements

Human review must confirm:

- `rust_native_official` remains planned, not executable;
- Python and TypeScript remain SDK/tooling only until developer-platform gates;
- WASM/WASI and containers remain deferred until sandbox and registry gates;
- arbitrary shell, unrestricted filesystem, wallet, settlement, token, and
  mainnet modes remain blocked;
- execution availability remains false until runtime features implement it;
- runtime matrix cannot bypass language, dependency, package manifest, scope,
  permission, audit, boundary eval, local registry, sandbox, or runtime gates.

## Next Roadmap Step

The next architecture feature after this contract is:

```text
AGENT-MANIFEST-SCHEMA-SOURCE-OF-TRUTH-001
```
