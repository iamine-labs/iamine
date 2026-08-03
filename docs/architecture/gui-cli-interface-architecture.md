# GUI-CLI-INTERFACE-ARCHITECTURE-001

## Status

```text
feature: GUI-CLI-INTERFACE-ARCHITECTURE-001
state: READY FOR MERGE REVIEW
base: origin/develop at 2d51b9532992b0857856b8d3450cc9e85cf2470c
branch: feature/gui-cli-interface-architecture-001
runtime behavior change: none
frontend application created: no
shared payload schema finalized: no
```

## Purpose

Define how IAMINE's local dashboard and CLI expose one core without creating
two policy engines or treating the frontend as a privileged runtime client.

The dashboard is the default operator interface for general users. The CLI
remains the advanced, automation, QA, and headless interface. This feature
defines ownership and interaction boundaries; `GUI-CLI-SHARED-CONTRACTS-001`
will define the stable payload schemas and command/event types.

## Architectural Decision

IAMINE has one domain core and two interface adapters:

```text
                         +----------------------+
                         | IAMINE owner modules |
                         | policy, state, audit |
                         +----------+-----------+
                                    |
                         shared typed operation
                         and contract boundary
                                    |
              +---------------------+---------------------+
              |                                           |
       +------+-------+                            +------+-------+
       | CLI adapter  |                            | API adapter  |
       | parse/render |                            | local auth   |
       +------+-------+                            +------+-------+
              |                                           |
       human/JSON CLI                               dashboard data source
                                                          |
                                                   +------+-------+
                                                   | UI features  |
                                                   | presentation |
                                                   +--------------+
```

The core owns truth and policy. An adapter may translate transport or command
syntax, but it may not decide eligibility, permission, scope, audit, scheduler,
model, P2P, worker, lifecycle, or agent execution behavior.

## Existing Owner Evidence

The architecture is grounded in current repository surfaces:

| Concern | Current owner evidence | Interface rule |
| --- | --- | --- |
| Node evidence | `iamine-node/src/node_doctor_evidence_provider.rs` | Expose only its typed, redacted, read-only projection through a later contract. |
| LAN diagnostics | `iamine-node/src/lan_node_doctor.rs` | Reuse owner checks; do not parse CLI text or duplicate check identifiers. |
| Cluster state | `iamine-node/src/cluster_status.rs` and `cluster_registry.rs` | Preserve snapshot/readiness semantics; dashboard does not recompute health. |
| Hardware | `iamine-hardware` through `hardware_cli.rs` | Keep hardware collection and persistence outside UI code. |
| Models | `iamine-models` and node capability owners | Display availability reported by owners; do not infer compatibility in TypeScript. |
| Identity/configuration | `node_identity_cli.rs` and `node_config_schema.rs` | Treat read and mutation as different operations with different gates. |
| Tasks/support | `tasks_cli.rs` and `user_diagnostics_support.rs` | Render bounded reports; never expose raw logs, paths, or secrets. |
| Agent policy/runtime | `iamine-agents` and `iamine-agent-runtime` | Dashboard cannot authorize or execute an agent locally. |

The current CLI JSON renderers are useful owner-level evidence, but their
serialized shapes are not automatically the future Local Control API. The
shared-contract feature must review compatibility, privacy, versioning, and
provenance before promoting any shape.

## Ownership Matrix

| Layer | Owns | Must not own |
| --- | --- | --- |
| Domain crates and owner modules | State, policy, validation, redaction, lifecycle, execution decisions | Interface layout, browser state, CLI wording |
| Shared contract layer | Operation IDs, typed request/response/event schemas, versioning, compatibility rules | Business decisions, network binding, UI rendering |
| CLI adapter | Argument parsing, command help, human rendering, explicit JSON rendering | Policy reimplementation, shelling out to itself, hidden runtime startup |
| Local Control API | Local transport, origin/session checks, request validation, authorization handoff, bounded response transport, audit handoff | P2P access, frontend policy, arbitrary command execution |
| Dashboard adapter | API client, generated DTO decoding, view-model mapping, loading/error state | Direct domain policy, permission decisions, shell or filesystem access |
| Dashboard UI | Presentation, navigation, accessibility, user intent collection | Calling P2P, selecting models, granting permissions, executing agents |
| QA and release | Contract, parity, privacy, security, visual, and field evidence | Mutating runtime during mock-only validation |

`iamine-node/src/main.rs` remains wiring only. `iamine-node/src/cluster_registry.rs`
remains stable; any future growth requires an extraction plan before code is
added.

## Operation Model

Every interface action must identify one shared operation intent before it is
rendered or transported. Operation intent is not a route path or CLI string.

The initial operation classes are:

| Class | Examples from current surfaces | Default interface status |
| --- | --- | --- |
| Read-only diagnostic | node evidence, hardware inspect, node config status, identity status | CLI available; dashboard mock-only until Local Control API read contract closes |
| Read-only operational | cluster status, task stats, task trace, model catalog | CLI available where existing; dashboard integration deferred |
| Planned mutation | config migrate/rollback, identity init, hardware refresh, support bundle write | Must expose a plan or unavailable state before any confirmation flow |
| Runtime mutation | worker lifecycle, resource controls, service activation | Not available to dashboard until lifecycle, authorization, and audit gates close |
| Agent operation | permission confirmation, handoff, execution, cancellation | Must use agent runtime authority; UI cannot execute or authorize directly |

These classes are architectural categories, not a replacement for the stable
operation IDs. The next shared-contract feature owns the canonical names,
request shapes, response shapes, and compatibility policy.

An operation is not successful merely because an adapter parsed a command or a
mock returned data. Success requires an owner-produced result with valid
provenance and all relevant gates satisfied.

## CLI Flow

The CLI flow is:

```text
argv
-> CLI parser
-> shared operation intent
-> owner service / policy gate
-> typed result or typed problem
-> human renderer or explicit JSON renderer
```

Rules:

- human text is for operators and must not be parsed by the dashboard;
- `--json` output is machine-readable CLI output but is not automatically the
  Local Control API contract;
- help and read-only commands must not start workers, P2P, PubSub, model loads,
  downloads, inference, or dynamic checks unless explicitly authorized by the
  command contract;
- CLI options that are advanced or headless-only may remain CLI-only;
- CLI compatibility requires preserving existing human output unless a
  separate feature documents an intentional change.

The CLI adapter may call an owner function directly. It must not invoke a
second copy of IAMINE through a shell or parse its own serialized output.

## Dashboard Flow

The future real dashboard flow is:

```text
user intent
-> dashboard operation client
-> localhost Local Control API
-> local authorization and request validation
-> owner module / shared policy gate
-> redacted typed response and audit evidence
-> dashboard adapter
-> view model
-> accessible presentation
```

The mock-only flow is intentionally separate:

```text
typed fixture
-> MockDashboardDataSource
-> view model
-> presentation
```

There is no fallback from a failed real request to mock data. Mock mode must be
explicit and visibly non-authoritative. A mock must not expose a callback that
can be rebound to a real node action.

The dashboard must not:

- execute `iamine-node` through a subprocess;
- build shell commands dynamically;
- call P2P, PubSub, scheduler, worker, model, or agent modules directly;
- read raw logs, filesystem paths, process lists, or credentials;
- recreate Rust validation, authorization, redaction, or readiness logic;
- treat browser localhost access as proof of operator authorization.

## Shared Contract Boundary

`GUI-CLI-SHARED-CONTRACTS-001` must establish the following without moving
policy into adapters:

```text
Rust owner type and policy result
-> reviewed shared operation contract
-> generated JSON Schema / transport DTO
-> CLI adapter and Local Control API adapter
-> interface-specific renderer or view model
```

The contract layer must provide, at minimum:

- bounded operation identity;
- schema version and compatibility behavior;
- typed success, unavailable, blocked, stale, and error outcomes;
- redacted data and provenance declarations;
- bounded warnings that cannot upgrade a blocked result;
- event identity and ordering rules where a stream is required;
- explicit read-only, planned, and mutating operation classes;
- no free-form command, shell, URL, path, or credential fields.

The final payload field names, schema IDs, and generated-code mechanism remain
out of scope here so the next feature can review them as one coherent contract
set. Rust owner types remain the source of truth; TypeScript types must be
generated or mechanically derived from approved schemas and never hand-copied
as a second domain model.

Unknown enum values, missing required fields, incompatible schema versions,
malformed payloads, and contradictory state must become explicit unavailable or
error outcomes. An adapter must not guess a successful value.

## Error and State Semantics

The two interfaces must preserve the distinction between:

```text
available      owner has valid current evidence
attention      owner has usable evidence with warnings
blocked        a gate rejects the operation
unavailable    the owner or required contract cannot provide evidence
stale          evidence exists but exceeded its freshness boundary
unknown        required evidence was not observed or cannot be classified
```

An interface may translate wording, but it must not merge `blocked` into
`unavailable`, or `unknown` into `available`. A UI disabled state is a
presentation of an authoritative outcome, not a permission decision.

Error rendering must include a stable code, safe operator guidance, and the
operation context without echoing prompts, paths, host identifiers, peer
addresses, credentials, or arbitrary backend messages.

## Event Boundary

The dashboard may eventually consume a bounded status/event stream through the
Local Control API. It must not tail NDJSON files or subscribe to P2P topics.

Events must be:

- emitted by the owning runtime or audit module;
- typed and versioned;
- redacted before crossing the interface boundary;
- bounded in size and rate;
- ordered or explicitly marked as a snapshot/reconciliation event;
- safe to drop and replay without authorizing an action.

An event that says a permission was requested is not a permission grant. An
event that says an agent completed is not proof that a user approved its next
action. UI state must be reconciled with authoritative read responses.

## Security and Privacy Contract

The interface architecture adopts these non-bypass rules:

```text
localhost-only by default
no default 0.0.0.0 bind
no remote dashboard before security review
no credentials or private keys in frontend bundles
no sensitive values in browser storage
no direct P2P or filesystem access from the dashboard
no frontend authorization or audit implementation
no mutation before local authorization and audit close
no mock evidence claimed as production integration
```

The Local Control API contract must define origin/session handling, replay
protection where needed, request limits, and authorization handoff. This
feature does not select a transport or implement authentication.

The dashboard may display a redacted reason code and safe status summary. It
must not request more data merely because a user opens a detail view; each
detail operation has its own contract and privacy review.

## Compatibility and Platform Rules

- CPU-only nodes, GPU nodes, macOS, Linux, VMs, containers, mock workers, and
  constrained hosts remain supported by the core.
- Headless nodes remain usable without the dashboard, Node.js, or a browser.
- Existing CLI commands remain the recovery and automation surface.
- The browser target remains local and responsive; native mobile is deferred.
- A future desktop wrapper must reuse the same Local Control API and contracts,
  not create a privileged bridge around domain internals.
- A remote dashboard is a separate security and deployment feature.

## Test and QA Contract

This architecture feature is documentation-only. It requires no field QA and
does not authorize a frontend application or API implementation.

The next shared-contract feature must add focused evidence for:

- CLI and API operation identity parity;
- schema version and incompatible-version behavior;
- success, blocked, unavailable, stale, and unknown outcome preservation;
- safe error-code redaction;
- no direct shell, P2P, filesystem, or runtime access from adapters;
- mock adapter isolation from real actions;
- event ordering, replay, drop, and reconciliation semantics;
- compatibility with existing CLI human and explicit JSON behavior.

Later visual features add browser, accessibility, responsive, visual, loading,
empty, error, and no-overlap evidence. Real API and lifecycle features add
Mac, TS140, and Proxmox/R5500 field QA when their runtime surface requires it.

## Parallel Ownership

After this feature closes, the following visual work may proceed in separate
worktrees with non-overlapping ownership:

| Feature | Ownership |
| --- | --- |
| IAMINE-DASHBOARD-DESIGN-SYSTEM-001 | `dashboard/src/components/`, `dashboard/src/styles/` |
| IAMINE-DASHBOARD-SHELL-001 | `dashboard/src/app/` |
| IAMINE-DASHBOARD-OVERVIEW-MOCK-001 | `dashboard/src/features/overview/`, `dashboard/src/mocks/` |

Shared contracts, generated DTOs, package manifests, lockfiles, global config,
CI, and Local Control API files remain serialized integration surfaces.

## Out of Scope

This feature does not:

- create a frontend application or install dependencies;
- finalize shared DTO field names or schema IDs;
- implement a Local Control API or local authorization;
- expose real node data to a browser;
- add dashboard routes, components, mock fixtures, or visual assets;
- change CLI output, runtime, scheduler, P2P, PubSub, worker, model, hardware,
  lifecycle, agent, packaging, or service behavior;
- add a browser session, token store, telemetry, remote endpoint, or desktop
  wrapper.

## Risks and Controls

| Risk | Control |
| --- | --- |
| CLI and dashboard drift | One operation identity and owner result; separate renderers and adapters. |
| Frontend becomes a policy engine | UI receives authoritative outcomes; all decisions remain in Rust owners/API. |
| CLI JSON is mistaken for an API | Shared-contract review is required before any real dashboard integration. |
| Mocks become a hidden action path | Mock data source has no transport or mutation callback and is visibly non-authoritative. |
| Events become an authorization channel | Events are advisory/reconcilable; authoritative read and permission responses decide state. |
| Sensitive diagnostics leak | Redaction and bounded reason codes happen at the owner/API boundary. |
| Main module grows | This feature adds no Rust code; future wiring stays out of `main.rs` except minimal dispatch. |
| Parallel work conflicts | Component, shell, overview, contract, and config ownership is explicit. |

## Acceptance Criteria

- dashboard and CLI ownership is explicit;
- current owner modules are identified without duplicating their logic;
- CLI, Local Control API, adapter, and UI data flows are distinct;
- shared contract ownership is reserved for `GUI-CLI-SHARED-CONTRACTS-001`;
- read, planned, mutation, and agent operations have different gates;
- error, state, event, privacy, security, and compatibility rules are explicit;
- parallel visual ownership is clear;
- no frontend, API, dependency, runtime, or CLI behavior is changed;
- the next feature is `GUI-CLI-SHARED-CONTRACTS-001`.

## Local Validation Evidence

```text
quality gate guard-only: PASS
required_failures: 0
warnings: 0
skipped: 0
cargo fmt --all -- --check: PASS
git diff --check: PASS
git diff --cached --check: PASS
staged scope: two docs files only
dashboard application directory: absent
main.rs delta from base: 0
```

No full runtime suite was required for this checkpoint because the diff has no
Rust, CLI, API, frontend, dependency, or runtime changes. The existing runtime
baseline remains outside this feature's executable surface.

## QA Classification

Field QA is not required for this documentation-only architecture feature. No
executable surface, API, frontend application, or runtime behavior is changed.
