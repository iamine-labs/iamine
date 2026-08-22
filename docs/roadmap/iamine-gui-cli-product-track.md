# IAMINE GUI and CLI Product Track

## State

```text
track: IAMINE-GUI-CLI-PRODUCT-TRACK
state: PROPOSED
milestone placement: unresolved by design
implementation authorization: none beyond closed rows
next real-integration candidate: NODE-LOCAL-CONTROL-API-001 (PROPOSED)
parallel implementation boundary: typed visual mocks only
```

This track records the strategically accepted product direction without
renumbering existing milestones. IAMINE has one core with two interfaces:

- the local dashboard is the default interface for general users;
- the CLI remains the advanced, automation, QA, and headless interface.

Both interfaces must consume shared typed contracts. The frontend must not
duplicate IAMINE domain, validation, permission, audit, P2P, scheduler, model,
or execution logic.

The preflight selected the frontend technology, repository layout, dependency
policy, supported targets, validation commands, packaging constraints, and
future desktop/mobile reuse. The design-system feature now owns the first
canonical application scaffold and reusable presentation primitives. It
remains a non-authoritative mock surface without node connectivity.

## Candidate Features

| Feature | State | Dependency or boundary |
| --- | --- | --- |
| DASHBOARD-FRONTEND-PREFLIGHT-001 | CLOSED | Selected the canonical React, TypeScript, Vite, npm, validation, layout, contract, packaging, and target strategy without creating the frontend; merge `396389f`, focused post-merge validation PASS with accepted baseline/environment exceptions. |
| GUI-CLI-INTERFACE-ARCHITECTURE-001 | CLOSED | Defined interface ownership and shared-core boundaries; merge `ff87b25`, focused post-merge validation PASS, no field QA required. |
| GUI-CLI-SHARED-CONTRACTS-001 | CLOSED | Shared typed contracts merged as `ee488b0`; 1,148 workspace tests and architecture guards PASS, no runtime or dashboard integration. |
| NODE-LOCAL-CONTROL-API-CONTRACT-001 | MERGED / VALIDATED / CLOSED | Loopback-only HTTP profile, strict shared-contract envelopes, bounded ingress/response validation, non-authorizing local authorization/replay/audit handoffs, and explicit local threat model; merge `4bb90fd`, 1,157 workspace tests and post-merge quality gate PASS, no server or dashboard integration. |
| NODE-LOCAL-CONTROL-API-CATALOG-001 | MERGED / VALIDATED / CLOSED | Documented the single contracted endpoint, 17 logical operations, payload ownership, stable envelopes, and non-authorizing authorization/replay/audit handoffs; commit `42f0dcd`, merge `0ecf6d1`, exact-merge quality gate PASS WITH WARNINGS with zero required failures. No HTTP server or owner dispatch was added. |
| DASHBOARD-LOCAL-AUTHORIZATION-001 | MERGED / VALIDATED / CLOSED | Opaque operator-local sessions, explicit decisions, bounded replay evidence, denial semantics, and attached audit handoffs in `iamine-core`; merge `ee0f074b`, post-merge quality gate PASS WITH WARNINGS with zero required failures, no server or dashboard connectivity. |
| NODE-SERVICE-LIFECYCLE-001 | PROPOSED | Stable node lifecycle and recovery semantics. |
| HEADLESS-NODE-MODE-001 | PROPOSED | Service lifecycle and CLI parity. |
| NODE-LOCAL-CONTROL-API-001 | PROPOSED | Contract, authorization, validation, and audit gates. |
| IAMINE-DASHBOARD-DESIGN-SYSTEM-001 | MERGED / VALIDATED / CLOSED | React, TypeScript, and Vite scaffold; official dark tokens and IAMINE assets; reusable primitives; strict npm lifecycle policy; responsive non-authoritative Overview visual preview. Merge `7bb7de8`; focused post-merge validation PASS on Mac. Real control integration remains blocked. |
| IAMINE-DASHBOARD-SHELL-001 | MERGED / VALIDATED / CLOSED | Routed static application shell, navigation lifecycle, inert reserved destinations, and top-level failure boundaries; merge `5c05e65`, post-merge frontend validation PASS, no core or real node behavior changed. |
| IAMINE-DASHBOARD-OVERVIEW-MOCK-001 | MERGED / VALIDATED / CLOSED | Feature-owned typed presentation contract, deterministic non-authoritative source, and loading/ready/empty/error states; merge `f62db25`, post-merge frontend validation PASS, no core or real node behavior changed. |
| IAMINE-DASHBOARD-OVERVIEW-READONLY-INTEGRATION-001 | PROPOSED | Authorized read-only API, shared contracts, local authorization, and audit evidence. |
| NODE-ONBOARDING-WIZARD-001 | PROPOSED | Stable setup, configuration, privacy, and rollback contracts. |
| NODE-RESOURCE-CONTROLS-001 | PROPOSED | Stable resource policy and bounded mutation. |
| DASHBOARD-DIAGNOSTICS-001 | PROPOSED | Privacy-safe diagnostics contract. |
| DASHBOARD-AGENT-CATALOG-001 | MERGED / VALIDATED / CLOSED | Typed, non-authoritative Agent Catalog preview with bounded search, stage filters, local detail selection, complete UI states, and Mac multibrowser QA; merge `45923de`, post-merge frontend and repository gates PASS, no core or real agent behavior changed. |
| DASHBOARD-AGENT-PERMISSION-FLOW-001 | ARCHITECTURE APPROVED / DEVELOPMENT AUTHORIZED | Typed, non-authoritative permission review preview with local confirmation, denial, reset, and non-persisted audit projection; real authorization, audit emission, package mutation, and execution remain blocked. |
| DASHBOARD-AGENT-EXECUTION-001 | PROPOSED | Functional runtime, loader, authorization, and executor. |
| LOCAL-DASHBOARD-BUNDLING-001 | PROPOSED | Stable shell, service lifecycle, and release packaging. |
| GUI-CLI-COMMAND-PARITY-001 | PROPOSED | Shared contracts and supported command inventory. |
| DASHBOARD-E2E-001 | PROPOSED | All promised dashboard journeys and failure states. |

Accessibility and responsive behavior are continuous acceptance gates, not
one-time feature rows. Every visual feature must define supported viewports,
keyboard and focus behavior, semantic labels, contrast, text-fit behavior,
loading/empty/error states, and automated or manual evidence appropriate to
its surface.

## Parallel Visual Sequence

The following sequence may proceed independently from the sequential agent
runtime track:

```text
DASHBOARD-FRONTEND-PREFLIGHT-001
-> GUI-CLI-INTERFACE-ARCHITECTURE-001
-> IAMINE-DASHBOARD-DESIGN-SYSTEM-001
-> IAMINE-DASHBOARD-SHELL-001
-> IAMINE-DASHBOARD-OVERVIEW-MOCK-001
```

After the preflight and interface architecture freeze ownership and typed mock
boundaries, Design System, Shell, and Overview work may use separate branches
or worktrees with non-overlapping file ownership. Each feature still requires
its own lifecycle and validation evidence.

Real integration remains sequential:

```text
GUI-CLI-SHARED-CONTRACTS-001
-> NODE-LOCAL-CONTROL-API-CONTRACT-001
-> DASHBOARD-LOCAL-AUTHORIZATION-001
-> NODE-LOCAL-CONTROL-API-001
-> IAMINE-DASHBOARD-OVERVIEW-READONLY-INTEGRATION-001
```

## Non-Bypass Rules

```text
localhost-only by default
no default 0.0.0.0 bind
no direct frontend access to P2P
no business logic in frontend code
no dynamic shell construction
no duplicated permission or audit validation
no credentials in frontend bundles
no remote dashboard before security review
no real mutation before authorization and audit close
no claimed production integration from mock evidence
```

Visual design, shell composition, and overview work may proceed with typed
mocks. Mocks must be visibly non-authoritative and must not create a path to
real node actions.

The parallel visual scope may include brand assets, tokens, components, shell
composition, typed fixtures, loading/empty/error states, visual QA, responsive
layouts, and accessibility foundations. It must not include real node data,
real logs, node mutation, agent execution, resource configuration, service
lifecycle, local or remote API access, dashboard bundling, or direct P2P
access.

Milestone placement and closure gates require a later Architecture decision.
This track does not modify the current v0.11.2 sequence.
