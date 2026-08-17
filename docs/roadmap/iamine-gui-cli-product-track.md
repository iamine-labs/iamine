# IAMINE GUI and CLI Product Track

## State

```text
track: IAMINE-GUI-CLI-PRODUCT-TRACK
state: PROPOSED
milestone placement: unresolved by design
implementation authorization: none; IAMINE-DASHBOARD-DESIGN-SYSTEM-001 is closed
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
| GUI-CLI-SHARED-CONTRACTS-001 | PROPOSED | Stable typed command, status, error, and event contracts. |
| NODE-LOCAL-CONTROL-API-CONTRACT-001 | PROPOSED | Shared contracts and explicit local threat model. |
| DASHBOARD-LOCAL-AUTHORIZATION-001 | PROPOSED | Local Control API contract and audit requirements. |
| NODE-SERVICE-LIFECYCLE-001 | PROPOSED | Stable node lifecycle and recovery semantics. |
| HEADLESS-NODE-MODE-001 | PROPOSED | Service lifecycle and CLI parity. |
| NODE-LOCAL-CONTROL-API-001 | PROPOSED | Contract, authorization, validation, and audit gates. |
| IAMINE-DASHBOARD-DESIGN-SYSTEM-001 | MERGED / VALIDATED / CLOSED | React, TypeScript, and Vite scaffold; official dark tokens and IAMINE assets; reusable primitives; strict npm lifecycle policy; responsive non-authoritative Overview visual preview. Merge `7bb7de8`; focused post-merge validation PASS on Mac. Real control integration remains blocked. |
| IAMINE-DASHBOARD-SHELL-001 | PROPOSED | Design system and typed mock adapters. |
| IAMINE-DASHBOARD-OVERVIEW-MOCK-001 | PROPOSED | Non-authoritative typed fixtures only; no node connection or fictitious endpoint. |
| IAMINE-DASHBOARD-OVERVIEW-READONLY-INTEGRATION-001 | PROPOSED | Authorized read-only API, shared contracts, local authorization, and audit evidence. |
| NODE-ONBOARDING-WIZARD-001 | PROPOSED | Stable setup, configuration, privacy, and rollback contracts. |
| NODE-RESOURCE-CONTROLS-001 | PROPOSED | Stable resource policy and bounded mutation. |
| DASHBOARD-DIAGNOSTICS-001 | PROPOSED | Privacy-safe diagnostics contract. |
| DASHBOARD-AGENT-CATALOG-001 | PROPOSED | Official agent registry and display contracts. |
| DASHBOARD-AGENT-PERMISSION-FLOW-001 | PROPOSED | Permission display, confirmation, denial, and audit. |
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
