# IAMINE GUI and CLI Product Track

## State

```text
track: IAMINE-GUI-CLI-PRODUCT-TRACK
state: PROPOSED
milestone placement: unresolved by design
implementation authorization: none
```

This track records the strategically accepted product direction without
renumbering existing milestones. IAMINE has one core with two interfaces:

- the local dashboard is the default interface for general users;
- the CLI remains the advanced, automation, QA, and headless interface.

Both interfaces must consume shared typed contracts. The frontend must not
duplicate IAMINE domain, validation, permission, audit, P2P, scheduler, model,
or execution logic.

## Candidate Features

| Feature | State | Dependency or boundary |
| --- | --- | --- |
| GUI-CLI-INTERFACE-ARCHITECTURE-001 | PROPOSED | Define interface ownership and shared-core boundaries. |
| GUI-CLI-SHARED-CONTRACTS-001 | PROPOSED | Stable typed command, status, error, and event contracts. |
| NODE-LOCAL-CONTROL-API-CONTRACT-001 | PROPOSED | Shared contracts and explicit local threat model. |
| DASHBOARD-LOCAL-AUTHORIZATION-001 | PROPOSED | Local Control API contract and audit requirements. |
| NODE-SERVICE-LIFECYCLE-001 | PROPOSED | Stable node lifecycle and recovery semantics. |
| HEADLESS-NODE-MODE-001 | PROPOSED | Service lifecycle and CLI parity. |
| NODE-LOCAL-CONTROL-API-001 | PROPOSED | Contract, authorization, validation, and audit gates. |
| IAMINE-DASHBOARD-DESIGN-SYSTEM-001 | PROPOSED | Typed mocks only until real control contracts close. |
| IAMINE-DASHBOARD-SHELL-001 | PROPOSED | Design system and typed mock adapters. |
| IAMINE-DASHBOARD-OVERVIEW-001 | PROPOSED | Read-only typed status mocks or authorized API. |
| NODE-ONBOARDING-WIZARD-001 | PROPOSED | Stable setup, configuration, privacy, and rollback contracts. |
| NODE-RESOURCE-CONTROLS-001 | PROPOSED | Stable resource policy and bounded mutation. |
| DASHBOARD-DIAGNOSTICS-001 | PROPOSED | Privacy-safe diagnostics contract. |
| DASHBOARD-AGENT-CATALOG-001 | PROPOSED | Official agent registry and display contracts. |
| DASHBOARD-AGENT-PERMISSION-FLOW-001 | PROPOSED | Permission display, confirmation, denial, and audit. |
| DASHBOARD-AGENT-EXECUTION-001 | PROPOSED | Functional runtime, loader, authorization, and executor. |
| LOCAL-DASHBOARD-BUNDLING-001 | PROPOSED | Stable shell, service lifecycle, and release packaging. |
| GUI-CLI-COMMAND-PARITY-001 | PROPOSED | Shared contracts and supported command inventory. |
| DASHBOARD-ACCESSIBILITY-001 | PROPOSED | Stable interactive dashboard surfaces. |
| DASHBOARD-RESPONSIVE-001 | PROPOSED | Stable dashboard layouts and supported viewports. |
| DASHBOARD-E2E-001 | PROPOSED | All promised dashboard journeys and failure states. |

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
```

Visual design, shell composition, and overview work may proceed with typed
mocks. Mocks must be visibly non-authoritative and must not create a path to
real node actions.

Milestone placement and closure gates require a later Architecture decision.
This track does not modify the current v0.11.2 sequence.
