# IAMINE Canonical Roadmap Reconciliation

## Feature

```text
IAMINE-CANONICAL-ROADMAP-RECONCILIATION-001
```

## Decision

```text
ACCEPTED WITH RECONCILIATION REQUIRED
```

The prior roadmap proposal remains strategically useful, but its repository
baseline, v0.11.2 progress, next feature, test baseline, v0.11.3 state, release
numbering, dashboard status, and Security/CI status were superseded.

## Baseline

```text
branch: origin/develop
commit: c836d5c8f18fd95967b0114fbc0bd185c59158de
tree: a351ba66c486975261ba1050f730a00ebe7f8aac
v0.11.2 executable rows: 15 of 19 CLOSED
last closed: AGENT-AUDIT-EVENT-ENFORCEMENT-001
next implementation feature: AGENT-EXECUTION-AUTHORIZATION-001
runtime tests: 103/103 PASS
agents tests: 109/109 PASS
```

## Decisions

1. Preserve the existing v0.12.0, v0.12.1, v0.12.2, and v0.13.0 numbering.
2. Keep v0.11.3 closed as a documentation and internal-contract milestone.
3. Keep all six official P0 skeletons non-executable and not user available.
4. Pause execution authorization until this docs-only feature closes.
5. Keep the three later runtime rows proposed and unauthorized.
6. Execute the v0.11.2 milestone QA gate only after all 19 rows close.
7. Register GUI/CLI and Security/CI as proposed parallel tracks.
8. Treat closed internal framework and template identifiers as dependencies,
   not as new public implementation rows.
9. Keep distinct internal and public assistant IDs only where their
   deliverables and maturity stages genuinely differ.
10. Redact infrastructure and personal identifiers from canonical docs.

## Runtime Boundary

This feature changes no Rust source, dependencies, workflow, runtime, agent,
package-load, dashboard, Local Control API, CI, model, scheduler, worker, P2P,
inference, or service behavior.

It authorizes no package loading, execution, functional agents, dependency
updates, workflow repair, dashboard code, or milestone renumbering.

## Operational Order

```text
IAMINE-CANONICAL-ROADMAP-RECONCILIATION-001
-> AGENT-EXECUTION-AUTHORIZATION-001
-> AGENT-PACKAGE-LOAD-EVIDENCE-INTEGRATION-001
-> AGENT-PACKAGE-LOADER-001
-> AGENT-RUNTIME-EXECUTOR-001
-> V0.11.2-AGENT-RUNTIME-BASELINE-MILESTONE-QA-001
-> NODE-DOCTOR-EVIDENCE-PROVIDER-001
-> NODE-DOCTOR-AGENT-001
```

Every arrow still requires the complete canonical lifecycle. Parallel track
registration does not authorize implementation.
