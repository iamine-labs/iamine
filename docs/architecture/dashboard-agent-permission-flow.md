# DASHBOARD-AGENT-PERMISSION-FLOW-001

## State

```text
feature: DASHBOARD-AGENT-PERMISSION-FLOW-001
state: LOCAL VALIDATION PASSED
development: IMPLEMENTATION COMPLETE
branch: codex/dashboard-agent-permission-flow-001
base: 9ba34dddc987d090e49dba02aaac788826a67186
base tree: 33e9daab8a52741b466528408298e76d2e00e1c9
target: develop
runtime behavior changed: no
field QA required: no; browser-only typed mock surface
```

## Goal

Add a feature-owned permission review preview linked from the existing Agent
Catalog. The page presents one deterministic request, its declared permission
surface, blocked boundaries, local preview decision, and bounded audit
projection without connecting to IAMINE authorization, audit, package, or
runtime owners.

The feature demonstrates the operator experience only. A confirmed preview is
not a local authorization decision, permission grant, execution permit,
session capability, audit record, package review, or agent-runtime authority.

## Route And Ownership

```text
/agents/:agentId/permissions                         typed preview route
dashboard/src/contracts/view-models/agentPermissionReview.ts
dashboard/src/mocks/agentPermissionReviewFixtures.ts
dashboard/src/mocks/agentPermissionReviewMockDataSource.ts
dashboard/src/features/agent-permission-review/      presentation owner
dashboard/src/features/agent-catalog/                navigation trigger only
dashboard/src/app/                                   route composition only
```

The permission review view model contains display-ready values and stable
presentation enums. It must not be reused as permission metadata, policy,
authorization, replay evidence, audit evidence, package state, or runtime
input.

## State Contract

```text
loading -> ready
loading -> empty
loading -> error -> retry -> loading
ready/pending -> confirmed-preview
ready/pending -> denied-preview
confirmed-preview | denied-preview -> reset -> pending
```

Confirmation requires an explicit local acknowledgement. Denial remains
available without acknowledgement. Both outcomes only update in-memory React
state and append a deterministic presentation event. Reloading restores the
fixture to `pending`.

## Audit Projection Boundary

Every displayed event is fixed presentation metadata with these invariants:

```text
persisted: false
emitted: false
containsPayload: false
authorizesAction: false
```

No wall-clock timestamp, host identifier, account, path, address, request ID,
session identity, credential, package value, prompt, or private payload is
displayed or generated.

## Core Alignment

The existing Rust owners remain authoritative:

- `iamine-core` owns Local Control API and dashboard-local authorization;
- `iamine-agents` owns permission and scope policy;
- `iamine-agent-runtime` owns agent execution authorization and lifecycle;
- the audit owner remains responsible for persistence and emission.

The frontend does not import, translate, recompute, or validate those owners'
policies. Fixture labels illustrate a reviewed presentation shape only. Unknown
agent IDs return a controlled empty state and never become permissive defaults.

## Safety Boundary

- No Rust crate, workspace manifest, node runtime, Local Control API, scheduler,
  P2P, model, inference, package, permission, audit, or execution source change.
- No HTTP, WebSocket, filesystem, shell, cookie, credential, browser storage,
  or service-worker use.
- No real confirmation, denial, authorization, audit persistence, dispatch, or
  agent execution.
- No install, enable, start, run, download, publish, or marketplace action.
- Mock provenance and non-authoritative outcome labels remain visible.
- The Reporter branch remains isolated in its current QA lifecycle.

## Acceptance Criteria

- every catalog row can open a deterministic permission preview by stable agent
  ID;
- unknown IDs render a controlled empty state;
- loading, ready, empty, error, retry, confirm, deny, and reset are tested;
- confirm is disabled until explicit acknowledgement;
- deny never requires acknowledgement and neither outcome authorizes action;
- the audit projection remains bounded, payload-free, and explicitly not
  persisted or emitted;
- browser reload returns the flow to pending fixture state;
- keyboard order, focus, text fit, and responsive layout pass at supported
  desktop and mobile viewports;
- there are no console errors, failed requests, Axe violations, or document
  overflow;
- core path diff is empty and the complete repository gate passes.

## Risks

| Risk | Control |
| --- | --- |
| Preview appears to grant a real permission | Persistent preview provenance and non-authorizing outcome copy. |
| Frontend duplicates core policy | Display-only fixture values; no policy computation or shared authority types. |
| Simulated audit appears durable | Fixed `not persisted` and `not emitted` facts on every outcome. |
| Unknown agents gain a permissive fallback | Exact fixture lookup and controlled empty state. |
| Decision state survives unexpectedly | In-memory state only; reload returns to pending. |
| New route breaks shell selection | Descendant route tests keep Agents selected and Overview unchanged. |
| Responsive review becomes card-heavy | Two bounded operational panels plus one unframed timeline, stacked on mobile. |
| Frontend damages core | Isolated worktree and mandatory pre-push core diff gate. |

## Validation Plan

```bash
cd dashboard
npm ci
npm run format:check
npm run lint
npm run typecheck
npm test -- --run
npm run build
npm run e2e
npm audit --audit-level=moderate
cd ..
./scripts/quality-gate.sh
git diff --check
```

TS140 and Proxmox Field QA are not required because this feature is a local
browser preview and does not connect to or change node behavior.

## Validation Outcome

```text
implementation commit: ab445b380bf5002b6bd8b5a95d5d032d1a278a9b
implementation tree: 08587a039ccd912ad9f5f6575053904c88be683d
frontend static, unit, build, audit, E2E, and visual checks: PASS
repository quality gate: PASS WITH WARNINGS
required failures: 0
new warnings: 0
core path diff: empty
field QA: not required
recommendation: READY FOR ARCHITECTURE MERGE REVIEW
```
