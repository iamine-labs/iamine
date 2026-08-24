# DASHBOARD-ACTIVITY-MOCK-001

## State

```text
feature: DASHBOARD-ACTIVITY-MOCK-001
state: ARCHITECTURE REVIEW REQUIRED
architecture: REVIEW REQUIRED
development: IMPLEMENTATION COMPLETE
branch: codex/dashboard-activity-mock-001
base: 6e66e2f3c4478367e9bc5fb27d4dfa04d26e4f76
base tree: a19388cda194b1f4b951299413e3d0d1eb1f7349
implementation commit: 79d5062ab4a2351c8d2606ee0edc6b711e829700
implementation tree: 64ee74c26eb818c39cce01c9fe7f5e23193c21bd
target: develop
runtime behavior changed: no
field QA required: no; browser-only typed mock surface
```

## Goal

Replace the reserved Activity destination with a feature-owned, typed,
deterministic preview. The page gives an operator a synthetic activity list for
testing search, preview-signal and category filtering, local selection,
responsive detail, and failure states without reading logs, audit records,
runtime events, task traces, or network traffic.

This microfeature does not implement the future bounded event stream, event
reconciliation, audit evidence, task history, log ingestion, export, telemetry,
notification delivery, or operational actions.

## Ownership

```text
dashboard/src/contracts/view-models/activity.ts presentation contracts
dashboard/src/mocks/activityFixtures.ts         synthetic fixtures
dashboard/src/mocks/activityMockDataSource.ts   non-authoritative source
dashboard/src/features/activity/                page, filters, list, detail
dashboard/src/app/                               route composition only
```

The view model contains display-ready labels and stable presentation enums. It
must not be reused as a runtime event, audit event, task trace, log schema,
notification, authorization, P2P, or Local Control API contract.

## State Contract

```text
loading -> ready
loading -> empty
loading -> error -> retry -> loading
ready -> filtered result
ready -> no matching result
ready -> selected local detail
```

Search, preview-signal filtering, category filtering, retry, and selection are
in-memory presentation state. They do not tail, subscribe, acknowledge,
approve, retry, replay, export, or persist an event.

## Presentation Contract

- Route: `/activity`.
- Source kind: `mock` only.
- Identities: generic `Preview Event A` through `Preview Event F`.
- Order labels: generic `Moment A` through `Moment F`; no real timestamps.
- Signals: `informational`, `attention`, and `boundary` presentation labels.
- Categories: display-only labels owned by the fixture.
- Detail: bounded synthetic summary, labels, provenance, and fixture notes.
- Provenance: always visible and encoded as `authoritative: false`.
- Real event, audit, task, log, notification, and export actions: absent.

## Privacy And Safety Boundary

- No Rust crate, workspace manifest, runtime event, audit, task trace, log,
  notification, node, model, agent, scheduler, P2P, or network source changes.
- No HTTP, WebSocket, filesystem, shell, Local Control API, credential, browser
  persistence, or telemetry use.
- Fixtures contain no timestamp, prompt, output, task ID, trace ID, request ID,
  peer ID, IP, MAC, hostname, username, path, model ID, agent package ID,
  credential, token, backend message, or real log content.
- Generic event and moment aliases are presentation labels, not identifiers or
  chronological claims.
- No synthetic signal or category is audit evidence or runtime authority.
- No acknowledge, approve, deny, retry task, replay, export, clear, delete, or
  open-log action is exposed.

## Acceptance Criteria

- `/activity` renders a feature-owned preview instead of the reserved route;
- loading, ready, empty, error, retry, and no-match states are tested;
- search and segmented preview-signal filters are keyboard accessible;
- category filtering and local selection expose bounded synthetic metadata;
- preview provenance is visible in every ready-state layout;
- mobile and desktop layouts preserve navigation, text fit, and focus order;
- no request, persistence, real event action, or core change exists;
- existing routes remain functional and repository gates pass.

## Risks

| Risk | Control |
| --- | --- |
| Preview is mistaken for audit evidence | Persistent preview badge, provenance, and `authoritative: false`. |
| Fixture leaks operational or personal data | Generic aliases and an explicit forbidden-field list. |
| List order implies real chronology | `Moment A-F` presentation labels and no timestamps. |
| Presentation enums become runtime policy | Separate view-model namespace and no shared-core import. |
| Activity page becomes monolithic | Separate contract, source, page, toolbar, list, and detail modules. |
| Frontend work damages core | Isolated worktree and mandatory core diff gate before push. |

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

Playwright and accessibility checks cover desktop and mobile ready states,
navigation, filtering, provenance, overflow, and route non-regression. Remote
Field QA is not required because the feature is browser-only and has no event,
audit, log, node, or service connection.

## Development And QA Handoff

```text
implementation: COMPLETE
local frontend validation: PASS
repository gate: PASS WITH ENVIRONMENTAL EXCEPTIONS
exact sandbox failures repeated outside sandbox: 5/5 PASS
core diff: empty
blocking findings: 0
recommendation: READY FOR ARCHITECTURE MERGE REVIEW
```

The implementation and QA evidence are recorded in
`docs/qa/dashboard-activity-mock.md`. Architecture must review the complete
feature diff and explicitly accept or reject the environmental exception before
merge authorization.
