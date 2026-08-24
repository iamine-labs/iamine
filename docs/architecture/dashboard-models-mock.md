# DASHBOARD-MODELS-MOCK-001

## State

```text
feature: DASHBOARD-MODELS-MOCK-001
state: ARCHITECTURE REVIEW REQUIRED
architecture: APPROVED
architecture review: REQUIRED
development: IMPLEMENTATION COMPLETE
local validation: PASSED
branch: codex/dashboard-models-mock-001
base: f5978c185ca766c9a47f485f450435c9364846d3
base tree: d4380eaed21504c3c94039bc78b9530b85fd72e7
implementation commit: 82d5dcb39542291407109ebdacd3539caad02477
implementation tree: 90d4bd224452d5d1f9aa1870d4a3a065f6b85330
target: develop
runtime behavior changed: no
field QA required: no; browser-only typed mock surface
```

## Goal

Replace the reserved Models destination with a feature-owned, typed,
deterministic preview. The page gives an operator a synthetic library for
testing search, preview-state filtering, local selection, responsive detail,
and failure states without reading a model registry or claiming that an
artifact is installed, compatible, licensed, available, or executable.

This microfeature does not implement a real model catalog, model discovery,
download, installation, storage, verification, license acceptance, backend
selection, hardware compatibility, routing, inference, or execution.

## Ownership

```text
dashboard/src/contracts/view-models/models.ts presentation contracts
dashboard/src/mocks/modelsFixtures.ts         synthetic fixtures
dashboard/src/mocks/modelsMockDataSource.ts   non-authoritative source
dashboard/src/features/models/                page, filters, list, detail
dashboard/src/app/                            route composition only
```

The view model contains display-ready labels and stable presentation enums. It
must not be reused as an `iamine-models`, model registry, artifact, backend,
compatibility, scheduler, routing, inference, authorization, audit, or Local
Control API contract.

## State Contract

```text
loading -> ready
loading -> empty
loading -> error -> retry -> loading
ready -> filtered result
ready -> no matching result
ready -> selected local detail
```

Search, preview-state filtering, retry, and selection are in-memory
presentation state. They do not discover, download, install, validate, select,
load, run, remove, or persist a model.

## Presentation Contract

- Route: `/models`.
- Source kind: `mock` only.
- Preview states: `shown`, `attention`, and `unavailable`.
- Categories: display-only labels owned by the fixture.
- Identities: generic `Preview Model` aliases only.
- Artifact, license, compatibility, and backend facts: not represented.
- Detail: bounded synthetic description, category, preview state, and labels.
- Provenance: always visible and encoded as `authoritative: false`.
- Real model and artifact actions: absent.

## Privacy And Safety Boundary

- No Rust crate, workspace manifest, model registry, model storage, backend,
  inference, node runtime, scheduler, P2P, authorization, audit, or network
  source changes.
- No HTTP, WebSocket, filesystem, shell, Local Control API, credential, browser
  persistence, or telemetry use.
- Fixtures contain no real model identity, commercial name, artifact path,
  filename, size, format, checksum, license, backend, compatibility claim,
  credential, token, device identifier, or real log content.
- Generic aliases such as `Preview Model A` are display labels, not model IDs.
- No synthetic state or category is a claim about the Mac or an IAMINE node.
- No download, install, accept, activate, select, load, run, remove, or route
  action is exposed.

## Acceptance Criteria

- `/models` renders a feature-owned preview instead of the reserved route;
- loading, ready, empty, error, retry, and no-match states are tested;
- search and segmented preview-state filters are keyboard accessible;
- local selection exposes only bounded synthetic metadata;
- preview provenance is visible in every ready-state layout;
- mobile and desktop layouts preserve navigation, text fit, and focus order;
- no request, persistence, real model action, or core change exists;
- existing routes remain functional and repository gates pass.

## Risks

| Risk | Control |
| --- | --- |
| Preview is mistaken for a real library | Persistent preview badge, provenance, and `authoritative: false`. |
| A fixture implies an install or compatibility fact | Generic identities and explicit excluded fields. |
| Presentation enums become model policy | Separate view-model namespace and no shared-core import. |
| Models page becomes monolithic | Separate contract, source, page, toolbar, list, and detail modules. |
| Real actions are implied before integration | No artifact, lifecycle, selection, backend, or execution command. |
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
Field QA is not required because the feature is browser-only and has no model,
node, or service connection.

## Validation Outcome

```text
frontend format, lint, typecheck, unit, build, audit, and E2E: PASS
unit coverage: 9 files / 45 tests
Playwright matrix: 12/12 tests across 4 projects
repository quality gate: PASS WITH ENVIRONMENTAL EXCEPTION RECOMMENDED
required failures in sandbox: 3 aggregate checks / 5 exact cases
exact failed cases repeated outside sandbox: 5/5 PASS
new warnings: 0
optional skipped: cargo-audit, cargo-deny, gitleaks unavailable
core path diff: empty
largest feature logic module: 188 lines
largest feature CSS module: 219 lines
field QA: not required
recommendation: READY FOR ARCHITECTURE MERGE REVIEW
```

The sandbox blocked four Metal inference assertions and one Unix socket test.
All five exact cases passed individually outside the sandbox against the same
implementation tree. The feature contains no Rust or workspace-manifest diff.

The first Models E2E run exposed an ambiguous `Review` selector in the new test.
The selector was made exact and the full 12-case browser matrix then passed.
No product behavior failed.
