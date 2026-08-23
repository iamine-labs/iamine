# DASHBOARD-DIAGNOSTICS-MOCK-001

## State

```text
feature: DASHBOARD-DIAGNOSTICS-MOCK-001
state: APPROVED FOR MERGE
architecture: APPROVED
architecture review: PASSED
development: IMPLEMENTATION COMPLETE
local validation: PASSED
branch: codex/dashboard-diagnostics-mock-001
base: f9a51eff5008755978ad71c2077ab14d829cb34e
base tree: 772d99dfbecd1e415e781edb4ede5ec1db3a2c91
implementation commit: 57497f3e170a82dbad560a14e2e81d740335db0a
implementation tree: 6534971e9edc2ab7f8bce028e9fb0c6f1b9b3614
target: develop
runtime behavior changed: no
field QA required: no; browser-only typed mock surface
```

## Goal

Replace the reserved Diagnostics destination with a feature-owned, typed,
deterministic preview. The page gives an operator a compact way to inspect
synthetic health categories, filter results, and review privacy-safe detail
without connecting to a node, reading the Mac, or claiming a real diagnostic
result.

This microfeature does not implement `DASHBOARD-DIAGNOSTICS-001`. The latter
retains ownership of the future privacy-safe diagnostic contract, authorized
Local Control API integration, and real evidence semantics.

## Ownership

```text
dashboard/src/contracts/view-models/diagnostics.ts presentation contracts
dashboard/src/mocks/diagnosticsFixtures.ts         synthetic fixtures
dashboard/src/mocks/diagnosticsMockDataSource.ts   non-authoritative source
dashboard/src/features/diagnostics/                page, filters, list, detail
dashboard/src/app/                                 route composition only
```

The view model contains display-ready labels and stable presentation enums. It
must not be reused as a node-health, hardware, networking, model, security,
authorization, audit, runtime, or Local Control API contract.

## State Contract

```text
loading -> ready
loading -> empty
loading -> error -> retry -> loading
ready -> filtered result
ready -> no matching result
ready -> selected local detail
```

Search, status filtering, retry, and selection are in-memory presentation
state. They do not execute a diagnostic, export evidence, mutate a node, or
create durable preferences.

## Presentation Contract

- Route: `/diagnostics`.
- Source kind: `mock` only.
- Status values: `healthy`, `attention`, and `unavailable`.
- Categories: display-only labels owned by the fixture.
- Detail: bounded synthetic code, summary, observation, and suggested next
  step; no logs or identifiers.
- Provenance: always visible and encoded as `authoritative: false`.
- Real run and export actions: absent from the interactive contract.

## Safety Boundary

- No Rust crate, workspace manifest, node runtime, scheduler, P2P, model,
  inference, hardware profiler, authorization, audit, or networking changes.
- No HTTP, WebSocket, filesystem, shell, Local Control API, credential,
  browser persistence, or telemetry use.
- Fixtures contain no IP, MAC, hostname, serial, disk identifier, personal
  path, prompt, credential, token, or real log content.
- No synthetic status is a claim about the local Mac or an IAMINE node.
- No run, repair, export, copy-evidence, or node mutation action is exposed.

## Acceptance Criteria

- `/diagnostics` renders a feature-owned preview instead of the reserved route;
- loading, ready, empty, error, retry, and no-match states are tested;
- search and segmented status filters are keyboard accessible;
- local selection exposes only bounded synthetic detail;
- preview provenance is visible in every ready-state layout;
- mobile and desktop layouts preserve navigation, text fit, and focus order;
- no request, persistence, real diagnostic action, or core change exists;
- existing routes remain unchanged and repository gates pass.

## Risks

| Risk | Control |
| --- | --- |
| Preview is mistaken for live health | Persistent preview badge, provenance, and `authoritative: false`. |
| Fixture leaks machine evidence | Synthetic bounded values and explicit forbidden fields. |
| Presentation enums become backend policy | Separate view-model namespace and no shared-core import. |
| Diagnostics page becomes monolithic | Separate contract, source, page, toolbar, list, and detail modules. |
| Real actions are implied before integration | No run, repair, or export command in the interactive surface. |
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
Field QA is not required because the feature is browser-only and has no node
connection.

## Validation Outcome

```text
frontend format, lint, typecheck, unit, build, audit, and E2E: PASS
unit coverage: 7 files / 33 tests
Playwright matrix: 4/4 projects
repository quality gate: PASS WITH WARNINGS
required failures: 0
new warnings: 0
optional skipped: cargo-audit, cargo-deny, gitleaks unavailable
core path diff: empty
largest feature logic module: 179 lines
largest feature CSS module: 219 lines
field QA: not required
recommendation: READY FOR ARCHITECTURE MERGE REVIEW
```

The Node `26.7.0` runtime available on the Mac is outside the dashboard's
declared Node `24.x` engine range. npm `11.19.0` is supported, all required
frontend checks pass, and this mismatch remains an environmental warning for
future reproducibility with the pinned Node line.

## Architecture Review

```text
reviewed commit: 40fa3aee0f8264d18288feb0c31a258d6a258962
reviewed tree: 41604505953174dfb7abd93f203e5ceec22e0e76
scope conformance: PASS
ownership and modularity: PASS
mock authority boundary: PASS
privacy and security boundary: PASS
core non-regression: PASS
blocking findings: 0
authorization: APPROVED FOR MERGE
```

The approval applies only to the deterministic browser preview. Real
diagnostic evidence, Local Control API access, node inspection, repair, and
export remain owned by future authorized features.
