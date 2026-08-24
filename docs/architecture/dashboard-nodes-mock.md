# DASHBOARD-NODES-MOCK-001

## State

```text
feature: DASHBOARD-NODES-MOCK-001
state: MERGED / VALIDATED / CLOSED
architecture: APPROVED
architecture review: PASSED
development: IMPLEMENTATION COMPLETE
local validation: PASSED
branch: codex/dashboard-nodes-mock-001
base: 6dcb75e718c53b79bb4e3c51478da027e293de43
base tree: 5721b91883cec7a81aa33d8c533dee6541d16e58
implementation commit: 2cc346898c732b3892d1db6b6a5dd2c8e2082ae3
implementation tree: 06af5a2296c669d22e625f0f932fe55fc436b130
target: develop
runtime behavior changed: no
field QA required: no; browser-only typed mock surface
```

## Goal

Replace the reserved Nodes destination with a feature-owned, typed,
deterministic preview. The page gives an operator a compact synthetic inventory
for testing list, filter, selection, and responsive-detail behavior without
discovering a device, connecting to a cluster, or claiming a real node state.

This microfeature does not implement `NODE-LOCAL-CONTROL-API-001`,
`IAMINE-DASHBOARD-OVERVIEW-READONLY-INTEGRATION-001`, node discovery, hardware
profiling, scheduler eligibility, worker lifecycle, or resource controls.

## Ownership

```text
dashboard/src/contracts/view-models/nodes.ts presentation contracts
dashboard/src/mocks/nodesFixtures.ts         synthetic fixtures
dashboard/src/mocks/nodesMockDataSource.ts   non-authoritative source
dashboard/src/features/nodes/                page, filters, list, detail
dashboard/src/app/                           route composition only
```

The view model contains display-ready labels and stable presentation enums. It
must not be reused as a hardware profile, cluster registry, capability,
scheduler, worker, network, authorization, audit, or Local Control API contract.

## State Contract

```text
loading -> ready
loading -> empty
loading -> error -> retry -> loading
ready -> filtered result
ready -> no matching result
ready -> selected local detail
```

Search, preview-status filtering, capability filtering, retry, and selection
are in-memory presentation state. They do not discover, connect, trust,
configure, start, stop, schedule, or persist a node.

## Presentation Contract

- Route: `/nodes`.
- Source kind: `mock` only.
- Status values: `available`, `limited`, and `offline`.
- Capability values: display-only labels owned by the fixture.
- Detail: bounded synthetic role, environment, capacity, and capability labels.
- Provenance: always visible and encoded as `authoritative: false`.
- Real discovery, connection, configuration, and lifecycle actions: absent.

## Privacy And Safety Boundary

- No Rust crate, workspace manifest, node runtime, cluster registry, scheduler,
  P2P, model, inference, profiler, worker, authorization, audit, or network
  source changes.
- No HTTP, WebSocket, filesystem, shell, Local Control API, credential, browser
  persistence, or telemetry use.
- Fixtures contain no IP, MAC, hostname, serial, disk identifier, personal path,
  machine fingerprint, credential, token, or real log content.
- Generic aliases such as `Preview Node A` are display labels, not identifiers.
- No synthetic status or capability is a claim about the Mac or an IAMINE node.
- No discover, add, connect, trust, configure, start, stop, drain, schedule,
  allocate, or remove action is exposed.

## Acceptance Criteria

- `/nodes` renders a feature-owned preview instead of the reserved route;
- loading, ready, empty, error, retry, and no-match states are tested;
- search and segmented preview-status filters are keyboard accessible;
- capability filtering and local selection expose only bounded mock metadata;
- preview provenance is visible in every ready-state layout;
- mobile and desktop layouts preserve navigation, text fit, and focus order;
- no request, persistence, real node action, or core change exists;
- existing routes remain functional and repository gates pass.

## Risks

| Risk | Control |
| --- | --- |
| Preview is mistaken for a live cluster | Persistent preview badge, provenance, and `authoritative: false`. |
| Fixture leaks a device identifier | Generic aliases and explicit forbidden fields. |
| Presentation enums become scheduler policy | Separate view-model namespace and no shared-core import. |
| Nodes page becomes monolithic | Separate contract, source, page, toolbar, list, and detail modules. |
| Real actions are implied before integration | No discovery, connection, lifecycle, or configuration command. |
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
unit coverage: 8 files / 39 tests
Playwright matrix: 8/8 tests across 4 projects
repository quality gate: PASS WITH ENVIRONMENTAL EXCEPTION RECOMMENDED
required failures in sandbox: 3 aggregate checks / 5 exact cases
exact failed cases repeated outside sandbox: 5/5 PASS
new warnings: 0
optional skipped: cargo-audit, cargo-deny, gitleaks unavailable
core path diff: empty
largest feature logic module: 191 lines
largest feature CSS module: 219 lines
field QA: not required
recommendation: READY FOR ARCHITECTURE MERGE REVIEW
```

The sandbox blocked four Metal inference assertions and one Unix socket test.
All five failed checks passed individually outside the sandbox against the same
implementation tree. The feature contains no Rust or workspace-manifest diff.

The Mac has Node `26.7.0`, outside the dashboard's declared Node `24.x` engine
range. npm `11.19.0` is supported and every required frontend check passed.

## Architecture Review

```text
reviewed commit: 3cca6795aa8ec45d0ac51745b86103fec39ab6fe
reviewed tree: 4057bfdd11080e92ff67836d3c2b35529be24bee
scope conformance: PASS
ownership and modularity: PASS
mock authority boundary: PASS
privacy and security boundary: PASS
responsive and accessibility evidence: PASS
core non-regression: PASS
environmental exception classification: ACCEPTED
blocking findings: 0
authorization: APPROVED FOR MERGE
```

The approval applies only to the deterministic browser preview. Real node
discovery, inventory, connectivity, hardware data, scheduler decisions,
resource controls, lifecycle actions, and Local Control API access remain
unavailable and owned by their future authorized features.

## Controlled Merge And Closure

```text
target before merge: 6dcb75e718c53b79bb4e3c51478da027e293de43
source: origin/codex/dashboard-nodes-mock-001
source commit: 84471bbef5d4ab2342515940be1fe7cae625ae27
source tree: c276776cc6f8fc030b6668e2c9271d3ebdf2188f
precomputed merge tree: c276776cc6f8fc030b6668e2c9271d3ebdf2188f
merge commit: b58a1a81e1a1afe8e759c95853d26f992dbb196e
merge tree: c276776cc6f8fc030b6668e2c9271d3ebdf2188f
tree identity: PASS
merge conflicts: none
runtime behavior changed: no
field QA executed: no, not required for browser-only mock behavior
post-merge validation: PASS WITH ACCEPTED ENVIRONMENTAL EXCEPTION
```

Post-merge installation, static checks, 39 unit tests, production build, npm
audit, and all eight Playwright cases passed. Rust formatting, 163 network unit
tests, four routing tests, node build, clippy, repository guards, and core diff
also passed. Clippy warnings are historical and outside this frontend diff.

The complete source gate and its five exact out-of-sandbox environmental
repetitions apply to the merge because source and merge trees are identical.
The environment-sensitive suite was not redundantly repeated after the merge;
no Rust or workspace-manifest content changed.

This closes only the deterministic Nodes preview. Real inventory, discovery,
connectivity, hardware facts, scheduling, lifecycle, configuration, and Local
Control API access remain unavailable.
