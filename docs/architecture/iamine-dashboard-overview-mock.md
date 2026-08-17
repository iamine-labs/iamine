# IAMINE-DASHBOARD-OVERVIEW-MOCK-001

## State

```text
feature: IAMINE-DASHBOARD-OVERVIEW-MOCK-001
state: READY FOR MERGE REVIEW
branch: feature/iamine-dashboard-overview-mock-001
base: 5e4e9f7914adfa5cae62edbd017892fe0e1d204c
target: develop
runtime behavior changed: no
field QA required: no; browser-only mock surface
```

## Goal

Promote the approved Overview composition from a shell-owned preview into a
feature-owned, typed, deterministic mock journey. The feature must demonstrate
loading, ready, empty, error, and retry behavior without connecting to an
IAMINE node or creating a fictitious endpoint.

## Ownership

```text
dashboard/src/contracts/view-models/  presentation-only Overview contracts
dashboard/src/mocks/                  deterministic non-authoritative data source
dashboard/src/features/overview/      Overview lifecycle, panels, and charts
dashboard/src/app/                    route composition only
```

`OverviewViewModel` contains values already formatted for display. It is not a
node API contract and must not be reused as an authority for scheduler,
runtime, model, permission, or audit behavior. `OverviewDataSource` is a narrow
presentation boundary whose only implementation in this feature has
`kind: mock` and returns local fixtures through a promise.

## State Contract

```text
loading -> ready
loading -> empty
loading -> error -> retry -> loading
```

- `ready` renders the approved Overview panels from one typed view model.
- `empty` reports that the local mock source returned no data.
- `error` emits a bounded message and never exposes rejected exception text.
- `retry` repeats only the mock load and does not rebuild or mutate the shell.
- stale loads are ignored after unmount or source replacement.

## Safety Boundary

- No Rust crate, workspace manifest, node runtime, scheduler, P2P, model, or
  inference source changes.
- No HTTP, WebSocket, filesystem, shell, Local Control API, or credential use.
- Mock provenance remains visible and is encoded as non-authoritative.
- Only navigation to the already reserved Nodes route is enabled.
- Actions without an implemented feature remain disabled.
- No mock value is a claim about the local machine or connected network.

Real node data remains blocked behind:

```text
GUI-CLI-SHARED-CONTRACTS-001
-> NODE-LOCAL-CONTROL-API-CONTRACT-001
-> DASHBOARD-LOCAL-AUTHORIZATION-001
-> NODE-LOCAL-CONTROL-API-001
-> IAMINE-DASHBOARD-OVERVIEW-READONLY-INTEGRATION-001
```

## Acceptance Criteria

- shell imports one `OverviewPage` feature boundary;
- no Overview fixture or lifecycle logic remains under `app/` or `preview/`;
- mock loading, ready, empty, error, and retry states have automated tests;
- source rejection details are not rendered;
- nonfunctional Overview controls are disabled;
- approved Nodes navigation remains functional;
- charts derive their presentation from the typed view model;
- keyboard, semantic, responsive, and structural accessibility checks pass;
- core diff is empty and repository quality gates pass.

## Risks

| Risk | Control |
| --- | --- |
| Mock values appear authoritative | Visible preview provenance and `authoritative: false`. |
| Frontend contract duplicates core behavior | View model contains presentation values only. |
| Fixture loading leaks into the shell | `OverviewPage` owns the data source and lifecycle. |
| Inert controls imply real operations | Unimplemented actions are disabled. |
| Overview becomes a monolith | Contracts, source, page, panels, and charts remain separate. |
| Frontend work damages core | Isolated worktree and mandatory pre-push core safety gate. |

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

The Mac terminal Node version mismatch remains an environment finding. It does
not authorize a dependency or canonical toolchain change in this feature.

## Architecture Checkpoint

```text
shell ownership: routing and composition only
overview ownership: dashboard/src/features/overview/
mock ownership: dashboard/src/mocks/
largest changed TypeScript module: 169 lines
largest changed CSS module: 499 lines
iamine-node/src/main.rs: 4935 -> 4935 lines
iamine-node/src/cluster_registry.rs: 862 -> 862 lines
Rust/core diff: empty
new dependency: none
real node connection: none
real node action: none
```

The checkpoint confirms that the mock feature is modular, non-authoritative,
and ready for merge review. It does not authorize the future read-only node
integration or any runtime mutation.
