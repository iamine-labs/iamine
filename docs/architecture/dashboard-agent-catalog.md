# DASHBOARD-AGENT-CATALOG-001

## State

```text
feature: DASHBOARD-AGENT-CATALOG-001
state: MERGED / VALIDATED / CLOSED
development: CLOSED
branch: codex/dashboard-agent-catalog-001
base: 65f12dc3c7b6a67489fe54e691dd30778bd6a183
base tree: 604bc770eef3374eb34858019e586653e72956a9
target: develop
runtime behavior changed: no
field QA required: no; browser-only typed mock surface
```

## Goal

Promote the reserved Agents destination into a feature-owned, typed,
deterministic catalog preview. The page helps an operator scan official agent
roles, lifecycle labels, operating modes, and declared boundaries without
connecting to an IAMINE node, registry, runtime, or marketplace.

This feature is the visual-only phase of the canonical Agent Catalog row. It
does not claim that listed agents are installed, executable, available on the
local node, or authorized for an operator.

## Ownership

```text
dashboard/src/contracts/view-models/agentCatalog.ts  presentation contracts
dashboard/src/mocks/agentCatalogFixtures.ts          deterministic fixtures
dashboard/src/mocks/agentCatalogMockDataSource.ts    non-authoritative source
dashboard/src/features/agent-catalog/                page, filters, list, detail
dashboard/src/app/                                   route composition only
```

The view model contains display-ready values and stable presentation enums. It
must not be reused as an agent manifest, package, permission, scope, runtime,
authorization, audit, registry, or marketplace contract.

## State Contract

```text
loading -> ready
loading -> empty
loading -> error -> retry -> loading
ready -> filtered result
ready -> no matching result
ready -> selected local detail
```

Search, lifecycle filters, and row selection are in-memory presentation state.
They do not invoke commands, mutate packages, or create durable preferences.

## Safety Boundary

- No Rust crate, workspace manifest, node runtime, scheduler, P2P, model,
  inference, package, permission, or audit source changes.
- No HTTP, WebSocket, filesystem, shell, Local Control API, credential, or
  browser persistence use.
- Mock provenance remains visible and encoded as `authoritative: false`.
- No install, execute, enable, permission, download, or marketplace action.
- No status is a claim about the local machine or connected network.
- Reporter remains isolated on its own branch and in its canonical lifecycle.

Real agent data and actions remain blocked behind the canonical shared
contracts, Local Control API, authorization, audit, package loader, and runtime
execution gates.

## Acceptance Criteria

- `/agents` renders a feature-owned catalog instead of the reserved route;
- typed mock loading, ready, empty, error, retry, and no-match states are
  covered by automated tests;
- search and segmented lifecycle filters are keyboard accessible;
- selection exposes only bounded presentation metadata;
- preview provenance is visible and no fixture appears authoritative;
- mobile and desktop layouts preserve text fit and navigation behavior;
- no agent action or real node request is exposed;
- shell and Overview routes remain unchanged;
- core diff is empty and frontend validation passes.

## Risks

| Risk | Control |
| --- | --- |
| Preview looks like live node state | Visible preview provenance and `authoritative: false`. |
| UI duplicates agent policy | Presentation-only enums and no executable decisions. |
| Catalog page becomes monolithic | Separate contract, source, page, toolbar, list, and detail modules. |
| Inert controls imply real actions | Only search, filter, selection, and retry are interactive. |
| Responsive table becomes unusable | Stable desktop columns and a dedicated compact mobile list. |
| Frontend work damages core | Isolated worktree and mandatory pre-push core diff gate. |

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

Playwright screenshots and accessibility checks must cover desktop and mobile
ready states plus the reserved-route non-regression. No remote Field QA is
required because this feature is browser-only and contains no node connection.

## Validation Outcome

```text
implementation commit: 687e7240f9b0ec29f5254c83bb3a8f0995c80bbf
implementation tree: 711fcd623bead6710e2594bd456711e0b333cac6
frontend static, unit, build, audit, E2E, and visual checks: PASS
repository quality gate: PASS WITH WARNINGS
required failures: 0
core path diff: empty
field QA: not required
recommendation: READY FOR ARCHITECTURE MERGE REVIEW
```

## Controlled Merge And Closure

```text
target before merge: 65f12dc3c7b6a67489fe54e691dd30778bd6a183
source: origin/codex/dashboard-agent-catalog-001
source commit: 97d1cfe40762c163b78a858010954f5d418d6e43
source tree: 8a4297ade2737d12da697e5e4b2fce279ceafccb
merge commit: 45923de09a329220135b6bc54615e00ed235de48
merge tree: 8a4297ade2737d12da697e5e4b2fce279ceafccb
tree identity: PASS
merge conflicts: none
runtime behavior changed: no
field QA executed: no, not required for browser-only mock behavior
```

Post-merge frontend validation repeated reproducible installation, format,
lint, typecheck, 19 unit tests, production build, and all four Playwright
projects successfully. The repository quality gate passed with zero required
failures and zero new warnings; three unavailable optional tools remain
recorded as skipped.

This closes only the deterministic Agent Catalog preview. Real registry data,
node availability, agent permissions, installation, execution, and marketplace
behavior remain blocked by their owning canonical features.
