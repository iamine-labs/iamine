# IAMINE-DASHBOARD-DESIGN-SYSTEM-001

## Status

```text
feature: IAMINE-DASHBOARD-DESIGN-SYSTEM-001
state: MERGED / VALIDATED / CLOSED
base: origin/develop at 1409b6fa9cb780d00fb840503c16f83bd35c0405
branch: feature/iamine-dashboard-design-system-001
runtime behavior change: none
real node integration: none
field QA required: no; mock-only browser surface
```

## Purpose

Create the first canonical dashboard application and a reusable visual system
for later IAMINE dashboard features. This feature implements presentation
foundations only. It does not define transport contracts, connect to a node, or
claim production data integration.

## Selected Toolchain

The scaffold implements the stack selected by
`DASHBOARD-FRONTEND-PREFLIGHT-001`:

| Layer | Implementation |
| --- | --- |
| Runtime | Node.js 24.18.0 and npm 11.16.0 |
| Application | React 19.2.8 and TypeScript 6.0.3 in strict mode |
| Build | Vite 8.2.1 |
| Styling | CSS Modules and IAMINE-owned CSS custom properties |
| Icons | `lucide-react` |
| Unit tests | Vitest and Testing Library |
| Accessibility | semantic assertions and `axe-core` |
| Browser E2E | Playwright |
| Static checks | ESLint and Prettier |

Versions are exact in `dashboard/package.json` and resolved in the committed
`package-lock.json`. The repository pins Node through `.node-version` and npm
through the `packageManager` and `engines` fields.

## Ownership

```text
dashboard/src/components/  reusable presentation primitives
dashboard/src/styles/      reset, typography, layout, and semantic tokens
dashboard/src/preview/     deterministic fixtures and preview composition
dashboard/src/test/        shared unit-test setup
dashboard/tests/e2e/       browser acceptance evidence
dashboard/public/assets/   reviewed IAMINE-owned local brand assets
```

The application entry point only mounts the official Overview visual preview.
Its shell, navigation, charts, fixture panels, and status bar are local React
state. Components remain independent of IAMINE runtime policy and transport
types.

## Component Surface

The initial component set provides:

- brand mark and wordmark;
- primary, secondary, ghost, and danger buttons;
- icon buttons with accessible names and tooltips;
- semantic status badges;
- segmented controls and toggles;
- labeled text fields with independent descriptions and errors;
- determinate progress;
- responsive data tables with a keyboard-focusable scroll region;
- loading, empty, unavailable, and error state panels.
- 72px desktop sidebar, 64px top navigation, and 24px content spacing;
- responsive Overview composition for operations, resources, active agent,
  inference queue, nodes, traffic, inference totals, activity, and logs;
- local-only navigation placeholders that cannot imply real integration.

Every reusable component accepts presentation data only. No component decides
node readiness, eligibility, authorization, scheduler policy, model policy, or
agent execution behavior.

## Visual Contract

The project-owner-provided dashboard references are the visual source of truth.
The implementation uses the documented official tokens:

```text
canvas: #060f14
surface: #11161d
copper/action: #ff8a00
healthy: #22c55e
information: #3a8aff
agent: #8b5cf6
error: #ef4444
```

The design system is intended for a quiet operational interface:

- dark neutral canvas and surfaces with distinct copper, green, blue, purple,
  and red semantic roles;
- no CSS gradient decoration, remote fonts, or remote images;
- card radii no greater than 8px;
- stable responsive dimensions without viewport-scaled typography;
- visible keyboard focus and semantic landmarks;
- contrast and accessibility checks at all supported browser viewports;
- explicit loading, empty, unavailable, and error examples.

The preview identifies itself as `Preview data` at every supported viewport.
Fixtures are deterministic and cannot invoke real node actions.

## Official Assets

Two project-owner-provided assets are copied byte-for-byte into the dashboard:

| Asset | SHA-256 | Purpose |
| --- | --- | --- |
| `iamine-mark.png` | `a12514d84a44a9783b1cf1b1caebff424fc23db67989089bd8d90634b6b56bd1` | Sidebar and mobile brand mark |
| `iamine-network-wallpaper.png` | `32cf7bf5ada4689a6ee8fca206357dd4eb2a904ae1c962039961e6f8d72271ac` | Operational network preview panel |

The social banner, profile compositions, lab badge, and specification sheets
are not shipped at runtime. They are marketing or design-reference material,
not dashboard primitives.

## Security And Integration Boundary

The frontend contains no use of:

```text
fetch or WebSocket
browser local or session storage
dynamic HTML insertion
filesystem or shell access
direct P2P access
credentials or secrets
remote runtime assets
```

Dependency lifecycle scripts are denied by default with npm's strict script
allowlist. `fsevents` is explicitly denied because it is optional on macOS and
is not required by the dashboard.

Real node data remains blocked until the reviewed sequence closes:

```text
GUI-CLI-SHARED-CONTRACTS-001
-> NODE-LOCAL-CONTROL-API-CONTRACT-001
-> DASHBOARD-LOCAL-AUTHORIZATION-001
-> NODE-LOCAL-CONTROL-API-001
-> IAMINE-DASHBOARD-OVERVIEW-READONLY-INTEGRATION-001
```

This feature renders the intended shell appearance only to validate the design
system. It does not close `IAMINE-DASHBOARD-SHELL-001`. That feature still owns
production composition, route semantics, error boundaries, and shell lifecycle
without introducing a fictitious endpoint.

## Architecture Maintenance

```text
iamine-node/src/main.rs: 4935 -> 4935 lines
iamine-node/src/cluster_registry.rs: 862 -> 862 lines
dashboard/src/App.tsx: 5 lines
largest preview TypeScript module: 196 lines
largest preview CSS module: 494 lines
```

No Rust module grew and no runtime ownership moved. Navigation, summary panels,
telemetry panels, charts, fixtures, and shell composition are separate modules.
The shell feature can replace preview wiring without carrying fixture policy
into production routes.

## Known Limits

- This is a design-system preview, not an operational dashboard.
- No real IAMINE contracts, routes, authorization, audit, or node actions exist.
- Browser evidence was collected on the development Mac only.
- Remote field QA is deferred until a feature changes runtime or adds real node
  integration.

## Controlled Merge And Closure

```text
target before merge: 1409b6fa9cb780d00fb840503c16f83bd35c0405
source: origin/feature/iamine-dashboard-design-system-001
source commit: 677901ae427912224395354c8d6e4c57e1961878
merge commit: 7bb7de8c6d9464482fd863d5fdeee00c8207275a
merge tree: 9ee0a0feebdf286a3e05e8c948d1d88c9805d9b3
merge conflicts: none
runtime behavior changed: no
field QA executed: no, not required for the mock-only browser surface
```

The merge tree is byte-identical to the reviewed source tree because
`origin/develop` had not moved from the feature base.

Focused post-merge validation:

```text
npm ci: PASS, 245 packages from lockfile
npm run format:check: PASS
npm run lint: PASS
npm run typecheck: PASS
npm test -- --run: PASS, 3 files / 5 tests
npm run build: PASS
npm run e2e: PASS, 4 Playwright projects
source quality gate on the identical tree: PASS WITH WARNINGS
required failures: 0
new warnings: 0
optional tools skipped: cargo-audit, cargo-deny, gitleaks
workspace tests: 1138 passed
```

This closure approves the design-system foundation and non-authoritative
Overview visual preview only. It does not close
`IAMINE-DASHBOARD-SHELL-001`, authorize a real node connection, or convert any
preview control into a runtime action.
