# IAMINE-DASHBOARD-DESIGN-SYSTEM-001

## Status

```text
feature: IAMINE-DASHBOARD-DESIGN-SYSTEM-001
state: LOCAL VALIDATION PASSED / ARCHITECTURE REVIEW REQUIRED
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
```

The application entry point only mounts the design-system preview. Components
remain independent of IAMINE runtime policy and transport types.

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

Every reusable component accepts presentation data only. No component decides
node readiness, eligibility, authorization, scheduler policy, model policy, or
agent execution behavior.

## Visual Contract

The design system is intended for a quiet operational interface:

- neutral canvas and surfaces with distinct green, blue, amber, and red
  semantic roles;
- no gradients, decorative background shapes, remote fonts, or remote images;
- card radii no greater than 8px;
- stable responsive dimensions without viewport-scaled typography;
- visible keyboard focus and semantic landmarks;
- contrast and accessibility checks at all supported browser viewports;
- explicit loading, empty, unavailable, and error examples.

The preview identifies itself as `Preview data` at every supported viewport.
Fixtures are deterministic and cannot invoke real node actions.

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

The next visual feature, `IAMINE-DASHBOARD-SHELL-001`, may compose these
components with typed visual mocks. It must preserve the same non-authoritative
boundary and must not introduce a fictitious endpoint.

## Architecture Maintenance

```text
iamine-node/src/main.rs: 4935 -> 4935 lines
iamine-node/src/cluster_registry.rs: 862 -> 862 lines
dashboard/src/App.tsx: 5 lines
largest preview TypeScript module: 267 lines
largest preview CSS module: 280 lines
```

No Rust module grew and no runtime ownership moved. The preview is intentionally
separate from reusable components so the shell can replace it without carrying
fixture composition into production routes.

## Known Limits

- This is a design-system preview, not an operational dashboard.
- No real IAMINE contracts, routes, authorization, audit, or node actions exist.
- Browser evidence was collected on the development Mac only.
- Remote field QA is deferred until a feature changes runtime or adds real node
  integration.
