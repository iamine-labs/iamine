# IAMINE-DASHBOARD-SHELL-001

## State

```text
feature: IAMINE-DASHBOARD-SHELL-001
state: READY FOR MERGE REVIEW
branch: feature/iamine-dashboard-shell-001
base: 0c299833c74b99bed84a1a68a241a6dba528f2e8
target: develop
runtime behavior changed: no
field QA required: no; browser-only mock surface
```

## Goal

Promote the approved dashboard preview composition into a stable browser
application shell. The shell owns navigation, route semantics, responsive
chrome, top-level loading and failure handling, and explicit availability
boundaries. It must remain independent from IAMINE node runtime behavior.

## Ownership

```text
dashboard/src/app/        composition, routes, chrome, shell lifecycle
dashboard/src/components reusable visual primitives
dashboard/src/preview/    deterministic non-authoritative panels and fixtures
dashboard/src/features/   reserved for feature-owned journeys
```

The shell may render the approved Overview preview, but it does not own future
Overview contracts, adapters, data loading, or node connectivity.

## Route Contract

The first shell uses declarative `react-router` hash routes:

```text
#/overview      approved non-authoritative preview
#/agents        reserved destination
#/nodes         reserved destination
#/models        reserved destination
#/activity      reserved destination
#/marketplace   reserved destination
*               bounded not-found state
```

Hash routing keeps direct navigation and refresh deterministic for static
assets without requiring a server-side SPA fallback. `react-router@7.18.2` is
an exact, lockfile-owned MIT dependency. It owns browser route parsing and
history behavior; IAMINE does not hand-roll those concerns.

## Safety Boundary

- No Rust crate or workspace manifest changes.
- No Local Control API, HTTP, WebSocket, P2P, filesystem, or shell access.
- No dashboard route represents an IAMINE command or operation identity.
- No reserved destination exposes an enabled action.
- Preview provenance and disconnected core state remain visible.
- Unknown paths fail closed into an inert not-found state.
- Render failures report a local UI failure without claiming a node action.

Real node reads remain blocked behind:

```text
GUI-CLI-SHARED-CONTRACTS-001
-> NODE-LOCAL-CONTROL-API-CONTRACT-001
-> DASHBOARD-LOCAL-AUTHORIZATION-001
-> NODE-LOCAL-CONTROL-API-001
-> IAMINE-DASHBOARD-OVERVIEW-READONLY-INTEGRATION-001
```

## Modularity

- `routes.ts` is the single typed route inventory.
- `DashboardChrome.tsx` renders navigation and responsive drawer controls.
- `DashboardShell.tsx` composes routes and approved preview content.
- `DashboardStatusBar.tsx` reports non-authoritative connection state.
- `DashboardErrorBoundary.tsx` owns fatal render fallback behavior.

Fixture data remains in `preview/`; it is not moved into route or shell state.

## Acceptance Criteria

- root navigation redirects to `#/overview`;
- each approved destination has a stable URL and survives refresh;
- unknown paths render a bounded not-found state;
- mobile navigation closes after route selection and supports Escape;
- keyboard focus, landmarks, labels, disabled states, and skip navigation are
  present;
- all supported viewports render without overlap or horizontal overflow;
- browser console errors and failed requests remain empty;
- mock provenance and disconnected core state are visible;
- no core source or behavior changes;
- full frontend and repository quality gates pass.

## Risks

| Risk | Control |
| --- | --- |
| Preview is mistaken for live data | Persistent preview badge and disconnected status bar. |
| Placeholder becomes a fake operation | Reserved routes contain no transport or mutation callbacks. |
| Route logic spreads across features | One typed route inventory owned by `app/`. |
| Static refresh fails | Hash routing keeps paths out of server requests. |
| Shell becomes a frontend monolith | Chrome, routes, status, errors, and content are separate modules. |
| Frontend work damages core | Isolated worktree plus mandatory pre-push core safety gate. |

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
cd ..
./scripts/quality-gate.sh
git diff --check
```

The Node version mismatch of the current Mac terminal must be classified as an
environment finding if Node 24.18.0 is unavailable; it does not authorize a
toolchain change inside this feature.

## Architecture Checkpoint

```text
production shell ownership: dashboard/src/app/
largest new TypeScript module: 171 lines
largest new CSS module: 306 lines
iamine-node/src/main.rs: 4935 -> 4935 lines
iamine-node/src/cluster_registry.rs: 862 -> 862 lines
Rust/core diff: empty
new dependency: react-router@7.18.2, MIT, exact lock
real node connection: none
real node action: none
```

The implementation preserves the approved boundaries and is ready for merge
review. This checkpoint does not authorize real dashboard integration or a
merge.
