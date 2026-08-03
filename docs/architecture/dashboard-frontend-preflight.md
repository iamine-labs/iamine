# DASHBOARD-FRONTEND-PREFLIGHT-001

## Status

```text
feature: DASHBOARD-FRONTEND-PREFLIGHT-001
state: READY FOR MERGE REVIEW
base: origin/develop at e2e6a8a70a8f952bf4eb064a7fd9f70e39aac72a
branch: feature/dashboard-frontend-preflight-001
runtime behavior change: none
frontend application created: no
dependency installation: none
```

## Purpose

Select the canonical foundation for IAMINE's local dashboard before creating
application directories, lockfiles, build output, or a desktop wrapper.

The dashboard is the default operator interface for general users. The CLI
remains the advanced, automation, QA, and headless interface. Both surfaces
must consume the same IAMINE-owned contracts and must not implement competing
domain rules.

## Repository Findings

At this baseline:

- the repository is a Rust workspace;
- no canonical frontend application, JavaScript manifest, or frontend lockfile
  exists;
- TypeScript is the roadmap language for dashboard and tooling work;
- LAN beta packaging already supports macOS launchd and Linux systemd, while
  Windows remains a product target without an equivalent package in the
  repository;
- node management, local authorization, and Local Control API contracts remain
  independent future features;
- native mobile implementation is deferred.

No existing application must be migrated or preserved by this preflight.

## Canonical Stack Decision

The first dashboard implementation will use:

| Layer | Decision |
| --- | --- |
| Language | TypeScript in strict mode |
| UI | React functional components |
| Build and development | Vite |
| Package manager | npm with a committed `package-lock.json` |
| Styling | CSS Modules plus IAMINE-owned CSS custom-property tokens |
| Icons | `lucide-react` |
| Unit and component tests | Vitest, Testing Library, and `user-event` |
| Accessibility checks | semantic queries plus `axe-core` integration |
| Browser and visual E2E | Playwright |
| Formatting | Prettier |
| Static analysis | ESLint with TypeScript rules |

The implementation feature must pin an active Node.js LTS release in a
repository toolchain file and record the npm version through the package
manifest. Exact dependency versions are selected and locked when the frontend
is scaffolded, not in this documentation-only preflight.

React and Vite are selected for the local browser experience and for a future
thin desktop wrapper. Flutter is not selected because native mobile is
deferred and the current product boundary is a local dashboard that consumes
JSON contracts. A native desktop shell must not become a second domain layer.

## Dependency Policy

The first frontend must keep its runtime dependency set small:

- React and React DOM are baseline runtime dependencies.
- Lucide is the canonical icon source.
- Routing may be added by the Shell feature when more than one real route
  exists.
- Server-state libraries remain deferred until a real read-only API contract
  demonstrates a need.
- Accessible headless primitives may be added one component at a time after
  license, maintenance, bundle, keyboard, and focus review.
- A general component suite, analytics SDK, telemetry SDK, remote font, remote
  icon service, and CSS utility framework are not baseline dependencies.
- New dependencies require a named owner, direct use, compatible license,
  locked version, and validation evidence.

The repository lockfile is single-owner integration surface. Parallel frontend
branches must not independently add dependencies without coordination.

## Future Repository Layout

The scaffold feature will create one application rather than a JavaScript
monorepo:

```text
dashboard/
  package.json
  package-lock.json
  tsconfig.json
  vite.config.ts
  src/
    app/
    adapters/
    components/
    contracts/
      generated/
      view-models/
    features/
    mocks/
    styles/
  tests/
    e2e/
```

Ownership rules:

- `app/` owns composition, providers, routes, and top-level error handling.
- `components/` owns reusable presentation primitives without IAMINE domain
  policy.
- `features/` owns user journeys and feature-local presentation.
- `adapters/` maps a typed data source into view models.
- `contracts/generated/` contains generated API types and must not be edited by
  hand.
- `contracts/view-models/` contains presentation types only; it cannot redefine
  node policy.
- `mocks/` contains deterministic fixtures and a mock adapter, never a fake
  network endpoint.
- `styles/` owns global reset, tokens, typography, and theme foundations.

Shared packages must be introduced only after a second application proves a
real sharing boundary.

## Contract Boundary

Rust remains the source of truth for IAMINE domain behavior. Future shared
contracts must flow in one direction:

```text
Rust owner types
-> reviewed Local Control API schema
-> generated TypeScript transport types
-> frontend adapter
-> dashboard view models
-> React presentation
```

The frontend must not duplicate:

- node eligibility or readiness policy;
- model, scheduler, P2P, worker, or inference decisions;
- agent scope, permission, authorization, audit, or execution rules;
- privacy redaction rules;
- lifecycle transition rules.

Unknown enum variants, absent optional fields, incompatible schema versions,
and malformed payloads must become bounded unavailable or error states. They
must not be guessed into a successful state.

Mock contracts are presentation fixtures, not provisional API contracts. Mock
data must be selected through an explicit `MockDashboardDataSource`, visibly
identified as non-authoritative in the rendered experience, and impossible to
use for real node actions.

## Runtime and Security Boundary

The dashboard must comply with these invariants:

```text
localhost-only by default
no default 0.0.0.0 bind
no direct P2P access
no direct filesystem or shell access
no business or authorization logic in the frontend
no credentials, tokens, private keys, or wallet secrets in bundles
no sensitive values in browser storage
no remote telemetry by default
no remote assets required at runtime
no real mutation before authorization and audit gates close
```

The future Local Control API owns origin validation, local authorization,
request validation, redaction, audit, and mutation policy. A browser address of
`localhost` is not by itself an authorization mechanism.

Frontend code must avoid dynamic HTML insertion and dynamic shell or command
construction. Content Security Policy, origin policy, and local session
semantics are defined with the Local Control API and bundling features.

## Product and Visual Baseline

The dashboard is an operational tool, not a marketing page. It must prioritize
scanning, comparison, status, and repeated actions.

Continuous acceptance requirements:

- restrained multi-hue palette with clear semantic status colors;
- stable responsive dimensions and no content overlap;
- keyboard navigation and visible focus;
- semantic landmarks, labels, headings, and status announcements;
- readable loading, empty, unavailable, error, and stale states;
- text fit at supported viewports without viewport-scaled typography;
- familiar icons for common commands, with tooltips where meaning is unclear;
- no nested cards or decorative dashboard sections;
- no action displayed as available when its backend gate is unavailable.

Target accessibility is WCAG 2.2 AA for the supported dashboard journeys.

## Supported Targets

The first implementation target is a local responsive web application:

- macOS, Linux, and Windows operator browsers;
- the Chromium, Firefox, and WebKit versions pinned by Playwright;
- keyboard and pointer input;
- representative viewport evidence at 1440x900, 1024x768, 390x844, and
  360x800.

Native mobile applications are out of scope. Responsive behavior should keep
contracts, tokens, information architecture, and copy reusable, but it must not
claim native mobile reuse.

## Validation Contract

Once the application exists, every frontend feature must run the narrowest
relevant commands while iterating and this full gate before handoff:

```bash
npm ci
npm run format:check
npm run lint
npm run typecheck
npm test -- --run
npm run build
npm run e2e
```

The scaffold feature must provide these stable scripts. Playwright evidence
must include supported desktop and mobile-sized viewports, browser console
errors, failed requests, nonblank rendering, text fit, and overlap checks.

CI must add a frontend job only after the application and lockfile exist. Rust
quality gates remain unchanged and frontend success cannot mask a Rust gate
failure.

For this preflight itself, required validation is:

```bash
git diff --check
./scripts/quality-gate.sh
```

No npm command is valid yet because no frontend manifest exists.

## Packaging Direction

The production build must be deterministic static assets with no CDN or
runtime Node.js requirement. Packaging remains a separate feature:

- `IAMINE-DASHBOARD-SHELL-001` creates the browser application shell;
- `NODE-SERVICE-LIFECYCLE-001` defines node start, stop, recovery, and status;
- `LOCAL-DASHBOARD-BUNDLING-001` may introduce a thin Tauri desktop wrapper or
  package the static dashboard with the local service after a dedicated review;
- code signing, update, rollback, and installer behavior remain separate
  release concerns.

Headless IAMINE nodes must remain fully usable without dashboard assets or a
JavaScript runtime. The CLI remains the parity and recovery surface.

## Parallel Ownership

After `GUI-CLI-INTERFACE-ARCHITECTURE-001` freezes boundaries, work may split
without overlapping ownership:

| Feature | Primary future ownership |
| --- | --- |
| IAMINE-DASHBOARD-DESIGN-SYSTEM-001 | `dashboard/src/components/`, `dashboard/src/styles/` |
| IAMINE-DASHBOARD-SHELL-001 | `dashboard/src/app/` |
| IAMINE-DASHBOARD-OVERVIEW-MOCK-001 | `dashboard/src/features/overview/`, `dashboard/src/mocks/` |

Package manifests, lockfiles, generated contracts, global configuration, and
CI are serialized integration surfaces with one owner at a time.

## Out of Scope

This feature does not:

- create `dashboard/` or any frontend source;
- install or select exact dependency versions;
- define the Local Control API;
- read real node status, logs, agents, models, peers, or hardware;
- start, stop, configure, or mutate a node;
- implement authentication, authorization, audit, or agent execution;
- add a desktop or mobile wrapper;
- modify Rust, Cargo, CI, packaging, P2P, scheduler, inference, or runtime
  behavior.

## Risks and Controls

| Risk | Control |
| --- | --- |
| Mock UI drifts from backend semantics | Keep mock view models explicitly non-authoritative; generate real transport types only from approved owner schemas. |
| Frontend duplicates policy | Restrict it to adapters, view models, and presentation; fail closed on unknown transport data. |
| Parallel branches conflict | Freeze ownership first and serialize lockfile, configuration, generated-contract, and CI changes. |
| Desktop packaging drives premature coupling | Keep the first build static and browser-capable; review Tauri only during bundling. |
| Dependency growth | Require direct use, ownership, license review, locked versions, and focused alternatives. |
| Mock actions look functional | Render them unavailable or demonstrative and provide no real action adapter. |
| Mobile scope expands silently | Support responsive web layouts only; native mobile remains deferred. |

## Acceptance Criteria

- canonical frontend stack and package manager are selected;
- future layout and file ownership are explicit;
- contract source of truth and failure behavior are explicit;
- security, privacy, accessibility, responsive, validation, and packaging
  boundaries are recorded;
- mock-only and real-integration boundaries remain distinct;
- no frontend application, dependency, lockfile, generated asset, or runtime
  behavior is introduced;
- the next feature is `GUI-CLI-INTERFACE-ARCHITECTURE-001`.

## Local Validation Evidence

Focused documentation and scope validation passed:

```text
quality gate guard-only: PASS
required_failures: 0
warnings: 0
skipped: 0
cargo fmt --all -- --check: PASS
git diff --check: PASS
git diff --cached --check: PASS
staged scope: two docs files only
dashboard application directory: absent
main.rs delta: 0
```

The unrestricted quality gate did not pass and was not reported as a pass:

- `iamine-models` unit tests passed 99/99;
- `iamine-models` integration tests passed 55/59; four existing real
  Metal/TinyLlama inference assertions returned `success=false`;
- `iamine-network` passed 167/167;
- `iamine-node` passed 479/480; the daemon socket test was denied by the local
  sandbox with `Operation not permitted`;
- `cargo build -p iamine-node` passed;
- `cargo test --workspace` was stopped after more than ten minutes without new
  output while sharing the repository target directory.

The staged diff contains no executable files, so these failures cannot be
caused by this feature's product behavior. They remain baseline/environment
exceptions for Architecture to classify; the full quality gate is not green.

## Architecture Checkpoint

The implementation checkpoint was reviewed at commit
`d24ced0e6165a036f3e89c5340b56d0b8cada70b` with tree
`8f1c15e32f42825ab8171f041061c51865b95f3f`:

```text
scope ownership: PASS
Rust and runtime non-regression by diff: PASS
frontend application remains absent: PASS
shared-contract source of truth: PASS
security and privacy boundaries: PASS
mock versus real integration boundary: PASS
dependency and packaging decisions: PASS
field QA requirement: NOT REQUIRED
broad-gate exceptions: ARCHITECTURE DECISION REQUIRED
recommendation: READY FOR ARCHITECTURE MERGE REVIEW
```

At checkpoint time, `origin/develop` had advanced to
`3374e27f7b6b132b39c3e979af7a1a03cd5daf9b` through the closed Node Doctor
evidence-provider work. That delta does not modify this feature's two owned
files and produces no merge-tree conflict. Controlled integration must still
recheck the current remote target immediately before merge.

## QA Classification

Field QA is not required for this documentation-only preflight. No executable
surface, frontend application, package, node contract, or runtime behavior
exists to exercise.

Future visual-only mock features require browser, viewport, accessibility, and
visual QA on the Mac development environment. Local Control API, service
lifecycle, real dashboard integration, and packaging features require their
own Architecture decision for Mac, TS140, and Proxmox/R5500 field coverage.
