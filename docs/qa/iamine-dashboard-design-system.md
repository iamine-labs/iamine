# IAMINE-DASHBOARD-DESIGN-SYSTEM-001 QA

## Identity And Scope

```text
branch: feature/iamine-dashboard-design-system-001
base: 1409b6fa9cb780d00fb840503c16f83bd35c0405
base tree: e55e88cbaf1f86a8b018c162a128ec7c2f13b5ef
platform: macOS arm64
runtime behavior changed: no
field QA required: no
```

The implementation adds a browser-only design-system and official Overview
visual preview under `dashboard/`, two IAMINE-owned image assets,
documentation, roadmap state, and generated-artifact ignore rules. It does not
modify Rust source, node startup, P2P, scheduler, model, agent, or inference
behavior.

## Official Asset Integrity

The three dashboard specification sheets were inspected at `1055x1491` and
used as the layout, token, component, and responsive source of truth. Two
separate official brand assets were selected for runtime use and copied without
modification:

| Runtime asset | Source SHA-256 | Repository SHA-256 | Result |
| --- | --- | --- | --- |
| `iamine-mark.png` | `a12514d84a44a9783b1cf1b1caebff424fc23db67989089bd8d90634b6b56bd1` | `a12514d84a44a9783b1cf1b1caebff424fc23db67989089bd8d90634b6b56bd1` | PASS |
| `iamine-network-wallpaper.png` | `32cf7bf5ada4689a6ee8fca206357dd4eb2a904ae1c962039961e6f8d72271ac` | `32cf7bf5ada4689a6ee8fca206357dd4eb2a904ae1c962039961e6f8d72271ac` | PASS |

Runtime weights are 232 KiB and 1.5 MiB respectively. Marketing compositions
and design sheets are excluded from the application bundle.

## Toolchain Preflight

The system Node.js version was newer than the selected application baseline.
QA therefore used the official macOS arm64 Node.js 24.18.0 archive from a
temporary directory.

```text
Node.js: 24.18.0
npm: 11.16.0
archive SHA-256 expected: e1a97e14c99c803e96c7339403282ea05a499c32f8d83defe9ef5ec66f979ed1
archive SHA-256 actual:   e1a97e14c99c803e96c7339403282ea05a499c32f8d83defe9ef5ec66f979ed1
checksum: PASS
```

The first dependency resolution found that the latest TypeScript 7 release was
outside `typescript-eslint`'s supported peer range. The implementation pinned
TypeScript 6.0.3 and regenerated the lockfile without forcing peer resolution.

The first `npm ci` also identified optional `fsevents` lifecycle scripts. The
package policy now enables strict allowlisting and explicitly denies those
scripts. A clean `npm ci` completed without script warnings afterward.

## Frontend Validation

All commands used the pinned Node.js and npm versions.

| Check | Result | Evidence |
| --- | --- | --- |
| `npm ci` | PASS | 245 packages installed from lockfile; strict script policy active |
| `npm run format:check` | PASS | All dashboard files conform to Prettier |
| `npm run lint` | PASS | ESLint reported no errors |
| `npm run typecheck` | PASS | Strict TypeScript project checks passed |
| `npm test -- --run` | PASS | 3 files, 5 tests |
| `npm run build` | PASS | Production bundle generated successfully |
| `npm run e2e` | PASS | 4 Playwright projects |
| `npm audit --audit-level=moderate` | PASS | 0 vulnerabilities |
| `npm ls --depth=0` | PASS | Direct dependency tree resolved cleanly |

Production build sizes:

```text
index.html: 0.49 kB
CSS: 22.59 kB, 5.22 kB gzip
JavaScript: 215.41 kB, 67.66 kB gzip
```

## Browser Matrix

| Browser | Viewport | Result |
| --- | ---: | --- |
| Chromium | 1440x900 | PASS |
| Firefox | 1024x768 | PASS |
| WebKit | 390x844 | PASS |
| Chromium | 360x800 | PASS |

Each project verified:

- brand and `Preview data` indicator visibility;
- local desktop navigation and the mobile drawer path;
- no browser console errors;
- no failed network requests;
- no document-width overflow or incoherent layout overlap;
- keyboard focus visibility;
- zero automated `axe-core` violations;
- full-page screenshot generation.

Visual inspection of desktop and mobile screenshots found no clipped text,
overlap, blank content, or missing assets. The Playwright harness emitted a
`NO_COLOR` and `FORCE_COLOR` environment warning; it is not browser or product
output.

The sandboxed E2E attempt could not bind `127.0.0.1:4173`, and the sandboxed
audit attempt could not resolve the npm registry. The identical checks passed
outside the network sandbox. Both initial failures are classified as harness
environment limits, not product failures.

## Findings Resolved During QA

1. TypeScript 7 was incompatible with the selected ESLint peer range. TypeScript
   6.0.3 is now pinned without a forced dependency resolution.
2. Initial Vitest project boundaries included configuration files incorrectly.
   Test and ESLint project scopes now match their owners.
3. A wrapped text-field label caused its description to enter the accessible
   name. Label, input, description, and error relationships are now explicit.
4. The mock-data indicator was hidden at a mobile breakpoint. It is now visible
   at every supported viewport.
5. Muted text did not meet the automated contrast gate. The semantic muted ink
   token was darkened.
6. The horizontally scrollable table region was not keyboard focusable. It now
   has a region role, accessible name, and tab stop.
7. macOS WebKit focus navigation required the platform `Alt+Tab` path. The E2E
   check now exercises the correct browser behavior.
8. The first official-theme lint pass contained one unused icon import. It was
   removed without suppressing the rule.
9. Automated accessibility checks rejected labels on roleless brand, avatar,
   and online-indicator elements. The decorative mark now uses visible text,
   while the avatar and indicator have explicit image roles.
10. The mobile status bar was horizontally scrollable but not focusable. It is
    now a labeled keyboard-focusable region.
11. A sticky status bar appeared between modules in full-page mobile evidence.
    It now participates in document flow and cannot obscure content.
12. The initial E2E navigation selectors matched both sidebar and topbar items.
    Exact accessible names now make the harness deterministic.

## Repository Gate

`./scripts/quality-gate.sh` completed as `PASS WITH WARNINGS`:

```text
required failures: 0
gate warnings: 0
skipped optional tools: cargo-audit, cargo-deny, gitleaks
workspace tests: 1138 passed
cargo clippy --workspace --all-targets: PASS
```

Existing compiler warnings in `iamine-client-rust`, `iamine-models`,
`iamine-network`, and `iamine-node` matched the repository baseline and are not
introduced by this frontend-only feature.

Generated `node_modules`, build, coverage, Playwright report, test result, and
TypeScript build-info artifacts are ignored and excluded from the feature.

## QA Boundary And Recommendation

TS140 and Proxmox QA were not executed because this feature has no runtime,
hardware, worker, capability, scheduler, broadcast, or inference behavior and
the current user direction limits execution to the Mac. Real node integration
must define and execute its own platform matrix.

```text
recommendation: READY FOR ARCHITECTURE MERGE REVIEW
```
