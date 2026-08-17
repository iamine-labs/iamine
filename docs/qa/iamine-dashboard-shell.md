# IAMINE-DASHBOARD-SHELL-001 QA

## Identity

```text
branch: feature/iamine-dashboard-shell-001
base: 0c299833c74b99bed84a1a68a241a6dba528f2e8
target: develop
platform: Mac development machine
field QA: not required for browser-only mock behavior
```

## Scope

Validate route persistence, inert reserved destinations, unknown-route and
fatal-render boundaries, responsive navigation, accessibility, preview
provenance, disabled actions, and absence of core changes.

## Required Checks

```text
CHECK 1 identity, base, scope, and core guard
CHECK 2 architecture and ownership
CHECK 3 modular shell implementation
CHECK 4 format, lint, typecheck, and unit tests
CHECK 5 build and multibrowser E2E
CHECK 6 Mac visual and responsive QA
CHECK 7 repository quality gate and size review
CHECK 8 architecture handoff and controlled push authorization
```

## Results

```text
CHECK 1 identity, base, scope, and core guard: PASS
CHECK 2 architecture and ownership: PASS
CHECK 3 modular shell implementation: PASS
CHECK 4 frontend static and unit validation: PASS after product correction
CHECK 5 build and multibrowser E2E: PASS after harness corrections
CHECK 6 Mac visual and responsive QA: PASS
CHECK 7 repository gate and size review: PASS WITH WARNINGS
CHECK 8 architecture handoff: READY FOR MERGE REVIEW
```

Frontend evidence:

```text
npm ci: PASS, 248 packages from lockfile
npm run format:check: PASS
npm run lint: PASS
npm run typecheck: PASS
npm test -- --run: PASS, 3 files / 7 tests
npm run build: PASS, deterministic static assets
npm run e2e: PASS, 4/4 projects
npm audit --audit-level=moderate: PASS, 0 vulnerabilities
```

Playwright projects:

```text
Chromium 1440x900: PASS
Firefox 1024x768: PASS
WebKit 390x844: PASS
Chromium 360x800: PASS
```

Each project validated successful wallpaper delivery, route persistence after
reload, keyboard order, disabled unavailable actions, no document overflow,
content below the top bar, no console errors, no failed requests, and no Axe
violations. Captures were inspected manually and showed no overlap, blank
canvas, or illegible text.

Repository evidence:

```text
./scripts/quality-gate.sh: PASS WITH WARNINGS
required failures: 0
new warnings: 0
workspace tests: 1138 passed
cargo clippy --workspace --all-targets: PASS with baseline warnings
optional skipped: cargo-audit, cargo-deny, gitleaks unavailable
main.rs: 4935 lines, delta 0
cluster_registry.rs: 862 lines, delta 0
largest new TypeScript module: 171 lines
largest new CSS module: 306 lines
core path diff: empty
git diff --check: PASS
```

## Findings

1. React Router 7 navigation returns a promise. Three lint failures were fixed
   by explicitly discarding navigation promises at event boundaries.
2. The new skip link changed expected keyboard order. The E2E harness was
   corrected to validate the skip link before route navigation.
3. Firefox intermittently cancelled the large wallpaper request during an
   immediate route transition. The harness now waits for an explicit successful
   wallpaper response before navigating; isolated Firefox and 4/4 final E2E
   runs passed.
4. The sandbox blocked the local Vite listener with `EPERM`; E2E passed outside
   the sandbox and the failure is classified as harness/environment.
5. The Mac terminal uses Node 26.0.0 and npm 11.12.1 while the project pins Node
   24.18.0 and npm 11.16.0. All checks passed, but `EBADENGINE` and the historical
   `strict-allow-scripts` warning remain an accepted environment exception for
   this run. The feature does not change the canonical toolchain.

## Recommendation

```text
READY FOR ARCHITECTURE MERGE REVIEW
```

A passing browser mock does not authorize real node connectivity or close any
control API dependency. Field QA on TS140 or Proxmox is not required because no
runtime, scheduler, capability, worker, hardware, network, or inference behavior
changed.
