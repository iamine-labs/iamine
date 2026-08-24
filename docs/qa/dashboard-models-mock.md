# DASHBOARD-MODELS-MOCK-001 QA

## Identity

```text
project: IAMINE
feature: DASHBOARD-MODELS-MOCK-001
state: LOCAL VALIDATION PASSED
branch: codex/dashboard-models-mock-001
base: f5978c185ca766c9a47f485f450435c9364846d3
base tree: d4380eaed21504c3c94039bc78b9530b85fd72e7
implementation commit: 82d5dcb39542291407109ebdacd3539caad02477
implementation tree: 90d4bd224452d5d1f9aa1870d4a3a065f6b85330
target: develop
production runtime behavior changed: no
field QA required: no
field QA executed: no
```

QA validated the exact implementation tree on the Mac. No TS140 or Proxmox
environment was used because the feature is a deterministic browser-only mock
with no node, model, service, filesystem, or network connection.

## Scope Evidence

- `/models` replaces the reserved shell destination with a typed mock page.
- Contracts, fixture, data source, page, toolbar, table, and detail are separate
  modules.
- Generic aliases `Preview Model A` through `Preview Model E` are the only model
  identities.
- Loading, ready, empty, error, retry, filtering, no-match, and selected-detail
  states are covered.
- No download, install, license, activate, select, load, run, remove, routing,
  backend, compatibility, registry, artifact, or inference action exists.
- No HTTP, WebSocket, filesystem, shell, browser persistence, or telemetry path
  exists.
- Core, Rust, workspace-manifest, and lockfile diff is empty.

## Frontend Validation

```text
npm ci: PASS, 248 packages from the lockfile
npm run format:check: PASS
npm run lint: PASS
npm run typecheck: PASS
npm test -- --run: PASS, 9 files / 45 tests
npm run build: PASS
npm audit --audit-level=moderate: PASS, 0 vulnerabilities
npm run e2e: PASS, 12/12 tests
git diff --check: PASS
git diff --cached --check: PASS
```

The Playwright matrix covered Chromium at 1440 pixels, Firefox at 1024 pixels,
WebKit at 390 pixels, and Chromium at 360 pixels. Shell, Nodes, and Models ran
in every project. Models validation covered reload, search, segmented state
filtering, category filtering, local selection, absent real actions, console
errors, request failures, document overflow, Axe, and screenshots.

Desktop and mobile screenshots were inspected manually. The Models hierarchy,
controls, table, selected detail, status labels, provenance, text fit, and focus
surface remained coherent without page-level horizontal overflow. The global
mobile status bar retains its pre-existing horizontal-scroll behavior.

## Repository Gate

```text
repository and architecture guards: PASS
cargo fmt --all -- --check: PASS
cargo test -p iamine-models: FAIL IN SANDBOX, 155/159 PASS
cargo test -p iamine-network: PASS, 163 unit + 4 routing
cargo test -p iamine-node: FAIL IN SANDBOX, 495/496 PASS
cargo build -p iamine-node: PASS
cargo test --workspace: FAIL IN SANDBOX, same four Metal cases
cargo clippy --workspace --all-targets: PASS WITH BASELINE WARNINGS
required aggregate failures: 3
exact failed cases repeated outside sandbox: 5/5 PASS
new warnings caused by this feature: 0
```

The sandbox failures were:

```text
iamine-models integration::test_concurrency_limit
iamine-models integration::test_inference_queue
iamine-models integration::test_real_inference
iamine-models integration::test_token_streaming
iamine-node daemon_runtime::tests::test_daemon_start_stop
```

The four Metal inference cases passed individually outside the sandbox against
the implementation tree. The daemon case also passed outside the sandbox,
where its Unix socket could bind. This is the same environment-sensitive
baseline accepted for the preceding browser-only preview, and the feature has
no Rust or workspace-manifest changes.

Historical Clippy and dead-code warnings remain outside the frontend diff. The
Mac uses Node `26.7.0`, while the dashboard declares Node 24.x; npm `11.19.0`
is within range and all required frontend checks passed.

Optional repository tools were reported, not inferred:

```text
cargo-audit: SKIPPED, unavailable
cargo-deny: SKIPPED, unavailable
gitleaks: SKIPPED, unavailable
```

## QA Finding And Correction

The first E2E run passed 8/12 cases. All four Models projects stopped because
the `Review` locator also matched accessible names containing `preview`. The
test was corrected to require the exact accessible name and the entire matrix
was repeated successfully at 12/12. This was an automation selector defect;
no product behavior failed.

## Recommendation

```text
blocking product findings: 0
known limitations: browser-only synthetic data; no real model integration
recommendation: READY FOR ARCHITECTURE MERGE REVIEW
```
