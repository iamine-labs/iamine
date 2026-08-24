# DASHBOARD-ACTIVITY-MOCK-001 QA

## Identity

```text
project: IAMINE
feature: DASHBOARD-ACTIVITY-MOCK-001
state: ARCHITECTURE REVIEW REQUIRED
branch: codex/dashboard-activity-mock-001
base: 6e66e2f3c4478367e9bc5fb27d4dfa04d26e4f76
base tree: a19388cda194b1f4b951299413e3d0d1eb1f7349
implementation commit: 79d5062ab4a2351c8d2606ee0edc6b711e829700
implementation tree: 64ee74c26eb818c39cce01c9fe7f5e23193c21bd
target: develop
production runtime behavior changed: no
field QA required: no
field QA executed: no
```

QA validated the exact implementation tree on the Mac. TS140 and Proxmox were
not used because the feature is a deterministic browser-only mock with no node,
event, audit, log, service, filesystem, or network connection.

## Scope Evidence

- `/activity` replaces the reserved shell destination with a typed mock page.
- Contracts, fixture, data source, page, toolbar, list, and detail are separate
  modules; no TSX or CSS module exceeds 190 lines.
- Generic aliases `Preview Event A` through `Preview Event F` and `Moment A`
  through `Moment F` are the only displayed identities and order labels.
- Loading, ready, empty, error, retry, search, filtering, no-match, and local
  selection states are covered.
- No real timestamp, audit evidence, runtime event, task trace, log, prompt,
  output, peer, host, node, model, agent package, or backend value is present.
- No acknowledge, approve, deny, retry task, replay, export, delete, clear-log,
  or open-log action exists.
- No HTTP, WebSocket, filesystem, shell, browser persistence, Local Control API,
  native bridge, or telemetry path exists.
- Rust, core, workspace-manifest, and lockfile diff is empty.

## Frontend Validation

```text
npm ci: PASS, 248 packages from the lockfile
npm run format:check: PASS
npm run lint: PASS
npm run typecheck: PASS
npm test -- --run: PASS, 10 files / 51 tests
npm run build: PASS, 1900 modules
npm audit --audit-level=moderate: PASS, 0 vulnerabilities
git diff --check: PASS
git diff --cached --check: PASS
```

The Playwright matrix covered Chromium at 1440 pixels, Firefox at 1024 pixels,
WebKit at 390 pixels, and Chromium at 360 pixels. Activity passed in all four
projects with Axe, request-failure, console-error, overflow, navigation,
filtering, local-selection, provenance, and absent-real-action assertions.

After the final mobile-only CSS adjustment, the aggregate matrix passed 15/16.
The pre-existing dashboard shell test in Firefox timed out waiting for
`networkidle`; its exact repeat passed in 5.7 seconds without a product or test
change. Final-tree coverage is therefore 15 matrix passes plus the exact failed
case passing on repeat. A focused Chromium 360 Activity repeat also passed and
regenerated the final mobile screenshot.

Desktop and mobile screenshots were inspected manually. The page hierarchy,
filters, list, detail, status labels, provenance, text fit, and focus surface
remain coherent without page-level horizontal overflow. The global mobile
status bar retains its pre-existing clipped/scrollable compact behavior.

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
new Rust warnings caused by this feature: 0
```

The sandbox failures were:

```text
iamine-models integration::test_concurrency_limit
iamine-models integration::test_inference_queue
iamine-models integration::test_real_inference
iamine-models integration::test_token_streaming
iamine-node daemon_runtime::tests::test_daemon_start_stop
```

All four Metal cases passed individually outside the sandbox against the exact
implementation tree. The daemon case also passed outside the sandbox, where
its Unix socket could bind. The failures are classified as environmental; the
aggregate FAIL remains recorded and is not rewritten as an unqualified pass.

Historical Clippy and dead-code warnings remain outside the frontend diff. The
Mac uses Node `26.7.0`, while the dashboard declares Node 24.x; npm `11.19.0`
is supported and all required frontend checks passed.

Optional repository tools were reported, not inferred:

```text
cargo-audit: SKIPPED, unavailable
cargo-deny: SKIPPED, unavailable
gitleaks: SKIPPED, unavailable
```

## QA Findings And Corrections

1. The first typecheck found an unsupported Testing Library `exact` option in
   the new unit test. The test-only call was corrected and the complete static
   frontend ladder passed.
2. The first Activity E2E run found 4.2:1 selected-row text contrast in all four
   projects. The selected secondary text token was corrected and Axe passed in
   all four projects.
3. Manual Chromium 360 inspection found adjacent segmented-control labels. The
   Activity-only mobile layout was changed to a stable 2x2 grid and revalidated.
4. A later Firefox shell `networkidle` timeout passed on exact repeat without a
   code change and is classified as a test-environment flake.

No blocking product finding remains.

## Recommendation

```text
blocking product findings: 0
known limitations: browser-only synthetic data; no real event integration
environmental exception: five sandbox-only cases, all passed outside sandbox
recommendation: READY FOR ARCHITECTURE MERGE REVIEW
```

## Architecture Review

```text
reviewed commit: c2aa12af892b83d2ad4bfeaa3b2fbe47509547d2
reviewed tree: 50c899e05fc86f8e401ef533790dae53d20bf87e
scope, ownership, authority, privacy, accessibility, and core guards: PASS
environmental exception: ACCEPTED
user authorization: EXPLICIT
blocking findings: 0
authorization: APPROVED FOR MERGE
```
