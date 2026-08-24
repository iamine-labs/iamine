# DASHBOARD-NODES-MOCK-001 QA

## Identity

```text
branch: codex/dashboard-nodes-mock-001
base: 6dcb75e718c53b79bb4e3c51478da027e293de43
base tree: 5721b91883cec7a81aa33d8c533dee6541d16e58
implementation commit: 2cc346898c732b3892d1db6b6a5dd2c8e2082ae3
implementation tree: 06af5a2296c669d22e625f0f932fe55fc436b130
target: develop
platform: Mac development machine
field QA: not required for browser-only mock behavior
```

## Scope

Validate the feature-owned `/nodes` route, typed non-authoritative fixture,
complete page states, bounded search and filters, local detail selection,
accessibility, responsive layout, privacy boundaries, and absence of core or
real node behavior.

## Required Checks

```text
CHECK 1 identity, base, scope, and core guard
CHECK 2 architecture, ownership, privacy, and mock authority boundary
CHECK 3 loading, ready, empty, error, retry, and no-match states
CHECK 4 search, status/capability filters, selection, and absent real actions
CHECK 5 format, lint, typecheck, unit tests, build, and dependency audit
CHECK 6 Mac multibrowser responsive E2E, accessibility, and visual inspection
CHECK 7 repository quality gate, architecture guards, and secret guard
CHECK 8 architecture handoff and controlled push authorization
```

## Results

```text
CHECK 1 identity, base, scope, and core guard: PASS
CHECK 2 architecture, privacy, and mock authority boundary: PASS
CHECK 3 complete page states: PASS
CHECK 4 filters, selection, and blocked real actions: PASS
CHECK 5 frontend validation and dependency audit: PASS
CHECK 6 Mac multibrowser E2E, accessibility, and visual QA: PASS
CHECK 7 repository gate: PASS WITH ENVIRONMENTAL EXCEPTION RECOMMENDED
CHECK 8 architecture handoff: READY FOR ARCHITECTURE MERGE REVIEW
```

Frontend evidence:

```text
npm ci: PASS, 248 packages from lockfile
npm run format:check: PASS
npm run lint: PASS
npm run typecheck: PASS
npm test -- --run: PASS, 8 files / 39 tests
npm run build: PASS
npm audit --audit-level=moderate: PASS, 0 vulnerabilities
npm run e2e: PASS, 8/8 tests across 4 projects
```

Playwright projects:

```text
Chromium 1440x900: PASS
Firefox 1024x768: PASS
WebKit 390x844: PASS
Chromium 360x800: PASS
```

Each project validates direct route load and reload, search, preview-status and
capability filters, selection, provenance, absence of operational actions,
document overflow, console errors, failed requests, and Axe violations. Final
full-page captures were inspected at all four viewports; no overlap, blank
content, escaping label, or incoherent layout was observed.

Repository evidence:

```text
./scripts/quality-gate.sh: sandbox produced 3 aggregate required failures
failed exact cases: 4 Metal inference assertions, 1 Unix socket test
exact failed cases repeated outside sandbox: 5/5 PASS
cargo fmt, network, node build, clippy, diff, and repository guards: PASS
new warnings: 0
optional skipped: cargo-audit, cargo-deny, gitleaks unavailable
tracked generated artifacts or model binaries: none
tracked sensitive files: none
main.rs: 4935 lines, delta 0
cluster_registry.rs: 862 lines, delta 0
largest feature logic module: 191 lines
largest feature CSS module: 219 lines
core path diff: empty
```

Exact environmental repetitions:

```text
test_real_inference: PASS, 9 tokens via Metal
test_inference_queue: PASS, both queued requests completed
test_concurrency_limit: PASS, three requests completed
test_token_streaming: PASS, 9 tokens via Metal
daemon_runtime::tests::test_daemon_start_stop: PASS, socket lifecycle complete
```

## Findings

1. Axe found `4.2:1` contrast in selected-row summaries. The feature changed
   that text from muted to secondary ink; all four browser projects then passed.
2. Firefox timed out on a brittle `networkidle` wait after the page was already
   usable. The E2E now waits for the visible route heading and passed all four
   projects without relaxing product assertions.
3. Sandboxed Playwright could not bind `127.0.0.1:4173` (`EPERM`). The exact E2E
   command passed outside the sandbox.
4. The Rust gate reproduced the known sandbox-only Metal and Unix socket
   failures. Every exact failed case passed outside the sandbox against the
   same tree; the frontend feature has no Rust diff.
5. Node `26.7.0` is outside the declared Node `24.x` range. npm is supported and
   all frontend checks pass; the mismatch remains a reproducibility warning.
6. The dashboard README still described every route except Overview as a
   placeholder. It now reflects the already approved Agents, Diagnostics, and
   Nodes previews without claiming real integration.

## Safety Result

```text
HTTP or WebSocket calls: none
browser persistence: none
filesystem or shell access: none
discovery, connection, lifecycle, configuration, or node mutation actions: none
IP, MAC, hostname, serial, disk, or machine fingerprint fixtures: none
local device or cluster authority claims: none
Rust or workspace manifest changes: none
```

## Recommendation

```text
READY FOR ARCHITECTURE MERGE REVIEW
```

This evidence covers the typed visual preview only. It does not authorize real
node discovery, inventory, connectivity, hardware data, scheduler decisions,
resource controls, or lifecycle actions. TS140 and Proxmox QA are not required
because runtime, networking, hardware, models, inference, and operational
behavior are unchanged.

## Architecture Review

```text
reviewed commit: 3cca6795aa8ec45d0ac51745b86103fec39ab6fe
reviewed tree: 4057bfdd11080e92ff67836d3c2b35529be24bee
scope, ownership, authority, privacy, accessibility, and core guards: PASS
environmental exception: ACCEPTED
blocking findings: 0
authorization: APPROVED FOR MERGE
```
