# Dashboard E2E Readiness Synchronization

## State

```text
feature: DASHBOARD-E2E-READINESS-SYNCHRONIZATION-001
state: LOCAL VALIDATION PASSED
branch: codex/dashboard-e2e-readiness-synchronization-001
base: 1d8b0aeb0e3254b915765d865ce572e448428c98
base tree: 7ad0375a7a4e5387b7a92efd7e7a4b080aa82b71
implementation: 04cbc04175a21b3ab6b228b03886bf1461112701
implementation tree: d90ee2938c82c8701a653031644e4d7f2a6fba8c
target: develop
production behavior changed: no
field QA required: no
```

## Problem

The dashboard shell E2E used `networkidle` after initial navigation and three
reloads. Vite keeps an HMR WebSocket open during Playwright runs, so Firefox
could time out after the page, assets, and route content had rendered
successfully. That synchronization signal did not represent application
readiness.

## Authorized Change

Remove the four global `networkidle` waits from
`dashboard/tests/e2e/dashboard-shell.spec.ts`. Preserve the existing bounded
signals that establish readiness and detect regressions:

- the wallpaper response must succeed;
- each route must expose its expected visible heading and content;
- URL transitions must match the selected route;
- console errors and failed requests must remain empty;
- accessibility and layout assertions must continue to pass.

No production function, dashboard component, runtime contract, API, Rust crate,
dependency, fixture, or roadmap state may change.

## Risks And Controls

| Risk | Control |
| --- | --- |
| Assertions begin before the route is usable | Playwright visibility and URL assertions auto-wait for the specific UI state. |
| A failed asset or request becomes invisible | The wallpaper response and existing failed-request listener remain active. |
| The maintenance masks a browser regression | Run the complete Chromium, Firefox, and WebKit matrix three times. |
| Frontend maintenance affects core | Isolated worktree and an empty core/Rust diff are required before push. |

## Validation Contract

Run format, lint, typecheck, unit tests, production build, dependency audit, the
exact Firefox shell case, three complete E2E matrices, the repository quality
gate, and diff checks. Raw sandbox failures must remain visible and receive
exact outside-sandbox reruns. The QA record is in
`docs/qa/dashboard-e2e-readiness-synchronization.md`.
