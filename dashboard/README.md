# IAMINE Dashboard

This directory contains IAMINE's local dashboard frontend. The current surface
is a routed application shell around the non-authoritative official Overview
preview. It does not connect to a node, a Local Control API, P2P, the
filesystem, or an operating-system shell.

Routes use the URL hash so the deterministic static build does not require a
server-side fallback. Only `/overview` contains approved preview content; the
other destinations are bounded placeholders for their own future features.

## Toolchain

- Node.js: use the exact version in `.node-version`.
- npm: use the exact major and package-manager version declared in
  `package.json`.
- Dependencies: install from `package-lock.json` with `npm ci`.

Dependency lifecycle scripts are denied by default through `.npmrc`. Every
exception must be explicit in the `allowScripts` map in `package.json`.

## Commands

```bash
npm ci
npm run dev
npm run format:check
npm run lint
npm run typecheck
npm test -- --run
npm run build
npm run e2e
npm audit --audit-level=moderate
```

## Ownership

- `src/components/` owns reusable presentation primitives.
- `src/app/` owns composition, routes, navigation, and top-level failures.
- `src/styles/` owns global tokens, reset, and typography foundations.
- `src/preview/` owns deterministic preview fixtures and panels only.
- `tests/e2e/` owns browser, responsive, keyboard, and accessibility evidence.
- `public/assets/` contains reviewed IAMINE-owned runtime image assets.

Rust remains the source of truth for IAMINE domain behavior. Future dashboard
features must consume reviewed typed contracts through adapters and must not
copy scheduler, model, authorization, audit, agent, or runtime policy into this
application.
