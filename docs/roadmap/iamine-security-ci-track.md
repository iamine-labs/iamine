# IAMINE Security and CI Track

## State

```text
track: IAMINE-SECURITY-CI-TRACK
state: PROPOSED / OPEN
implementation authorization: none
```

This cross-cutting maintenance track does not replace or reorder the sequential
Agent Runtime feature. Open findings are not necessarily blockers for every
development merge under the current workflow, but they block release closure
and supply-chain readiness.

## Features

| Feature | State | Current evidence | Closure condition |
| --- | --- | --- | --- |
| RUST-DEPENDENCY-SECURITY-REMEDIATION-001 | IMPLEMENTATION COMPLETE / ARCHITECTURE REVIEW REQUIRED | Exact base `1409b6f` measured 863 packages, 13 vulnerabilities, and 18 warnings. The Mac-validated checkpoint reduces the supported graph to 494 packages, 2 vulnerabilities, and 3 warnings; all 1,138 workspace tests pass serially and the local P2P smoke passes. `RUSTSEC-2026-0119` remains reachable through mDNS, `RUSTSEC-2026-0118` has no active DNSSEC feature, and TS140/Proxmox field QA is deferred. | No unaccepted vulnerabilities; every allowed warning inventoried and dispositioned; required Mac, TS140, and Proxmox/R5500 network regression evidence complete. |
| SECRETS-SCAN-CI-GATE-REPAIR-001 | PROPOSED / OPEN | The last recorded required Gitleaks job stopped on organization licensing/configuration before scanning. No secret finding is established; refresh the workflow state at activation. | Gitleaks or an approved equivalent executes successfully and reports no unaccepted secrets. |

## Required Validation

```text
cargo audit executes without unaccepted vulnerabilities
allowed warnings are inventoried
the secret scan actually executes
no unaccepted secrets are detected
Node 20 action debt is resolved
workspace, fmt, clippy, and required tests pass
relevant network security smoke passes
```

Dependency updates, workflow changes, accepted exceptions, and network security
smokes require their own feature lifecycles. This roadmap registration does not
authorize those changes.
