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
| RUST-DEPENDENCY-SECURITY-REMEDIATION-001 | PROPOSED / OPEN | `cargo audit` reports 13 vulnerabilities and 19 allowed warnings. The workflow job is informational. | No unaccepted vulnerabilities; every allowed warning inventoried and dispositioned. |
| SECRETS-SCAN-CI-GATE-REPAIR-001 | PROPOSED / OPEN | The required Gitleaks job stops on organization licensing/configuration before scanning. No secret finding is established. | Gitleaks or an approved equivalent executes successfully and reports no unaccepted secrets. |

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
