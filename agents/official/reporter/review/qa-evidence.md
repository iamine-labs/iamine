# QA Evidence

State:

```text
FIELD QA PASSED
READY FOR ARCHITECTURE MERGE REVIEW
```

The implementation was reconciled without conflicts onto canonical `develop`
`27c4d5fb7a6f4315546a5897c5e136c3748940ad`. Local QA found and fixed a
platform-sensitive Node Doctor timeout at the shared official-agent boundary.
The post-fix candidate is
`4a10d2912819592b6c5f0f7eef0b6ca6eb1a926c`, tree
`e416330a672270474bb99a55240075c72862d22d`. The conclusive quality gate passed
all required checks with one historical size warning and three unavailable
optional tools.

On 2026-08-29 UTC, the exact candidate passed detached, disposable QA on Mac,
TS140, and all four Proxmox/R5500 roles. Every role verified the same candidate,
tree, authorized base, and complete-history bundle before passing format, 10
Reporter tests, 21 Node Doctor regression tests, 8 runtime enforcement tests,
node build, positive report flows, bounded rejection flows, privacy no-echo,
cleanup, and no-side-effect checks. The Proxmox host inventory preflight also
passed.

The remote package required two non-product corrections: adding the installed
per-user Rust toolchain directory for non-interactive SSH and mapping SSH aliases
to the observed Proxmox VM names. Each affected attempt stopped before the next
role, preserved evidence, and did not modify product source. The full matrix
was repeated only after the candidate changed for the timeout fix.

Canonical evidence is recorded in `docs/qa/reporter-agent.md`. This package
metadata recommends `READY FOR ARCHITECTURE MERGE REVIEW`; QA does not authorize
merge or closure.
