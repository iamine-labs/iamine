# QA Evidence

State:

```text
FIELD QA PASSED
READY FOR ARCHITECTURE MERGE REVIEW
```

The implementation was reconciled without conflicts or Reporter content
changes onto canonical `develop` `1d8b0aeb0e3254b915765d865ce572e448428c98`.
Focused validation and exact-candidate Mac QA passed for commit
`b69fd9e85c5d18a287d4c6160b6b4b27c5c5134f`, tree
`4f21ffb6d30ade3f05f63afb0d3e88c447a871e8`. The broad quality gate's five
sandbox-only test failures all passed in isolated outside-sandbox reruns. The
complete `iamine-models`, `iamine-node`, and workspace suites then passed
outside the sandbox, closing Mac Field QA.

On 2026-08-29 UTC, the exact candidate passed detached, disposable QA on TS140
and all four Proxmox/R5500 roles. Every role verified the same candidate, tree,
authorized base, and complete-history bundle before passing format, 10 Reporter
tests, 21 Node Doctor regression tests, 8 runtime enforcement tests, node build,
positive report flows, bounded rejection flows, privacy no-echo, cleanup, and
no-side-effect checks. The Proxmox host inventory preflight also passed.

The remote package required two non-product corrections: adding the installed
per-user Rust toolchain directory for non-interactive SSH and mapping SSH aliases
to the observed Proxmox VM names. Each affected attempt stopped before the next
role, preserved evidence, and did not modify product source. TS140 was not
repeated after its exact PASS.

Canonical evidence is recorded in `docs/qa/reporter-agent.md`. This package
metadata recommends `READY FOR ARCHITECTURE MERGE REVIEW`; QA does not authorize
merge or closure.
