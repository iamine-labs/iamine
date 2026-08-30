# HID Shadow Mode QA

Feature: `HID-SHADOW-MODE-001`

This document defines the reusable QA contract. Exact candidate identity,
evidence, status, and next action remain in Git/HID outputs rather than being
copied into this Markdown file.

## Scope

Validate only `.hid/`, the HID Architecture/QA documents, and an unchanged
process-enabler roadmap registration. Product crates, runtime, dashboard,
networking, agents, models, inference, scheduler, protocols, installers, and
release behavior must remain unchanged.

Field QA is not required for process-only changes. Mac is sufficient; TS140 and
Proxmox/R5500 are not used to manufacture ceremony.

## Required Checks

1. Derive branch, HEAD, tree, dirty state, base, and ancestry from Git.
2. Parse every HID YAML, JSON, and JSONL artifact safely.
3. Validate feature IDs, event IDs, schemas, timestamps, actors, and states.
4. Reject a passed human gate without a matching human authorization event.
5. Reject a wrong actor, gate action, feature, commit, tree, or dirty artifact.
6. Reject privileged states unless their required gates, human authorization,
   and merge lifecycle events are present.
7. Derive `state_gate_inconsistency` before any merge action when those
   prerequisites are missing.
8. Verify referenced evidence exists.
9. Verify evidence commit existence and real commit-to-tree relationship.
10. Classify evidence as `VALID`, `STALE`, `INVALID`, or `UNKNOWN`.
11. Require passed local validation to bind to candidate-snapshot evidence.
12. Check coverage, dependencies, and artifact/environment validity fields.
13. Fail field-based and free-text `NEVER_STORE` data and surface `REDACT`
    warnings, including compressed IPv6.
14. Check append-only baseline prefix when available; visibly report
    `not_checked` otherwise.
15. Label `origin/develop` as a local tracking ref whose remote freshness is not
    verified; never fetch automatically.
16. Confirm LAN File Share remains unimplemented and `PROPOSED`.
17. Confirm changed paths remain process-only and run Git whitespace checks.

## Regression Cases

The standard-library Minitest suite covers:

```text
passed human gate without authorization -> FAIL
agent actor on human authorization -> FAIL
authorization bound to wrong tree -> FAIL
matching human authorization -> PASS
APPROVED FOR MERGE with pending gate(s) -> FAIL
APPROVED FOR MERGE with all synthetic prerequisites -> PASS
MERGED without merged event -> FAIL
CLOSED equivalent without post-merge events -> FAIL
evidence for another current artifact -> STALE
commit/tree contradiction -> INVALID
missing referenced evidence -> FAIL
api_key-like field -> privacy_violation
secret assignment in arbitrary text -> privacy_violation
complete prompt/response field -> privacy_violation
prompt IDs, hashes, and token counts -> PASS
compressed and full IPv6 -> privacy_warning
local absolute path -> privacy_warning
append-only preserved prefix -> PASS
append-only changed prefix -> FAIL
missing local origin/develop baseline -> not_checked
Git capture -> derived current identity
```

## Evidence Policy

Evidence certifies only its named commit/tree and bounded claims. A later commit
that records evidence is a distinct artifact and therefore visibly stale under
the conservative v0.0.2 rule. This is expected and must not be described as
carry-forward.

Environment classes replace host identity. Evidence must not contain raw logs,
prompts, credentials, local absolute paths, hostnames, addresses, machine IDs,
or secret-bearing commands.

## Result Boundary

Development may recommend only:

```text
READY FOR ARCHITECTURE REVIEW
```

QA and HID do not authorize merge, replace the canonical workflow, approve a
product feature, or start the LAN File Share pilot.
