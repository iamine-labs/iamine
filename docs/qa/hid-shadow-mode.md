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
6. Verify referenced evidence exists.
7. Verify evidence commit existence and real commit-to-tree relationship.
8. Classify evidence as `VALID`, `STALE`, `INVALID`, or `UNKNOWN`.
9. Require passed local validation to bind to candidate-snapshot evidence.
10. Check coverage, dependencies, and artifact/environment validity fields.
11. Fail detected `NEVER_STORE` data and surface `REDACT` warnings.
12. Derive next action without granting authorization.
13. Check append-only baseline prefix when available; visibly report
    `not_checked` otherwise.
14. Confirm LAN File Share remains unimplemented and `PROPOSED`.
15. Confirm changed paths remain process-only and run Git whitespace checks.

## Regression Cases

The standard-library Minitest suite covers:

```text
passed human gate without authorization -> FAIL
agent actor on human authorization -> FAIL
authorization bound to wrong tree -> FAIL
matching human authorization -> PASS
evidence for another current artifact -> STALE
commit/tree contradiction -> INVALID
missing referenced evidence -> FAIL
api_key-like field -> privacy_violation
local absolute path -> privacy_warning
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
