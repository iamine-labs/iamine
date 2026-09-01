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
8. Bind Human Gates to the current clean Git HEAD/tree, never a historical
   candidate snapshot, and apply the last relevant decision in log order.
9. Union non-configurable constitutional requirements with project policy;
   project policy may add gates but cannot remove Human Merge.
10. Fail startup and next-action derivation when any canonical privileged state
    lacks policy or constitutional coverage.
11. Require physical log order `authorization < merge < post-merge < closure`
    for the relevant feature and artifacts.
12. Require real commit/tree identity and exact candidate binding for merge,
    post-merge validation, and closure events.
13. Require a two-parent `no_ff_merge` with the exact candidate as second
    parent and the integration artifact contained in local `develop`.
14. Reject side-branch-only integration, alternate event targets, unsupported
    strategies, missing canonical refs, and unverified containment.
15. Ask Git for the deterministic clean merge tree of the integration commit's
    first parent and exact candidate; reject any actual-tree mismatch.
16. Fail closed when the clean tree is unavailable or the merge conflicts;
    manual conflict resolution is unsupported by the canonical workflow.
17. Verify referenced evidence exists.
18. Verify evidence commit existence and real commit-to-tree relationship.
19. Classify evidence as `VALID`, `STALE`, `INVALID`, or `UNKNOWN`.
20. Require passed local validation to bind to candidate-snapshot evidence.
21. Check coverage, dependencies, and artifact/environment validity fields.
22. Fail field-based and free-text `NEVER_STORE` data and surface `REDACT`
    warnings, including compressed IPv6.
23. Check append-only baseline prefix when available; visibly report
    `not_checked` otherwise.
24. Label Git integration containment as local-only and `origin/develop` as a
    local tracking ref whose remote freshness is not verified; never fetch
    automatically.
25. Confirm both proposed pilots remain unimplemented and `PROPOSED`.
26. Confirm changed paths remain process-only and run Git whitespace checks.

## Regression Cases

The standard-library Minitest suite covers:

```text
passed human gate without authorization -> FAIL
agent actor on human authorization -> FAIL
authorization bound to wrong tree -> FAIL
matching human authorization -> PASS
historical snapshot authorization for newer current candidate -> FAIL
current candidate authorization -> PASS
approval then denial -> NOT AUTHORIZED
denial then approval -> AUTHORIZED
APPROVED FOR MERGE with pending gate(s) -> FAIL
APPROVED FOR MERGE with all synthetic prerequisites -> PASS
privileged state missing policy entry -> POLICY_INCOMPLETE
partial policy omits human_merge -> FAIL
policy explicitly disables human_merge -> CONSTITUTIONAL_POLICY_VIOLATION
project policy adds security_review and satisfies it -> PASS
MERGED without merged event -> FAIL
MERGED with nonexistent or unrelated Git artifact -> FAIL
CLOSED equivalent without post-merge events -> FAIL
valid real Git merge/post-merge/closure path -> PASS
linear descendant on side branch -> CANDIDATE_INTEGRATION_MISMATCH
valid-shaped no-ff merge only on side branch -> CANONICAL_INTEGRATION_MISSING
valid no-ff merge contained in develop -> PASS
missing local develop -> CANONICAL_REF_UNAVAILABLE
event target differs from configured develop -> CANONICAL_TARGET_MISMATCH
fast-forward or other unsupported strategy -> INTEGRATION_STRATEGY_UNSUPPORTED
extra modifying commit after authorized candidate -> CANDIDATE_INTEGRATION_MISMATCH
post-merge or closure on side-only artifact -> FAIL
actual no-ff merge tree equals Git-computed clean tree -> PASS
manual topology with exact clean tree -> PASS
added, modified, or deleted merge-tree content -> MERGE_TREE_MISMATCH
clean merge conflict -> MERGE_NOT_CLEAN
fabricated manual conflict resolution -> MERGE_NOT_CLEAN
unavailable deterministic tree -> MERGE_TREE_NOT_VERIFIABLE
post-merge or closure on manipulated tree -> FAIL
23 invalid lifecycle permutations -> LIFECYCLE_ORDER_VIOLATION
authorization then merge then post-merge then closure -> PASS
approval then denial before merge -> FAIL
denial after valid merge -> historical merge remains valid
event from another feature -> ignored for this feature's sequence
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
