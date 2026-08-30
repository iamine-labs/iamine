# HID v0.0.1 Shadow Mode QA

Feature:

```text
HID-SHADOW-MODE-001
```

Current state:

```text
IMPLEMENTATION IN PROGRESS
LOCAL VALIDATION PENDING
```

## Authorized Identity

```text
branch: feature/hid-shadow-mode-001
base branch: develop
base: b6fad67e7af4f2e691f0334d64944ab5a3a9b1a5
base tree: a18f150de86da34f64aae7fe06e7ed760154d44c
runtime behavior changed: no
field QA required: no
```

## Scope

QA validates only `.hid/`, the HID Architecture and QA documents, and the
single process-enabler row in the canonical product roadmap. Rust, TypeScript,
Cargo, npm, runtime, dashboard, network, models, workers, inference, installer,
and release behavior are out of scope and must remain unchanged.

## Checks

1. Confirm branch, base, HEAD, tree, staging, and dirty state.
2. Parse every HID YAML document with safe YAML loading.
3. Parse the evidence template and every real evidence record as JSON.
4. Parse every non-empty event line independently as JSON.
5. Verify unique event IDs, nondecreasing timestamps, known actors, known
   feature IDs, allowed event types, and exact SHA shapes.
6. Compare HID lifecycle vocabulary with the canonical workflow.
7. Confirm all canonical references exist and the integration branch is
   `develop`.
8. Confirm Shadow Mode cannot enforce gates or model routing.
9. Confirm human silence cannot authorize and every protected decision remains
   human-owned.
10. Confirm evidence binds to exact commit and tree and tree changes default to
    stale.
11. Check append-only event history against the feature base when a base log
    exists.
12. Reject common secret, private-key, personal-path, IP, MAC, or token shapes
    from feature, event, and evidence data.
13. Confirm no historical feature, approval, token count, model identity, or
    evidence was invented.
14. Confirm no product or platform-dependent file changed.
15. Run Ruby syntax, the HID validator, Git whitespace checks, and focused
    roadmap/state scans.

## Field QA

```text
required: no
reason: process metadata and documentation only
```

Mac is sufficient for this feature. TS140 and Proxmox/R5500 must not be used
solely to create ceremony for a non-operational change.

## Evidence Policy

The first committed evidence record must certify the exact implementation
commit and tree. It must use an environment class rather than a hostname and
must list bounded check identifiers rather than raw logs. A later metadata-only
evidence commit is a different tree; its local validation is reported
separately and does not silently inherit the earlier record.

## Recommendation Gate

QA may recommend only:

```text
READY FOR ARCHITECTURE REVIEW
```

QA does not authorize merge, replacement of the canonical workflow, product
feature development, or the first LAN File Share pilot.
