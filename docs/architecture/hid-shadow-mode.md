# HID v0.0.1 Shadow Mode Architecture

Feature:

```text
HID-SHADOW-MODE-001
```

State:

```text
ARCHITECTURE APPROVED
DEVELOPMENT AUTHORIZED
IMPLEMENTATION COMPLETE
LOCAL VALIDATION PASSED
ARCHITECTURE REVIEW REQUIRED
```

## Baseline

```text
branch: feature/hid-shadow-mode-001
base branch: develop
base: b6fad67e7af4f2e691f0334d64944ab5a3a9b1a5
base tree: a18f150de86da34f64aae7fe06e7ed760154d44c
target: develop
runtime behavior changed: no
```

## Purpose

Introduce the smallest machine-readable observation layer needed to pilot
Human-IA Development in future IAMINE features. HID records bounded snapshots
of intent, scope, risk, lifecycle, Git identity, evidence, failures, authority,
model use, available metrics, and next action.

HID v0.0.1 is not a new source of authority. `AGENTS.md`, the canonical
workflow, Architecture, QA, the canonical roadmaps, and explicit human gates
continue to govern every decision.

## Architecture Decisions

1. HID is isolated under `.hid/`; product crates, dashboard code, runtime,
   network, models, workers, and inference remain unchanged.
2. `.hid/project.yaml` is the L0 constitution. It stores stable references and
   safety invariants, not current roadmap state.
3. `.hid/features/<FEATURE-ID>.yaml` is an L1 snapshot. It links to canonical
   documents instead of copying their complete narrative.
4. Architecture, QA, ADR-like decisions, and dependency documents remain L2
   knowledge in their existing paths. Historical evidence and logs remain L3.
5. HID reuses the canonical IAMINE lifecycle vocabulary. The alternate states
   proposed by the input prompt are represented through gate status, blockers,
   failure classification, or events rather than a competing state machine.
6. Git fields are timestamped observations. Evidence certifies an exact subject
   commit and tree; branch names alone are insufficient.
7. A tree change makes prior evidence stale by default. Any carry-forward must
   be an explicit Architecture or QA decision and must identify the changed
   surface.
8. The event log is append-only. Corrections are later events, not rewritten
   history.
9. Model routing is telemetry only. Profiles exist without provider mappings,
   routing decisions, cost claims, or automatic escalation.
10. Missing usage or metric values are recorded as `not_measured`; unavailable
    decisions remain `unknown`.

## Human Authority

Only a human may authorize intent and priority, material scope expansion,
architecture and security exceptions, risk acceptance, destructive operations,
merge, release, milestone closure, and public claims. Human silence is not
authorization. HID events cannot grant authority merely by naming a role.

Architecture and QA retain their canonical separation. QA evaluates an exact
candidate and does not edit product code to obtain a pass. A failure is first
classified as `product`, `baseline`, `harness`, `infrastructure`, `test_gap`, or
`unknown`.

## Privacy Boundary

HID repository data must not contain:

```text
prompt content
credentials, tokens, private keys, or secret-bearing commands
personal filesystem paths
hostnames, IP addresses, MAC addresses, or machine identifiers
raw QA logs or unredacted operator data
invented usage, cost, decision, or approval data
```

Environment observations use bounded classes such as `local_macos`,
`linux_physical`, or `linux_vm`, not machine identity. Narrative evidence keeps
following the repository artifact policy under `docs/qa/`.

## Minimal Infrastructure

```text
.hid/project.yaml
.hid/features/
.hid/evidence/
.hid/events.jsonl
.hid/templates/evidence.json
.hid/scripts/validate.rb
.hid/README.md
```

The Ruby validator has no external dependency. It validates YAML, JSON, JSONL,
canonical references, lifecycle drift, feature and evidence invariants,
append-only history when a base log exists, and common sensitive-value shapes.
It is a local check, not a HID control plane.

## Self-Reference And Evidence Recording

A commit cannot contain its own SHA. The manifest therefore records named,
timestamped Git captures instead of pretending to be a live pointer. Validation
evidence may be committed after the exact subject commit it certifies. The
evidence-recording commit is a different tree and does not inherit the subject
evidence automatically. This limitation is explicit in v0.0.1 and is a primary
candidate for a future bounded `hid capture` operation.

## Deviations From The Input Proposal

- The proposed replacement state names are not adopted because IAMINE already
  has an authoritative lifecycle. HID stores observations around those states.
- No historical feature manifests are reconstructed.
- No manifest for the next product feature is created before its separate
  Architecture authorization.
- No `hid status`, `hid capture`, or `hid evidence` command is implemented.
  Only the minimum validator required by this feature exists.
- No token, cost, context-size, or model identity is inferred when the session
  does not expose it as a measured routing decision.

These deviations preserve the canonical workflow, prevent duplicate authority,
and keep Shadow Mode small.

## Validation Gate

Required local checks:

```text
Ruby syntax and HID validator
YAML parse for project and feature manifests
JSON parse for templates and evidence
JSONL parse, event identity, ordering, and append-only check
canonical reference and lifecycle-state consistency
privacy and secret-shape scan
feature-ID uniqueness
changed-scope and product-code exclusion scan
git diff --check and git diff --cached --check
```

Field QA is not required because this feature changes repository process
metadata and documentation only. Any future HID tool that executes commands,
touches remote hosts, or affects product behavior requires a new Architecture
decision and corresponding QA.

## Out Of Scope

```text
database, vector store, server, dashboard, SaaS, or MCP server
workflow replacement or gate enforcement
automatic merge, release, or milestone closure
automatic model selection or escalation
automatic context retrieval
IdeaGraph integration
historical data reconstruction
product telemetry or user data collection
LAN-FILE-SHARE-ASSISTANT-AGENT-001 implementation
```

## First Pilot Candidate

`LAN-FILE-SHARE-ASSISTANT-AGENT-001` is the next sequential product candidate
in the canonical roadmap. It is the recommended first complete HID pilot, but
it remains `PROPOSED`. Separate Architecture must define its exact base, scope,
dependencies, risks, Field QA matrix, and Development authorization before a
pilot manifest or product branch is created.

## Implementation Checkpoint

```text
implementation commit: 2b7187a9fb98271487df05fb9fed1748871b2e4c
implementation tree: 3e8e22149021ab07201b298110c42694fb8491bb
evidence: HID-EVID-0001
local checks: 9 of 9 PASS
field QA: NOT REQUIRED
runtime behavior changed: no
```

The candidate is ready for an independent Architecture review of precedence,
privacy, Git/evidence semantics, overhead, and the boundary before the first
product pilot. Merge and product-feature authorization remain pending human
decisions.
