# HID Shadow Mode Architecture

Feature: `HID-SHADOW-MODE-001`

The canonical state is read from the feature manifest and workflow. This
document defines architecture and does not duplicate the current Git identity,
gate result, validation count, or next action.

## Purpose

HID v0.0.2 is a bounded machine-readable observation layer. It captures facts
needed to study a future control plane while the canonical IAMINE workflow,
Architecture, QA, roadmaps, and explicit human gates retain all authority.

It changes no product behavior. Runtime, Core, agents, models, networking,
inference, scheduler, dashboard, protocols, and product security are excluded.

## Data Ownership

HID distinguishes:

```text
SOURCE   human or Architecture input
DERIVED  current Git facts, evidence status, next action
SNAPSHOT derived facts captured at a historical moment
```

Current branch, HEAD, tree, dirty state, and ancestry come from Git through
`.hid/scripts/capture.rb`. Manifests may retain named snapshots but must not
present them as self-updating current state.

The canonical lifecycle is parsed from
`docs/process/iamine-canonical-workflow.md`; `.hid/project.yaml` no longer keeps
a second editable list of lifecycle states.

## Human Authority

Human gates are structurally correlated with `human_authorization` events. A
supported approval contains:

```text
feature
gate and action
actor.type = human
timestamp
clean commit and tree
decision = approved
```

A `passed` gate without the matching event is a validation failure. A matching
event does not set a gate automatically and does not grant HID authority.
Tooling validates structure and artifact correlation, not cryptographic human
identity. Agents must not represent themselves as humans or infer approval from
silence, tests, or prior conversation.

The prior `architecture: passed` observation had no supporting authorization
event. v0.0.2 corrects the mutable gate to `pending` and appends a corrective
event without rewriting the old event log.

## Evidence Integrity

Evidence status is derived against Git:

```text
VALID    commit exists, real tree matches, and current clean artifact matches
STALE    commit/tree is internally valid but belongs to another artifact
INVALID  commit is missing or its real tree contradicts the record
UNKNOWN  Git cannot verify the record in the current environment
```

Referenced evidence must exist. A passed local-validation gate must reference
evidence for the feature's recorded candidate snapshot. v0.0.2 uses a
conservative artifact-change-means-stale rule; ancestry-based reuse is deferred.

Coverage paths and claims describe only what was tested. Dependencies are
recorded only when they condition validity. There is no arbitrary TTL.

## Privacy

`.hid/privacy.yaml` defines:

- `ALLOW`: bounded identifiers, hashes, repository-relative paths, counts,
  classifications, and abstract profiles;
- `REDACT`: emails, addresses, local paths, usernames, sensitive hostnames, and
  unnecessary URL data;
- `NEVER_STORE`: secrets, credentials, tokens, private keys, secret-bearing
  URLs, environment dumps, full prompts, and full model responses.

Detected `privacy_violation` values fail validation. A `privacy_warning` remains
visible for human review and deterministic redaction. The validator never edits
data and regular expressions cannot prove the absence of secrets.

## Append-only Policy

The event log is an append-only policy with baseline-prefix validation when the
canonical base contains a prior log. Without that baseline, validation reports
`not_checked`. It is not tamper-proof or cryptographically immutable.

## Tool Boundary

The tooling remains repository-local:

```text
.hid/scripts/capture.rb   derive current Git facts to stdout
.hid/scripts/validate.rb  run structural and semantic checks
.hid/lib/hid/             small Git, privacy, and validation modules
.hid/tests/               bounded regression tests
```

It does not provide packaging, a server, database, dashboard, MCP, SaaS,
orchestrator, automatic routing, automatic redaction, merge, or release.

## Model Telemetry

Model telemetry remains observational. Task type, risk, and actual model/profile
are recorded when available. Complexity, context, tokens, escalation, and rework
are optional. Cost optimization, automatic selection, and historical
optimization are deferred. `FAST`, `BALANCED`, and `DEEP` remain provider-neutral.

## Known Limits

- A commit cannot contain evidence naming its own commit SHA. Evidence-recording
  commits therefore produce visible staleness against the later metadata tree.
- Git availability and the local `origin/develop` ref affect derived status.
- Privacy detection has false-negative and false-positive risk.
- Some source decisions remain represented in both canonical Markdown and a
  bounded feature snapshot while Shadow Mode is evaluated.
- Human identity is not cryptographically authenticated.

## Pilot Boundary

`LAN-FILE-SHARE-ASSISTANT-AGENT-001` remains `PROPOSED` and is too risky as the
first HID pilot. A later Architecture decision should select an existing, real,
local, reversible, low-risk microfeature with deterministic local validation.
