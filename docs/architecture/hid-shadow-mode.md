# HID Shadow Mode Architecture

Feature: `HID-SHADOW-MODE-001`

Operational state is derived from subject requirements, control facts and Git,
using the canonical workflow. This document does not duplicate current Git identity,
gate result, validation count, or next action.

## Purpose

HID v0.0.10 is a bounded machine-readable observation layer. It captures facts
needed to study a future control plane while the canonical IAMINE workflow,
Architecture, QA, roadmaps, and explicit human gates retain all authority.

It changes no product behavior. Runtime, Core, agents, models, networking,
inference, scheduler, dashboard, protocols, and product security are excluded.

## Data Ownership

HID distinguishes:

```text
SOURCE   subject intent, scope, static policy and requirements
DERIVED  current Git facts, gate outcomes, operational state and next action
SNAPSHOT derived facts captured at a historical moment
```

Current branch, HEAD, tree, dirty state, and ancestry come from Git through
`.hid/scripts/capture.rb`. Manifests may retain named snapshots but must not
present them as self-updating current state or use them as Human Gate authority.

The canonical lifecycle is parsed from
`docs/process/iamine-canonical-workflow.md`; `.hid/project.yaml` no longer keeps
a second editable list of lifecycle states.

## Control-Plane Ledger

The complete candidate Git commit/tree is the subject plane. Human decisions,
merge observations, post-merge results, closure, and later runtime observations
are control-plane records. Persisting a control record must not mutate the
subject it describes.

`.hid/events.jsonl` is the immutable pre-v0.0.8 baseline. Its configured blob
identity is validated from the complete file. New events are stored on the
dedicated `refs/heads/hid/control-plane` ref and are never written back to the
baseline or merged into `develop`.

Each control-ledger commit is linear, contains only `events.jsonl`, preserves
the previous JSONL content, and adds exactly one line. The supported writer
validates event schema, uniqueness, privacy, and exact authorization subject
before writing Git objects, then advances only the control ref with
compare-and-swap. Concurrent ref movement is `CONTROL_LEDGER_CHANGED` and fails
closed. HEAD, candidate tree, index, working tree, feature ref, and `develop`
remain unchanged.

The effective stream is the complete baseline followed by control commits in
first-parent order. Timestamp values are validated metadata but never reorder
events. A live event receives derived `control_record.ledger_commit` identity
when read; that ledger commit does not replace its `artifact` subject.

The validator rejects nonlinear histories, rewritten prefixes, commits that add
zero or multiple events, unexpected paths, and any current or historical
control-ledger commit contained in `develop`. Control-ref containment cannot
satisfy canonical integration.

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

A derived `passed` human gate requires the matching event and an eligible prior
capsule. A manifest status has no operational authority. Projection observes
the decision but does not grant HID permission to act.
Tooling validates structure and artifact correlation, not cryptographic human
identity. Agents must not represent themselves as humans or infer approval from
silence, tests, or prior conversation.

Human authorization targets the current clean Git candidate derived at
validation time, not `git.candidate_snapshot`. The snapshot remains historical
evidence metadata. For the same feature, gate, action, and current artifact,
the last relevant `human_authorization` event in append-only log order is the
effective decision: a later denial revokes an approval and a later approval can
supersede a denial. Decisions for older artifacts are stale and never carry
forward.

Privileged lifecycle states combine a small non-configurable HID Constitution
with the declarative project policy. The Constitution always requires local
validation, final review, human merge, and exact-candidate human authorization.
The project policy may add gates, including risk-specific gates, but omission,
`required: false`, or a modified human-gate action cannot remove the minimum.
`MERGED`, `POST-MERGE VALIDATION`, and `MERGED / VALIDATED / CLOSED` add the
corresponding merge, post-merge validation, and closure events. The canonical
workflow has no standalone `MERGING` or `CLOSED` state, so HID invents neither.

`next_action` follows the first missing canonical prerequisite, not a mutable
state label. Unknown requirements fail closed; invalid lifecycle facts cannot
yield a privileged action. Low-level invariant diagnostics remain separate
from the public runtime projection.

Privileged states are derived from the canonical lifecycle beginning at
`APPROVED FOR MERGE`. Every derived privileged state must have a declarative
policy entry. Missing coverage is `POLICY_INCOMPLETE`, fails validation at
startup, and derives `policy_incomplete` rather than a privileged action.

Lifecycle events supporting `MERGED`, `POST-MERGE VALIDATION`, or closure are
not accepted by name alone. Their commit must exist, the recorded tree must
match Git, and the integration artifact must be linked to the exact current
candidate by the canonical merge strategy. Post-merge validation and closure
events must bind to that valid merge artifact. Missing, unverifiable,
mismatched, unrelated, or noncanonical artifacts fail closed.

## Canonical Integration Integrity

The Human Gate authorizes the current candidate commit/tree. A later `merged`
event records that source separately from the integration artifact and observed
target. The event cannot redefine the constitutional target: it must match
`project.integration_branch`, currently `develop`.

The canonical workflow requires `git merge --no-ff` into `develop`, with no
additional changes after validation. HID therefore recognizes only
`strategy: no_ff_merge`. A valid integration artifact must:

1. exist with its recorded tree;
2. be a two-parent merge commit;
3. have the exact authorized candidate as its second parent;
4. be contained in the local `refs/heads/develop` history;
5. have the exact tree Git computes for a clean merge of its first parent and
   the authorized candidate.

This rejects both a linear descendant with arbitrary changes and a correctly
shaped merge commit that exists only on a side branch. Fast-forward, squash,
rebase, and cherry-pick remain unsupported and fail closed; HID does not infer
integration from similar content. If the local canonical ref is missing or Git
cannot verify it, the state is not eligible for `MERGED`.

HID asks the repository's Git implementation to calculate the expected tree
with `git merge-tree --write-tree <parent1> <candidate>`. Exit status zero and a
verifiable tree object represent a clean merge. Exit status one represents a
conflict. Other failures, malformed output, or an unavailable capability are
not verifiable. HID never accepts topology alone as a fallback.

The integration commit tree must equal the computed tree SHA. Content-addressed
tree identity covers additions, modifications, deletions, modes, and nested
trees without a custom diff parser. The event cannot declare the expected tree.
The command runs against the same repository and configuration as validation;
repository attributes and merge drivers therefore remain part of the bounded
Git environment. A conflict or driver result that cannot produce a clean,
verifiable tree fails closed.

The canonical workflow requires stopping on conflict and forbids changes after
the validated candidate. v0.0.7 consequently does not recognize manual conflict
resolution. A manually constructed two-parent commit remains acceptable only
when its graph, canonical containment, and tree are identical to the clean Git
result; HID observes Git facts rather than terminal history.

This check observes the available local branch. It does not fetch or establish
that local `develop` is current with the remote. Remote freshness remains a
separate, explicit merge-owner responsibility.

Effective baseline-plus-ledger position is the lifecycle sequence source. For the
same feature and relevant artifacts, a valid chain is:

```text
effective approved human_merge authorization
< merged
< post_merge_validation_passed
< feature_closed
```

An approval followed by denial before merge cannot support the transition. A
denial after a valid merge does not erase the historical Git event; it prevents
later actions that require current authority. Events from another feature or an
older candidate do not satisfy this chain.

The v0.0.2 correction of an unsupported `architecture: passed` remains historical.
v0.0.9 does not rewrite that history or promote those legacy statuses.

## Operational State And Gate Authority

Architecture Review #9 identified an operational self-reference after the first
real external human approval: the ledger advanced but runtime state and gate
statuses still required subject-manifest edits. Synthetic invariant fixtures
did not establish operational readiness.

The explicit v0.0.9 policy cutover keeps gate requirements in the subject and
moves outcome authority to typed schema `0.0.3` control facts. `state.current`
and `gates.*.status` are non-authoritative legacy snapshots. They cannot grant
or veto authority; mismatches are visible diagnostics. Static requirements,
canonical target, merge strategy and Constitution cannot be changed by events.

Responsibilities are split among small modules:

- `OperationalFacts`: typed outcome/evidence validation and policy-scoped mandates.
- `GateProjection`: requirements, latest relevant outcomes, canonical phase order,
  derived state/next action, and capsule eligibility.
- `LifecycleProjection`: prerequisite snapshots at merge, post-merge ordering,
  closure, and the existing exact Git integration invariants.
- `ControlLedger`: unchanged storage protocol, linear append and compare-and-swap.

The canonical order is development authorization (`architecture`), implementation,
local validation, Architecture checkpoint, Field QA when required, final review,
capsule and human merge. Implementation/checkpoint requirements represent
existing canonical phases, not new product gates. Additional required gates
need explicit authority rules and are checked before the final human decision.

Reviews reuse `architecture_approved`/`architecture_changes_required` with an
explicit gate, phase, pass/fail/blocked result, and mandate. Review #8 maps to
`final_review` only for its original artifact; it is not initial development
authorization and no retrospective fact is recorded by this implementation.

The Constitution explicitly separates review authority from human authority.
Final review remains mandatory, under a policy-authorized architect mandate;
human merge remains mandatory, bound to the exact candidate and a real human
decision. Project Policy may add requirements but cannot remove these minimums
or replace architect authority with a developer/system role. Mandates specify
feature, gates, actor types and roles. This bounds the trusted operator model;
it does not authenticate the claimed actor or report cryptographically.

Every new operational result carries bounded external evidence: feature,
exact HEAD/tree, kind, result and a report reference. No evidence reuse is
inferred from ancestry, similar trees, historical snapshots or stale files.
An event name alone has no authority. Latest relevant ledger order controls
negative reviews and reapproval; a later negative result blocks progression.

Capsule eligibility requires all applicable non-human prerequisites, fresh
evidence and a clean exact candidate. The request records the prerequisite
event IDs. A human approval must reference an earlier eligible capsule with
the same prerequisites; changed reviews require a new capsule and approval.
Early requests/approvals fail before persistence. Denials require no capsule.
This ordering applies prospectively; `HID-EVENT-0060` remains unmodified history
for the old artifact and cannot authorize the new candidate. No new real events
are written during Development.

Runtime projection reports the source candidate; after a real integration the
validator is run from the source checkout, keeping the separate integration
identity verified by Git. A valid past merge is not erased by a later denial.
Post-merge validation must precede Architecture closure and bind that same
integration artifact. The realistic temporary-repository E2E, not the synthetic
invariant fixtures, is the operational-readiness regression.

## Typed Facts And Authority Domains

Architecture Review #10 reproduced a cross-domain P1 in v0.0.9: human-event
validation returned before checking an extra `outcome`, while GateProjection
selected raw payloads by `outcome.gate`. A human denial could therefore invent
review or validation results, even without evidence. Five such records could
unlock a capsule and a later human merge decision.

v0.0.10 closes that route without changing the ledger protocol. Event type
selects a fixed authority domain in `AuthorityDomains`, not an event-supplied
domain or a gate label. `OperationalFacts` validates the complete payload before
constructing a detached, recursively frozen `OperationalFact` with feature,
subject, domain, gate, result, phase, authority, evidence and source event.

| Gate | Required authority kind | Canonical event |
| --- | --- | --- |
| architecture | REVIEW_VERDICT | architecture_approved / architecture_changes_required |
| implementation | LIFECYCLE_FACT | implementation_completed |
| local_validation | VALIDATION_RESULT | validation_passed / validation_failed |
| architecture_checkpoint | REVIEW_VERDICT | architecture_approved / architecture_changes_required |
| field_qa | QA_RESULT | field_qa_passed / field_qa_blocked |
| final_review | REVIEW_VERDICT | architecture_approved / architecture_changes_required |
| human_merge | HUMAN_DECISION | human_authorization |
| merged | INTEGRATION_FACT | merged |
| post_merge_validation | VALIDATION_RESULT | post_merge_validation_passed / validation_failed |
| closure | LIFECYCLE_FACT | feature_closed |

Initial Architecture and final review remain separate phases under the actual
canonical Architecture mandate. The current policy does not define a human
development-authorization event; `human_authorization` is exclusively a human
merge decision. No generic IAM/RBAC or additional human gate is introduced.
`human_decision_requested` has domain `CAPSULE_REQUEST` and emits no gate result.

Top-level payloads are discriminated allowlists. Only operational outcomes may
carry `outcome`; only human decisions may carry `authorization`/`capsule_id`;
only capsule requests may carry `capsule`; only merge facts may carry
`integration`. Nested authorization, outcome, evidence, artifact and capsule
fields are also bounded allowlists. Extra `domain`, `authority_kind`,
`review_type`, `review_phase`, `verdict`, `validation_result`, `qa_result` or
contradictory `result` fields cannot override canonical semantics. Innocuous
metadata remains non-authoritative and subject to the existing privacy policy.

The writer validates these contracts before compare-and-swap. Projection
independently normalizes raw input, verifies Git artifact identity, and selects
only typed facts matching the expected domain, gate, feature and subject.
Ledger ordering and latest-result rules then operate on those facts. Lifecycle
projection uses typed facts as well; existing exact Git integration invariants
remain unchanged. Additional policy gates require an explicit compatible domain;
minimum constitutional domains cannot be weakened.

All 59 baseline records and `HID-EVENT-0060` remain unmodified history. Legacy
schemas normalize to no new operational fact, rather than being guessed into a
privileged domain. This implementation does not persist real prerequisite,
review or human-authorization events. It requires a new exact-candidate review
and human decision before any integration.

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

The recursive privacy walker applies both field-name and text-content rules.
Obvious secret assignments inside arbitrary strings are violations. Complete
content under `prompt`, `model_prompt`, `response`, `model_response`, or
`completion` is prohibited, while bounded metadata such as IDs, hashes, sizes,
token counts, profile, and selection reason remains allowed. IPv6 is parsed
with Ruby's standard `IPAddr`, including compressed addresses, and produces a
`REDACT` warning rather than a silent pass.

## Append-only Policy

The baseline event log is locked to its configured Git blob after cutover. The
live ledger enforces one-event-per-commit prefix growth and compare-and-swap.
HID does not fetch automatically or claim that local refs are current on the
remote. The policy is not tamper-proof or cryptographically immutable against
an operator who can rewrite refs.

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
- Git availability and the local `origin/develop` tracking ref affect derived
  status; remote freshness is explicitly `not_verified`.
- Privacy detection has false-negative and false-positive risk.
- Some source decisions remain represented in both canonical Markdown and a
  bounded feature snapshot while Shadow Mode is evaluated.
- Human identity is not cryptographically authenticated.
- Canonical containment verifies the local `develop` ref, not remote freshness.
- Deterministic tree validation requires Git `merge-tree --write-tree`; an
  unavailable or failing capability blocks privileged integration state.
- Control-ledger append safety assumes Git ref updates are not force-rewritten
  outside the supported compare-and-swap writer.

## Future Human Gate UX

`Human Gate Approval Capsule` remains captured for future implementation. It
will present WHAT, WHY, CHANGE, EXCLUDES, IMPACT, RISK, EVIDENCE, FINDINGS,
LIMITATIONS, ARTIFACT, and ACTION while the human emits only APPROVE or DENY.
v0.0.8 implements storage separation, not that UI or a general CLI.

## Pilot Boundary

`QUALITY-SECURITY-EVIDENCE-CONTRACT-001` remains `PROPOSED` and is the candidate
for the first bounded pilot only after Architecture Review and a real Human Gate.
`LAN-FILE-SHARE-ASSISTANT-AGENT-001` also remains `PROPOSED` and is not started.
