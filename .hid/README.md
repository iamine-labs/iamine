# HID v0.0.10 Shadow Mode

HID is a machine-readable observation layer for IAMINE's existing workflow. It
does not enforce gates or replace `AGENTS.md`, the canonical workflow,
Architecture, QA, roadmaps, or explicit human decisions.

## Precedence

When HID and canonical sources disagree, follow the canonical source and record
the divergence. HID cannot authorize scope, exceptions, destructive actions,
merge, release, milestone closure, or public claims. Human silence is not
authorization.

## Data Semantics

- Subject `SOURCE` defines identity, intent, approved scope, risk, gate
  requirements, Constitution, Project Policy, and reviewer mandates.
- `DERIVED` is calculated from canonical sources or Git: current identity,
  ancestry, evidence status, operational gate outcomes, state, and next action.
- `SNAPSHOT` is a derived observation captured at a historical moment: Git
  identity, environment, or test result.

Current Git facts are never stored as a live authority. Run
`.hid/scripts/capture.rb` to derive them. Persisted Git values are snapshots.

## Human Gates

A derived human gate `passed` requires a correlated `human_authorization` event
for the same feature, gate, action, and candidate commit/tree. The actor must be
typed as human and the artifact must be clean. Tooling validates structure and
correlation; it does not authenticate a person's identity cryptographically.
An agent must not manufacture a human event from silence or inference.

Human decisions and runtime lifecycle observations are persisted outside the
subject artifact. `Validator#append_control_event` validates the proposed event,
privacy policy, event identity, exact current candidate, and control-ref
compare-and-swap before advancing the ledger. The resulting ledger commit is
control-record identity only; it never replaces or extends the authorized
subject commit/tree.

## Operational Projection

`GateProjection` reads requirements from the subject and typed operational facts
from the Control Ledger. Manifest `state.current` and `gates.*.status` are
non-authoritative legacy snapshots. Differences produce diagnostics; changing
them cannot grant or veto runtime authority. No derived view is written back.

New control events use schema `0.0.3`. A typed `outcome` includes `gate`, `phase`,
`result`, `mandate`, and bounded external `evidence` (feature, exact artifact,
kind, result, reference). Reviews reuse `architecture_approved` or
`architecture_changes_required`; validation reuses the existing validation
events. No manifest/evidence-index update is required to record an outcome.
Unknown requirements or authority rules cannot unlock a privileged action.
Additional pre-merge requirements must be explicit typed gates; unresolved
event-only policy extensions fail closed rather than being silently omitted.

v0.0.10 builds an immutable `OperationalFact` only after dispatching on the
event type and validating its discriminated payload, domain, gate policy,
mandate, phase, result and evidence. Gate and lifecycle projections normalize
their input independently of the writer and consume these facts, never raw
`outcome.gate` as authority. Unknown event types/domains fail closed.

`authority_kind` is explicit in Project Policy. Human decisions, review
verdicts, validation results, QA results, lifecycle facts and integration facts
are not interchangeable. Capsule requests are a separate non-gate domain.
Human events allow `authorization` and an optional `capsule_id`, not `outcome`,
review/validation/QA fields or integration data. Outcomes have one canonical
`result`; duplicate result/verdict/domain fields are rejected. Metadata is
observational only and still passes privacy-before-write.

`architecture` is development authorization. `architecture_checkpoint` is the
canonical checkpoint before QA. `final_review` is the final Architecture
decision; it does not retroactively authorize development. Review #8 belongs
to that final phase for its original candidate, but is not imported here.
The current workflow represents initial development authorization as a
policy-mandated Architecture verdict (`REVIEW_VERDICT`, phase
`development_authorization`), not a `human_authorization`. Only `human_merge`
accepts `HUMAN_DECISION`; no additional human development gate is introduced.

Reviewer mandates are scoped by feature, gate, phase, actor type, and role in
Project Policy. A role label without that policy permission is insufficient.
This is the existing operator-trusted local model, not identity authentication
or cryptographic reviewer attestation. Human merge still requires an actual
human decision; a reviewer mandate cannot create it.

`human_gate_eligibility` returns `READY` with the exact prerequisite event IDs,
or `NOT_READY` with `APPROVAL_CAPSULE_NOT_READY` and the missing gate. Future
`human_decision_requested` events must bind that snapshot to the exact candidate
and target. Approval must reference that earlier capsule, after all non-human
prerequisites; new review outcomes require a new capsule and approval. Denial
needs no capsule. Ledger position, not timestamps, determines precedence.

Historical schemas remain readable but cannot satisfy new operational gates.
`HID-EVENT-0060` remains untouched historical human merge approval for its exact
old artifact; it is neither a review nor authorization for a new artifact.
The full capsule UI/CLI remains captured for future implementation; only model
eligibility and the supported writer's ordering checks are implemented.

## Subject And Control Planes

The subject plane is the complete Git identity being reviewed: source, HID
files, feature branch, commit, and tree. No path is excluded and no pseudo-tree
or descendant equivalence is recognized.

The control plane is the dedicated `refs/heads/hid/control-plane` ref. Each
linear ledger commit contains only `events.jsonl`, preserves the previous
content, and appends exactly one event. Updates use `git update-ref` with the
observed prior value, so concurrent movement fails as `CONTROL_LEDGER_CHANGED`.
The write uses Git plumbing without checking out the control ref and does not
modify the subject HEAD, tree, index, working tree, feature ref, or `develop`.

The effective event order is:

```text
all baseline .hid/events.jsonl events
<
all control-ledger events in first-parent commit order
```

Timestamps remain validated metadata but do not define lifecycle order. Every
live event is associated in memory with the ledger commit that introduced it.
No control-ledger commit may be contained in `develop`; canonical integration
continues to require the real subject integration artifact.

## Canonical Integration

A `merged` event records the authorized source candidate separately from the
integration artifact. It is valid only when Git proves that the artifact is a
controlled `--no-ff` merge whose second parent is the exact candidate and that
the artifact is contained in the configured local `develop` branch. Git must
also reproduce a clean merge of the first parent and candidate, and that
deterministic tree must equal the integration commit tree exactly.

Containment in a side branch is not integration. An event cannot replace the
configured target branch. Fast-forward, squash, rebase, and cherry-pick are not
recognized because the canonical workflow does not authorize those strategies.
Missing or unverifiable canonical refs fail closed. HID does not fetch, so
remote freshness remains explicitly outside this check.

The expected tree comes from `git merge-tree --write-tree` in the repository;
it is never supplied by an event or reconstructed in Ruby. Conflicts, command
failure, unsupported Git environments, and tree mismatches fail closed. Manual
conflict resolution is not supported because the canonical workflow requires a
stop when conflicts appear.

## Evidence

Evidence records an exact commit/tree, bounded coverage, relevant dependencies,
environment, and execution result. Status is derived, never stored:

- `VALID`: recorded commit/tree exists and is the current clean artifact;
- `STALE`: internally valid evidence belongs to another artifact;
- `INVALID`: the commit is missing or its real tree contradicts the record;
- `UNKNOWN`: Git cannot verify the artifact in the current environment.

Stale evidence remains historical evidence. It is not automatically reused.
New operational evidence is embedded in the bounded control event; its feature,
artifact, validation/review kind and result must match the outcome exactly.
Its reference identifies an operator-held report, not an instruction to load
arbitrary paths or fetch a URL. Evidence authenticity remains an operator duty.

## Privacy

`.hid/privacy.yaml` defines `ALLOW`, `REDACT`, and `NEVER_STORE`. The validator
fails on detected violations, warns on values requiring review/redaction, and
never rewrites data. Pattern matching can miss secrets; human review remains
required before persistence and push.

## Append-only

`.hid/events.jsonl` is the locked pre-v0.0.8 baseline. Its configured Git blob
must remain exact and no new runtime event is written there. Live events use
the control ledger, where every commit must preserve the prior JSONL content
and append exactly one line. Compare-and-swap prevents supported writers from
silently losing a concurrent update. This remains policy enforcement, not
cryptographic immutability against an operator who can rewrite refs.

## Commands

```bash
ruby .hid/scripts/capture.rb
ruby .hid/scripts/validate.rb
ruby .hid/tests/validator_test.rb
ruby .hid/tests/gate_projection_test.rb
ruby .hid/tests/review_authority_test.rb
ruby .hid/tests/realistic_lifecycle_test.rb
```

The scripts use the Ruby standard library. Validation supplements rather than
replaces IAMINE repository tests and QA.
Post-merge validation uses the unchanged source candidate checkout; the
integration artifact is a separate Git identity, never a replacement candidate.
