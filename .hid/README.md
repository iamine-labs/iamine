# HID v0.0.6 Shadow Mode

HID is a machine-readable observation layer for IAMINE's existing workflow. It
does not enforce gates or replace `AGENTS.md`, the canonical workflow,
Architecture, QA, roadmaps, or explicit human decisions.

## Precedence

When HID and canonical sources disagree, follow the canonical source and record
the divergence. HID cannot authorize scope, exceptions, destructive actions,
merge, release, milestone closure, or public claims. Human silence is not
authorization.

## Data Semantics

- `SOURCE` is introduced by an authority: intent, approved scope, Architecture
  decisions, risk acceptance, and human authorization.
- `DERIVED` is calculated from canonical sources or Git: current identity,
  ancestry, evidence status, and next action.
- `SNAPSHOT` is a derived observation captured at a historical moment: Git
  identity, environment, or test result.

Current Git facts are never stored as a live authority. Run
`.hid/scripts/capture.rb` to derive them. Persisted Git values are snapshots.

## Human Gates

A human gate marked `passed` requires a correlated `human_authorization` event
for the same feature, gate, action, and candidate commit/tree. The actor must be
typed as human and the artifact must be clean. Tooling validates structure and
correlation; it does not authenticate a person's identity cryptographically.
An agent must not manufacture a human event from silence or inference.

## Canonical Integration

A `merged` event records the authorized source candidate separately from the
integration artifact. It is valid only when Git proves that the artifact is a
controlled `--no-ff` merge whose second parent is the exact candidate and that
the artifact is contained in the configured local `develop` branch.

Containment in a side branch is not integration. An event cannot replace the
configured target branch. Fast-forward, squash, rebase, and cherry-pick are not
recognized because the canonical workflow does not authorize those strategies.
Missing or unverifiable canonical refs fail closed. HID does not fetch, so
remote freshness remains explicitly outside this check.

## Evidence

Evidence records an exact commit/tree, bounded coverage, relevant dependencies,
environment, and execution result. Status is derived, never stored:

- `VALID`: recorded commit/tree exists and is the current clean artifact;
- `STALE`: internally valid evidence belongs to another artifact;
- `INVALID`: the commit is missing or its real tree contradicts the record;
- `UNKNOWN`: Git cannot verify the artifact in the current environment.

Stale evidence remains historical evidence. It is not automatically reused.

## Privacy

`.hid/privacy.yaml` defines `ALLOW`, `REDACT`, and `NEVER_STORE`. The validator
fails on detected violations, warns on values requiring review/redaction, and
never rewrites data. Pattern matching can miss secrets; human review remains
required before persistence and push.

## Append-only

`events.jsonl` follows an append-only policy. When a canonical baseline already
contains the log, validation checks that the baseline is an exact prefix. When
no baseline is available, the result is visibly `not_checked`. This is neither
tamper-proof storage nor a cryptographic immutability guarantee.

## Commands

```bash
ruby .hid/scripts/capture.rb
ruby .hid/scripts/validate.rb
ruby .hid/tests/validator_test.rb
```

The scripts use the Ruby standard library. Validation supplements rather than
replaces IAMINE repository tests and QA.
