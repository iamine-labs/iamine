# HID v0.0.1 Shadow Mode

HID is an experimental, machine-readable observation layer for IAMINE's
existing human and agent engineering workflow. During v0.0.1, the canonical
workflow, Architecture, QA, roadmaps, and explicit human gates retain all
authority.

## Precedence

When HID and canonical documentation disagree:

1. follow `AGENTS.md` and `docs/process/iamine-canonical-workflow.md`;
2. follow the canonical roadmap for product sequence and authorization;
3. record the HID discrepancy as a finding;
4. do not silently rewrite either history or authority.

HID cannot authorize scope, exceptions, destructive actions, merge, release,
milestone closure, or public claims. Human silence is never authorization.

## Layout

```text
.hid/project.yaml                 L0 constitution and stable references
.hid/features/<FEATURE-ID>.yaml   L1 active feature capsule
.hid/evidence/<EVIDENCE-ID>.json  exact-artifact evidence records
.hid/events.jsonl                 append-only observations
.hid/templates/                   non-evidence examples
.hid/scripts/validate.rb          local structural and privacy validator
```

Relevant Architecture, ADR-like decisions, QA, and dependency documents remain
L2 knowledge in their canonical repository locations. Historical logs and old
feature evidence remain L3 archive and are loaded only when needed.

## Artifact Semantics

Git values are timestamped snapshots, not self-updating fields. Evidence binds
to its subject commit and tree, while the later commit that records the evidence
may have a different documentation-only tree. That difference never carries
evidence forward automatically.

Feature manifests reference canonical documents instead of copying their full
content. `next_action` is explicit in v0.0.1 and is not yet calculated or
enforced.

## Event Rules

`events.jsonl` is append-only. Correct a bad observation with a later event;
do not edit history to erase it. Metadata must be bounded and must not include
prompt content, tokens, credentials, personal paths, hostnames, network
addresses, machine identifiers, or raw QA logs.

## Validation

Run:

```bash
ruby .hid/scripts/validate.rb
```

The validator uses only the Ruby standard library. It parses all HID YAML,
JSON, and JSONL; checks canonical references and lifecycle vocabulary; validates
feature, event, and evidence invariants; checks append-only history against the
feature base when available; and rejects common sensitive-value shapes.

HID validation does not replace repository tests or QA.
