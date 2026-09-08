# HID Phase 0 - Control Core Exit Contract

Contract version: `1.0`

Contract adoption: `APPROVED` by explicit human decision
`APPROVE PHASE 0 EXIT CONTRACT` on `2026-09-07`.

Phase closure: `CLOSED` by explicit human decision
`APPROVE HID PHASE 0 CLOSURE` on `2026-09-07`.

This records adoption of the previously proposed P0-01 through P0-08 criteria.
It does not assert that a formal phase contract existed earlier. The first
human decision authorized documenting this contract and reconciling the closed
feature in the roadmap. The later, separate human decision declares HID Phase
0 - Control Core `CLOSED` under this contract. Neither decision authorizes new
capabilities, pilots, automatic merge, or broader HID authority.

## Purpose And Boundary

Phase 0 demonstrates that HID's observation and validation core can accompany
and verify a complete real IAMINE feature lifecycle while preserving human
authority, exact artifact identity, evidence, privacy, temporal ordering, and
Subject/Control Plane separation through published integration and legitimate
feature closure.

The boundary is Shadow Mode, a local repository, and trusted operators. HID
does not independently govern the workflow, grant permissions, or replace
canonical Architecture, QA, roadmap, or human authority. Phase exit is not a
claim of autonomous governance, cryptographic security, cross-project
generalization, or a productized HID platform.

## Authority And Existing Requirements

The phase-level grouping below is the newly adopted contract. Its underlying
requirements already exist in:

- [Agent instructions](../../AGENTS.md) and the
  [canonical workflow](../process/iamine-canonical-workflow.md).
- Constitution implemented in
  [StateInvariants](../../.hid/lib/hid/state_invariants.rb), with operational
  authority rules in [OperationalFacts](../../.hid/lib/hid/operational_facts.rb).
- [Project Policy](../../.hid/project.yaml), including scoped mandates,
  non-interchangeable authority domains, and human milestone-closure authority.
- [HID architecture](hid-shadow-mode.md) and its hardening decisions.
- [HID QA contract](../qa/hid-shadow-mode.md) and
  [feature manifest](../../.hid/features/HID-SHADOW-MODE-001.yaml).

The [product roadmap](../roadmap/iamine-product-roadmap.md) owns phase and
feature reporting. This engineering-process phase does not close a product
milestone, change product sequencing, or waive canonical milestone QA gates.

## Adopted Exit Criteria

Each statement defines its PASS condition. Phase exit requires all eight
criteria to PASS; FAIL or NOT_PROVEN on a required criterion prevents exit.
The criteria are not weakened to accommodate a later implementation result.

| ID | Criterion / PASS condition | Rationale and source basis | Evidence required |
| --- | --- | --- | --- |
| P0-01 | HID preserves canonical authority and process-only scope; it does not infer permissions. | Prevent parallel authority; Project Policy and architecture. | Verified scope, authority rules, and absence of product behavior changes. |
| P0-02 | Control appends preserve the exact subject, baseline, and history; concurrent movement and canonical contamination are rejected. | Prevent candidate drift and history loss; architecture and QA. | Linear ledger, compare-and-swap, before/after invariants, and negative tests. |
| P0-03 | Only typed facts with compatible domains and mandates satisfy gates; invalid inputs fail closed. | Prevent authority transfer between domains; Constitution and Project Policy. | Real facts and regressions for domains, roles, gates, and payloads. |
| P0-04 | Authorization, evidence, and integration bind to exact Git identity; canonical merge and publication are verifiable. | Prevent artifact substitution; QA and canonical workflow. | HEAD/tree, exact parents, candidate as parent2, deterministic tree, containment, and fresh remote verification. |
| P0-05 | One real feature completes every applicable gate, capsule, approval, integration, post-merge validation, publication, and closure. | Prove operability beyond fixtures; canonical workflow and QA. | Current authoritative chain, execution evidence, and validated closure. |
| P0-06 | Privacy is validated before persistence; detected violations are rejected and limitations remain explicit. | Prevent sensitive ledger content; architecture and QA. | Privacy policy, record validation, positive/negative tests, and bounded review. |
| P0-07 | State and next action derive from current facts and respect precedence, revocation, and separate artifact identities. | Prevent snapshot-driven progression; policy and implementation. | Real projection and ordering, denial/reapproval, and stale-evidence regressions. |
| P0-08 | Applicable validation passes and no known open P1/P2 blocks this bounded use. | Require demonstrated readiness without a perfection claim; QA and manifest. | Artifact-bound results and explicit disposition of findings and limitations. |

P0-04 combines provenance and integration integrity to avoid duplicate exit
requirements. P0-06 makes the existing privacy boundary explicit. One real
IAMINE lifecycle is sufficient only together with every other criterion,
including negative regression coverage. It is not evidence of cross-project
generalization; a real denial is not required when revocation regressions pass.

## Pilots And Future Capabilities

Pilots are `POST_PHASE_0_VALIDATION`, not exit requirements. There is no
additional requirement for two features, multiple pilots, or another project
before this contract can pass. Cross-project confidence belongs to separately
authorized empirical validation; no unsupported phase number is assigned.

The following are `NOT_REQUIRED_FOR_PHASE_0`:

| Capabilities | Future destination |
| --- | --- |
| Productized CLI and dashboard | Later UX and productization. |
| Database, MCP, SaaS | Later integrations and platform work. |
| Memory Engine L0-L3 | Later design; documentary organization is not an implemented engine. |
| Advanced telemetry, analytics, and metrics | Later observability; retain current bounded measurements. |
| Operational Model Router | Later routing; OBSERVATION ONLY is acceptable now. |
| Multi-agent orchestration and multi-project management | Later generalization. |
| GitHub/CI, automatic merge, and pilot automation | Separately authorized automation. |

The candidate first bounded pilot is
`QUALITY-SECURITY-EVIDENCE-CONTRACT-001`, as described in the
[pilot boundary](hid-shadow-mode.md#pilot-boundary).
`LAN-FILE-SHARE-ASSISTANT-AGENT-001` retains its place in the product roadmap.
Both remain PROPOSED and require their own scope, review, and authorization.

## Non-Blocking Debt And Limits

| Debt | Blocks Phase 0? | Reason and destination |
| --- | --- | --- |
| Historical STALE evidence and legacy snapshots | NO | No current gate authority; later documentary maintenance without invented history. |
| Operator trust | NO within this scope | Explicit threat-model boundary; reassess authentication/attestation before broadening trust. |
| Privacy false-negative risk | NO absent a concrete blocking violation | No universal detection claim; human review and future regressions remain necessary. |
| Archive lifecycle policy | NO | Current preservation is verified; later authorized maintenance. |
| CLI/UX and advanced metrics | NO | Improvements outside this integrity boundary. |

A concrete violation or newly identified blocking defect invalidates the
applicable PASS. These limits do not grant future exceptions or waive risk
acceptance authority.

## Evidence And Separate Closure Decision

The [exit evidence package](../qa/hid-phase0-exit-evidence.md) identifies the
closed feature, source/integration artifacts, ledger and closure event,
criterion results, validation, publication, and known limitations. It refers
to evidence without copying the full ledger.

Contract approval defined when exit was justified; the later human closure
decision declared Phase 0 `CLOSED` after confirming P0-01 through P0-08. No
new HID gate, runtime state machine, or phase event is introduced. This phase
closure is neither a product-milestone closure nor authorization for a pilot,
product feature, or additional capability.
