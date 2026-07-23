# AGENT-PACKAGE-REVIEW-EVIDENCE-001

## State

```text
APPROVED FOR MERGE
branch: feature/agent-package-review-evidence-001
base: cfeec6f83e80b9a34a224cb1863d3e260d9f1e20
base tree: 3e8fa823d9e06e80a4e49ead8d442e35bc271f39
source commit: fbf1a8428f8095e80f37c22c293d0cbc2524602c
source tree: f1c577bf14343207c29b2932c305711305f4f4ad
runtime behavior change: passive in-memory review evidence
local validation: passed
architecture checkpoint: passed
field QA: passed on Mac, TS140, and Proxmox/R5500
final architecture review: passed
```

## Objective

Establish typed evidence for local-registry, language-policy,
dependency-policy, and independent human-review decisions. Evidence is bound to
an operator-local authority and to the exact in-memory manifest and resolved
reference set reviewed.

This feature does not consume package review files, remove package-load
blockers, authorize loading, or execute an agent.

## Ownership

The implementation belongs to `iamine-agent-runtime`:

```text
review_evidence/authority.rs
review_evidence/decision.rs
review_evidence/evidence.rs
review_evidence/error.rs
review_evidence/subject.rs
```

The modules remain separate so authority, decision validation, provenance,
subject binding, and diagnostics do not grow into a single runtime file.

## Trust Boundary

`PackageReviewAuthority` is an operator-local in-memory capability. Every
instance owns a private identity. Evidence can be verified only by the same
authority instance that issued it.

`PackageReviewSubject` binds:

- one exact validated manifest object through `DeclaredAgentPackage`;
- one exact `ResolvedPackageReferences` object produced by the bounded
  resolver.

Reparsing the same manifest, cloning the same resolved bytes, or constructing a
different authority creates a different subject or provenance. Evidence
verification fails for each substitution.

Package-controlled bytes are never parsed as registry, language, dependency,
or human-review decisions. Decision types do not implement deserialization and
contain no caller-provided booleans, paths, reviewer names, or free-form
evidence strings.

## Positive Decision Set

Evidence is established only when all decisions are:

```text
local registry: registry_review_ready
language policy: Rust official allowed
dependency policy: allowed
human review: independent approved
```

Candidate, under-review, blocked, deprecated, experimental, deferred,
needs-justification, missing, self-approved, or rejected states fail closed at
their independent requirement.

## Public Contract

- `PackageReviewAuthority`: issues and verifies authority-bound evidence.
- `PackageReviewSubject`: binds the exact declaration and resolved references.
- `PackageReviewDecisions`: carries four typed independent decisions.
- `PackageReviewEvidence`: opaque established evidence.
- `PackageReviewRequirement`: stable labels for the four owner gates.
- `ReviewEvidenceError`: privacy-safe requirement and stable error code.

Established evidence reports:

```text
load_allowed: false
execution_allowed: false
```

## Non-Bypass Rules

- Package content cannot create trusted review evidence.
- Evidence from another authority is rejected.
- Evidence for another manifest object is rejected.
- Evidence for a cloned or replaced resolution is rejected.
- Review evidence does not imply runtime or hardware compatibility.
- Review evidence does not remove static package-load blockers.
- Review evidence does not install, load, persist, publish, or execute.
- No node, scheduler, worker, P2P, PubSub, model, inference, service, reward,
  reputation, wallet, marketplace, or public-beta behavior changes.

## Privacy

Debug and errors expose only stable enum labels, counts, and redacted authority
or subject placeholders. They do not expose:

- package identifiers or versions;
- package paths or reference paths;
- reference contents;
- reviewer identities;
- host identifiers;
- credentials, keys, tokens, prompts, outputs, or private paths.

## Integration

```text
AGENT-PACKAGE-REFERENCE-RESOLVER-001
-> AGENT-PACKAGE-REVIEW-EVIDENCE-001
-> AGENT-RUNTIME-COMPATIBILITY-GATE-001
```

The later compatibility gate may consume verified review evidence, but only
`AGENT-PACKAGE-LOAD-EVIDENCE-INTEGRATION-001` may reconcile independent
evidence with package-load blockers. Final authorization remains a separate
owner.

## Field QA

The root workflow requires field QA because this feature changes the runtime
crate, even though the behavior is deterministic and in-memory. The exact
source commit must run on:

- Mac development host;
- TS140;
- Proxmox/R5500 guests.

QA must verify focused tests, privacy-safe output, unchanged package-load
blockers, and absence of node/process/network side effects.

## Risks

- Treating a caller-created authority as the configured operator authority.
- Reusing evidence for a different package snapshot.
- Treating established review evidence as load or execution authorization.
- Adding serialized review assertions to package-controlled files.
- Leaking package or reviewer data through diagnostics.
- Combining later compatibility or authorization logic into this owner.

## Success Criteria

- All four review decisions remain independently typed.
- Only the complete positive set can establish evidence.
- Authority, manifest, and resolution substitution fail verification.
- Package-controlled review claims cannot establish evidence.
- Debug and errors remain privacy-safe.
- Package-load behavior stays blocked and unchanged.
- `main.rs` and `cluster_registry.rs` do not change.
- Focused, workspace, quality, and field QA gates pass or receive an explicit
  evidence-backed baseline classification.

## Architecture Checkpoint

```text
owner crate and module boundary: PASS
authority provenance and exact-subject binding: PASS
package-content non-forgeability: PASS
independent decision gates: PASS
privacy-safe diagnostics: PASS
package-load and execution non-bypass: PASS
main.rs delta: 0
cluster_registry.rs delta: 0
largest new production module: 92 lines
local quality gate: PASS WITH WARNINGS
decision: FIELD QA AUTHORIZED
```

Architecture notes one required integration invariant for later consumers:
they must verify evidence against the operator-configured authority. Accepting
an authority supplied alongside package content would discard the provenance
boundary and is forbidden.

## Final Architecture Review

```text
exact source identity: PASS
local validation and quality gate: PASS
Mac field QA: PASS
TS140 field QA: PASS
Proxmox/R5500 field QA: PASS, 4/4 guests
canonical remote work preservation: PASS
runtime side effects: none observed
product failures: none
merge conflicts evaluated: pending merge owner
decision: APPROVED FOR MERGE
```

The source establishes an in-memory evidence type but keeps load and execution
false. The existing static package-load blockers remain unchanged. No package
filesystem value, manifest self-claim, or caller-selected authority can replace
verification against the operator-configured authority.
