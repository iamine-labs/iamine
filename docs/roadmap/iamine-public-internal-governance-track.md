# IAMINE Public and Internal Architecture Governance Track

## State

```text
track: IAMINE-PUBLIC-INTERNAL-ARCHITECTURE-GOVERNANCE-TRACK
state: PROPOSED
classification: CROSS-CUTTING / PRE-PUBLIC-RELEASE
implementation authorization: none
```

This track separates public product and architecture material from restricted
engineering assurance assets. It does not by itself change repository
visibility, licensing, release scope, or artifact publication.

## Candidate Features

| Feature | State | Boundary |
| --- | --- | --- |
| PUBLIC-INTERNAL-ARTIFACT-CLASSIFICATION-001 | PROPOSED | Define public, internal, restricted, and sanitized artifact classes. |
| INTERNAL-ENGINEERING-ASSET-GOVERNANCE-001 | PROPOSED | Define ownership, access, retention, review, and disclosure controls for internal engineering assets. |
| IAMINE-IP-LICENSING-STRATEGY-001 | PROPOSED | Record an approved legal and product strategy before changing licensing or publication scope. |
| THIRD-PARTY-LICENSE-COMPLIANCE-001 | PROPOSED | Inventory and disposition third-party license obligations. |
| THIRD-PARTY-NOTICES-001 | PROPOSED | Produce release-appropriate notices from approved dependency evidence. |
| SECURITY-DISCLOSURE-POLICY-001 | PROPOSED | Define reporting, triage, embargo, remediation, and coordinated disclosure behavior. |
| PUBLIC-ARCHITECTURE-DOCUMENTATION-001 | PROPOSED | Publish an accurate architecture description without restricted topology or assurance internals. |
| PUBLIC-SANITIZED-QA-SUMMARY-001 | PROPOSED | Publish bounded QA claims without private infrastructure or full internal evidence. |
| PUBLIC-SANITIZED-SECURITY-SUMMARY-001 | PROPOSED | Publish scoped security assurance claims without exploit details or restricted evidence. |
| ENGINEERING-ASSURANCE-PLATFORM-BOUNDARY-001 | PROPOSED | Separate IAMINE-specific assurance logic from potentially reusable platform contracts. |
| PROJECT-VALIDATION-PROFILE-CONTRACT-001 | PROPOSED | Define a project-owned profile without assuming a second consumer. |
| ENGINEERING-ASSURANCE-PLATFORM-EXTRACTION-001 | DEFERRED | Requires a second real consumer and an independent extraction decision. |

## Classification Direction

Expected public surfaces may include product binaries, installers, CLI,
Dashboard, public APIs and schemas, user documentation, sanitized architecture,
official agent packages, and release notes.

Expected internal or restricted assets may include internal QA and Security
agents, complete evidence stores, private lab topology, regression and
adversarial corpora, security exceptions, exploit reproductions, credentials,
and operator-specific infrastructure records.

Classification is evidence and policy driven. A public repository path does not
automatically make every generated artifact suitable for publication, and an
internal classification does not authorize removing existing public material.

## Non-Bypass Rules

```text
no secret or credential publication
no private host, address, user, path, VM, or topology publication
no silent license change
no hidden third-party obligation
no universal QA or security claims
no public exploit detail before disclosure disposition
no platform extraction without a second real consumer
```

This track must close its release-relevant classification, licensing,
third-party, and disclosure obligations before a public release gate can rely
on them. It does not reorder the current v0.11.2 runtime sequence.
