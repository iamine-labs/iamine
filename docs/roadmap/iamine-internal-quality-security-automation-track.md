# IAMINE Internal Quality and Security Automation Track

## State

```text
track: IAMINE-INTERNAL-QUALITY-SECURITY-AUTOMATION-TRACK
state: PROPOSED
classification: INTERNAL / PRIVILEGED / CROSS-CUTTING
user availability: none
marketplace eligibility: none
implementation authorization: none
```

This track records a future internal engineering platform. It does not replace
the canonical feature lifecycle, Architecture authority, independent QA,
manual field QA, or merge ownership. It does not authorize source changes,
shell access, commits, pushes, merges, deployments, feature closure, or release
approval.

Implementation of the internal agents starts only after:

```text
V0.11.2-AGENT-RUNTIME-BASELINE-MILESTONE-QA-001
```

Architecture-only planning and the separate deterministic Security/CI
maintenance track may proceed earlier. This track is not a new prerequisite
for functional P0 agents unless Architecture later promotes a bounded
prerequisite with explicit release impact.

## Shared Foundation

| Feature | State | Boundary |
| --- | --- | --- |
| QUALITY-SECURITY-EVIDENCE-CONTRACT-001 | PROPOSED | Versioned, bounded, redacted evidence shared by QA and Security without merging their verdicts. |
| QUALITY-SECURITY-EVIDENCE-STORE-001 | PROPOSED | Append-only evidence records with retention, redaction, integrity, and access controls. |
| INTERNAL-VALIDATION-ALLOWLISTED-RUNNER-001 | PROPOSED | Typed operations only; no arbitrary shell or dynamic command construction. |
| INTERNAL-VALIDATION-SANDBOX-001 | PROPOSED | Per-operation timeout, resource, filesystem, environment, network, output, and cleanup limits. |
| QUALITY-SECURITY-FINDING-LIFECYCLE-001 | PROPOSED | Triage, disposition, remediation, regression, and closure evidence. |
| ITERATION-LEARNING-EVIDENCE-001 | PROPOSED | Approved lessons tied to exact feature, commit, tree, policy, corpus, tool, and environment versions. |
| QUALITY-SECURITY-REGRESSION-CORPUS-001 | PROPOSED | Versioned functional regression cases accepted by human authority. |
| QUALITY-SECURITY-ADVERSARIAL-CORPUS-001 | PROPOSED | Versioned authorized abuse and bypass cases accepted by human authority. |
| QUALITY-SECURITY-FEEDBACK-LOOP-001 | PROPOSED | Retrieve approved prior evidence without self-modifying agent behavior. |
| QUALITY-SECURITY-CROSS-REVIEW-001 | PROPOSED | Each agent may review the other's output; neither may close its own findings. |

The runner contract must follow:

```text
Typed Validation Request
-> Policy Gate
-> Allowlisted Runner
-> Deterministic Tool Adapter
-> Bounded Evidence
```

Every operation declares:

```text
working directory
timeout
resource limits
network policy
environment allowlist
output limit
cleanup behavior
required authority
```

## Code QA Agent

| Feature | State |
| --- | --- |
| QA-AGENT-ARCHITECTURE-001 | PROPOSED |
| QA-POLICY-ENGINE-001 | PROPOSED |
| QA-IMPACT-ANALYZER-001 | PROPOSED |
| QA-REGRESSION-MATRIX-001 | PROPOSED |
| QA-STATIC-ANALYSIS-INTEGRATION-001 | PROPOSED |
| QA-DYNAMIC-VALIDATION-INTEGRATION-001 | PROPOSED |
| IAMINE-CODE-QA-AGENT-001-INTERNAL | PROPOSED |
| QA-REPORT-GENERATOR-001 | PROPOSED |
| QA-CI-INTEGRATION-001 | PROPOSED |
| QA-AGENT-MILESTONE-QA-001 | PROPOSED |

Allowed recommendations:

```text
PASS_RECOMMENDED
CHANGES_REQUIRED
BLOCKED
INCONCLUSIVE
MANUAL_QA_REQUIRED
FIELD_QA_REQUIRED
```

The Code QA Agent cannot emit `APPROVED_FOR_MERGE`, `MERGED`, `CLOSED`, or
`RELEASE_APPROVED`.

## Security Assurance Agent

| Feature | State |
| --- | --- |
| SECURITY-AGENT-ARCHITECTURE-001 | PROPOSED |
| SECURITY-THREAT-MODEL-001 | PROPOSED |
| SECURITY-POLICY-ENGINE-001 | PROPOSED |
| SECURITY-IMPACT-ANALYZER-001 | PROPOSED |
| SECURITY-STATIC-ANALYSIS-INTEGRATION-001 | PROPOSED |
| SECURITY-DEPENDENCY-SUPPLY-CHAIN-INTEGRATION-001 | PROPOSED |
| SECURITY-SECRETS-SCAN-INTEGRATION-001 | PROPOSED |
| SECURITY-DYNAMIC-ANALYSIS-INTEGRATION-001 | PROPOSED |
| SECURITY-FUZZING-PROPERTY-TESTING-001 | PROPOSED |
| SECURITY-ADVERSARIAL-LAB-001 | PROPOSED |
| IAMINE-SECURITY-ASSURANCE-AGENT-001-INTERNAL | PROPOSED |
| SECURITY-REPORT-GENERATOR-001 | PROPOSED |
| SECURITY-CI-INTEGRATION-001 | PROPOSED |
| SECURITY-AGENT-MILESTONE-QA-001 | PROPOSED |

Security may additionally report:

```text
SECURITY_REVIEW_REQUIRED
CONFIRMED_SECURITY_FINDING
SECURITY_EXCEPTION_REQUIRED
```

It cannot claim that IAMINE is universally secure or that no vulnerability
exists. Reports must delimit the executed scope, tools, environment, cases,
skipped checks, and residual risk.

## Evidence and Learning Rules

Agents do not retrain, edit policies, or update corpora automatically.
Iteration learning uses approved, versioned evidence:

```text
feature and exact commit/tree
QA and Security run identifiers
agent, policy, corpus, schema, and tool versions
positive and negative cases
confirmed findings and dispositions
false positives and false negatives
flaky tests
approved regression and adversarial cases
unresolved risks
approving authority
```

Only independently approved lessons may affect future iterations. A positive
result records what was proven and what remains uncovered. A confirmed defect
should produce a reproducible case and regression or adversarial coverage when
feasible.

Flaky tests use `QUARANTINED_FLAKY` and do not count as passing. Findings must
not transition directly from detection to closure without triage,
disposition, remediation evidence where required, and independent validation.
Suppressions must be scoped, owned, evidence-backed, and time-bounded.

## Bootstrap and Readiness

The first internal QA and Security agents cannot validate themselves. Bootstrap
requires deterministic tools, manual QA, independent Architecture and Security
review, exact-tree validation, field QA when applicable, and post-merge
validation.

```text
INTERNAL-QUALITY-SECURITY-READINESS-GATE-001
state: PROPOSED
```

This gate establishes readiness of the internal automation platform. It does
not grant merge, release, source-write, or feature-closure authority, and it
does not silently become the closure gate for v0.12.0 or functional P0 agents.
