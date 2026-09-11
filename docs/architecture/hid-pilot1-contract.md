# HID Pilot 1 Experimental Contract

Contract version: `0.1` - local review draft, not an activation decision.

| Identity | Value |
| --- | --- |
| pilot_id | QUALITY-SECURITY-EVIDENCE-CONTRACT-001 |
| pilot status | PROPOSED / NOT_STARTED |
| subject_feature_id | SECRETS-SCAN-CI-GATE-REPAIR-001 |
| subject status | PROPOSED / NOT_STARTED |
| authorization | APPROVE PILOT SUBJECT ONBOARDING PREPARATION; local preparation only |
| HID Phase 0 | CLOSED under exit contract v1.0; not reopened |
| Phase 1 relationship | PHASE_RELATIONSHIP_UNDEFINED; NOT_AUTHORIZED |
| Model Router | OBSERVATION ONLY |

This contract is a SOURCE proposal for human review. The initial preparation
approval covered exactly three onboarding files: `.hid/project.yaml`,
`.hid/features/SECRETS-SCAN-CI-GATE-REPAIR-001.yaml`, and this contract. A
later, separate human-authorized harness scope extension admitted exactly two
test files: `.hid/tests/realistic_lifecycle_test.rb` and
`.hid/tests/control_ledger_test.rb`. The current candidate therefore contains
five reviewed files; implicit scope expansion: NO. Neither authorization starts
either lifecycle, approves an operational gate, authorizes a commit, or grants
merge authority.
No pilot_id field is added to HID event schemas. Machine facts, when separately
authorized, identify only the subject_feature_id. Pilot outcomes remain
separate from subject feature closure. Repairing the scan alone does not close
the broader shared evidence-contract roadmap feature.

## Sources And Original Intent

Authority order: [Constitution](../../.hid/lib/hid/state_invariants.rb),
[Project Policy](../../.hid/project.yaml),
[canonical workflow](../process/iamine-canonical-workflow.md),
[Phase 0 exit contract](hid-phase0-exit-contract.md),
[HID architecture](hid-shadow-mode.md), [HID QA](../qa/hid-shadow-mode.md),
and the [product roadmap](../roadmap/iamine-product-roadmap.md).

EXPLICITLY DOCUMENTED: the [internal assurance track](../roadmap/iamine-internal-quality-security-automation-track.md)
proposes versioned, bounded, redacted evidence shared by QA and Security without
merging their verdicts. The HID architecture identifies this ID as the first
candidate pilot after Architecture review and a real human decision.

INFERRED FROM ARCHITECTURE: another real lifecycle can expose the practical
cost and limits of exact identity, separated authority, and evidence handling.
HID remains an observation layer; canonical roles govern the work.

NEW RECOMMENDATION: exercise the evidence contract during the bounded repair
already proposed in the [Security/CI track](../roadmap/iamine-security-ci-track.md).
Alternatives considered were implementing the shared evidence contract itself
(medium scope, but experimental and subject identity would overlap) and Rust
dependency remediation (broad graph and compatibility risk). The selected
subject has real value, observable failure modes, and a reversible CI diff.

## Baseline And Empirical Question

Planning baseline: commit `aa417c0a6d3a91593cf552ca0cfc0900ec3f75c8`, tree
`115efdeed492cee3a9eef95f4769dce71ca7cedd`. The new manifest's candidate_snapshot
records the clean published base observed before preparation edits, not a
future implementation, a tested subject candidate, or an approval.

Operational projection is relative to the checkout being inspected. A dirty
preparation checkout can report CHANGES REQUIRED and capture_clean_candidate;
a missing Architecture fact can request a review. Neither output activates the
pilot or reopens the closed HID feature. Historical closure must be evaluated
against its original source artifact and existing control facts.

Existing CI run `34186875528`, job `101936887087` (Secret scan), failed on that
commit. Read-only inspection found a required-license configuration error
mentioning GITLEAKS_LICENSE. This is not evidence that the scan found no
secrets. Refresh its detailed cause, coverage, and tool configuration before
implementation; do not store license values or raw scan logs here.

Baseline availability: PARTIAL. This CI failure is a real behavioral baseline.
There is no comparable measurement of full workflow cost with and without HID:
BASELINE_NOT_AVAILABLE for efficiency comparisons. Historical Phase 0 counts
are not Pilot 1 measurements. Do not invent improvement percentages.

Primary question: Can HID accompany a real secret-scan repair under trusted
operators and the canonical workflow, while preserving auditable evidence,
exact artifact identity, and separate authority, without exceeding the
proposed operational budget?

## Hypotheses And Experimental Budget

All thresholds below are PROPOSED EXPERIMENTAL BUDGET, not canonical HID
requirements, not new gates, and not approved by preparation authorization.
Freeze them by explicit decision before activation; later changes require
versioned human disposition and cannot retrospectively turn FAIL into PASS.

| Hypothesis | Falsifiable condition |
| --- | --- |
| Primary | Complete an auditable subject lifecycle with the quality/security contracts satisfied and the experimental budget respected. |
| H1 - quality/security | Clean scan, positive detection, and unavailable/failed scanner produce distinct correct results; no error or skip masquerades as PASS. |
| H2 - evidence/identity | Every required claim is recoverable and bound to the right artifact; stale or altered evidence does not satisfy gates. |
| H3 - authority/detection | No invalid transition is accepted; QA, Security, Architecture, and human decisions remain separately attributable. |
| H4 - operational cost | Manual HID overhead at most 120 minutes, at most 3 avoidable human interventions, 2 HID rework cycles, and 1 unjustified repeated validation. |

Estimate: 1-3 effective workdays for the subject, excluding external waits;
confirm feasibility after reviewing the actual scanner failure. This is an
estimate, not usage evidence. Count mandatory human decisions separately;
budget pressure never removes an approval or required validation.

## Scope, Quality, And Security

Proposed subject scope is the secrets job in
[quality-gate.yml](../../.github/workflows/quality-gate.yml), focused scanner
configuration/tests, and bounded redacted evidence. Before development,
Architecture must enumerate any additional file paths and approve the scanner
solution. Preserve scan coverage and blocking behavior, pin the selected
implementation and verify dependency integrity. No broad suppressions.

Excluded: other CI jobs, Rust dependency upgrades, Core, runtime, networking,
models, inference, dashboard, organization permission changes, automatic
merge/model selection, new authority domains, or Control Core changes.

QUALITY_PASS requires actual scanning, correct clean/positive/error cases,
unchanged intended coverage, a bounded maintainable diff, and all required
checks passing. A required behavioral or scope failure is QUALITY_FAIL.
Use synthetic non-sensitive positive cases only in isolated tests, not real
credentials or fabricated live HID approvals. Fixtures are not real incidents.

| Security category | PASS | FAIL / other |
| --- | --- | --- |
| Authority/permissions | Approved operations and least privilege | Expanded or unauthorised authority |
| Privacy/secrets | Redacted evidence and no unaccepted detected secrets | Exposure or an unaccepted finding |
| Input/error handling | Malformed input, unavailable tool, or failed download blocks success | Error or missing scan reported as clean |
| Supply chain/network | Approved downloads, version/integrity verification and bounded access | Unverified dependency or unexpected network access |
| Product service exposure | SECURITY_NOT_APPLICABLE for this CI-only scope | Reassess before any scope expansion |

Missing scanner execution is not SECURITY_PASS. QA and Security emit separate
reviews; scanner findings are not automatically defects detected by HID.
HID checks structural facts and attribution, not the truth of arbitrary logs.

Mac and Linux CI are the proposed validation environments. TS140/Proxmox Field
QA is not required by this CI-only scope; Architecture must reassess if any
product or platform-dependent behavior is introduced. No scan or CI job is
started by this contract. Required local/post-merge commands follow the
canonical workflow and Architecture's scoped test matrix; unavailable optional
tools stay SKIPPED, never PASS, and cannot waive the subject's required scan.

## Evidence Contract

| Claim | Required evidence | Producer | Freshness | Artifact binding | Invalidated by |
| --- | --- | --- | --- | --- | --- |
| Identity/scope | SHA, tree, base, diff, clean state | Git / Development | Each candidate and gate | Exact subject | Artifact or scope change |
| Quality | Commands, versions, coverage, results, skips | Development / QA | After last relevant edit | Candidate and environment | Changes or incomplete run |
| Security | Scan execution, configuration, redacted result, controlled cases | Scanner / Security reviewer | Final candidate | Actual scanned SHA/tree and rules | No scan, changed coverage, exposure |
| Reviews | Verdict, role, mandate, phase, evidence | Architecture / QA / Security reviewer | Current sequence | Exact subject | Superseding review, drift, invalid mandate |
| Human decision | Eligible capsule and later explicit decision | Human | After current prerequisites | Candidate, target, action | Denial, changed capsule or candidate |
| Integration/publication | Parents, deterministic tree, containment, fresh remote | Merge Owner | At operation | Source and integration distinguished | Conflict, wrong tree, remote movement |
| Post-merge/closure | Integration validation, findings, closure decision | QA / Architecture; human for experiment | After integration | Exact merge | Missing checks or unresolved blocker |

Use supported external evidence references for future operational facts.
Keep one recoverable, access-controlled, versioned package outside the subject
tree, containing bounded records and an index from reference IDs to content
digests. Approve the storage location, retention period, access, and retrieval
procedure before activation. Do not build the proposed evidence-store platform.
Do not embed raw logs, tokens, private paths, prompts, or model responses.
HID validates reference structure; a reviewer must verify retrieval and content.
Digest/index fields belong to the external package, not new event fields.

Do not commit evidence that changes the artifact it claims to certify, then
carry its authority forward. Validation records for this preparation are not
subject local_validation facts. Optional fixtures remain labelled test data.

## Human Gates And Metrics

| Decision class | Decisions |
| --- | --- |
| MANDATORY HUMAN GATE | Subject/scope selection and expansion, risk/security/test exceptions, merge, destructive operations, experiment outcome; publication when separately required |
| AGENT DECISION | Architecture development authorization, checkpoint/final review, QA, implementation, and feature closure within existing mandates |
| AUTOMATABLE CHECK | Git identity, schema/privacy checks, scoped tests, evidence shape and ordering |

No new Security gate or mandate is introduced. Security's separate evidence
informs existing reviews; it cannot be represented as human_merge or as QA's
verdict. The pilot-start human decision is not a fabricated merge capsule.

| Metric | Purpose / collection | Baseline | Decision use |
| --- | --- | --- | --- |
| HID defects detected/missed | Independently attribute findings; separate scanner defects | No comparable rate | Critical omissions prevent PASS |
| Invalid transitions rejected | Diagnostics and separately authorized isolated cases | Historical cases only | No invalid acceptance |
| Stale/incorrect evidence rejected | Result, reason, and subject identity | No comparable rate | Verify evidence validity |
| HID false positives | Independent disposition of unjustified rejections | Not available | Confidence and rework |
| Human interventions/gates/manual steps | Count by reason, mandatory versus avoidable | Canonical required steps | Budget for avoidable interventions only |
| Iterations/rework/repeated checks | Cycle IDs and identical-artifact run IDs | Not available | Proposed cycle/duplication budget |
| Workflow and overhead duration | Active HID effort separate from tests, waits, total elapsed | Not available | Proposed manual-time budget |
| Model usage | Actual manually selected model/effort, escalation reason, result, attributable rework, available tokens | Not available | Descriptive cost, no invented spend |
| Documentation volume | Files/bytes added for evidence and duplicate records | Not available | Identify overhead without arbitrary size gate |

All uncollected values are NOT_MEASURED; zero requires measurement. No observed
defect or invalid-transition opportunity means no empirical detection-rate
claim. Controlled cases supplement the real subject but are counted separately.
Observations must not duplicate already adequate validation just to fill metrics.

## Outcomes, Stop Rules, And Control Core Findings

PASS: complete subject lifecycle, H1-H4 satisfied, all required evidence present,
and no open blocking finding. Discovering and correctly handling a defect does
not itself prevent PASS. Missing effectiveness opportunities limit conclusions.
PARTIAL: useful evidence but noncritical coverage/measurement gaps or budget
overrun, with no critical authority/identity/security violation accepted.
FAIL: critical false confidence, unauthorized transition, identity substitution,
attributable exposure, or demonstrated inability to sustain the contract.
ABORT: unsafe scope, uncontrolled artifact, unresolved authority ambiguity, or
unresolvable dependency prevents completion without an already established FAIL.
Stopping execution does not erase a demonstrated failure.

Reuse failure_class values product, baseline, harness, infrastructure, test_gap,
unknown. Evidence may label security, authority, identity, workflow, operator,
false-positive, or false-negative subtypes without adding policy enums.

For a reproducible Control Core issue: PAUSE PILOT, classify P0-regression,
P1/P2/P3 or pilot-specific issue, propose a separate authorized HID change,
validate it, and resume only if authorized. Open blocking P0/P1/P2 findings
prevent progression; nonblocking P3 findings require disposition. No silent
self-modification and no hypothetical reopening of closed Phase 0.

Stop before expanding permissions, paths, suppression policy, or handling real
secret remediation. Preserve evidence and the isolated subject branch on abort.
Do not rewrite history. Any published rollback needs its own authorization.

## Entry, Exit, And Minimal Outputs

Entry checklist, all required before activation:

- Human-selected subject and locked exact paths; Architecture scope approval.
- Reviewed registration and scoped mandates; no inherited facts or decisions.
- Passing registration regressions with fixtures that supply all referenced
  documents and historical snapshot objects for every manifest they load.
- Fresh baseline/cause verification; scanner choice and isolated cases approved.
- Frozen hypotheses, experimental budget, metrics, and outcome rules.
- Approved human gates, evidence storage/access/retention and retrieval contract.
- Explicit baseline limitations and authorized rollback/abort strategy.
- Separate explicit pilot activation and subject development authorization.

HISTORICAL FINDING: during initial preparation, the multi-subject fixture
helpers did not supply every documentary reference required by the loaded
manifests, and the simulated Git model did not contain every legitimate
historical snapshot object. Independent review classified both gaps as
harness-only; they did not demonstrate a production Control Core defect.

CURRENT RESOLVED STATE: the later, separately human-authorized two-file test
scope corrected those gaps. The current harness copies and models real
references from every loaded manifest. Historical Git objects are admitted
only after the source repository verifies the exact commit/tree identity;
unknown or invalid objects remain rejected. Negative tests now assert the
intended rejection cause so preparation errors cannot mask fail-closed paths.

Exit requires a claim/evidence inventory, findings with independent disposition,
metrics and missing measurements, subject lifecycle status, and a separate
human pilot outcome decision. Roadmap/evidence updates require authorization.
PASS grants no permission to start another pilot or close another milestone.

Minimal outputs: this contract (SOURCE proposal); subject manifest and scoped
policy (SOURCE requirements); bounded external results/metrics package
(EVIDENCE); generated state/report views (DERIVED VIEW); independent assessment
and human pilot outcome (DECISION). Do not turn Markdown into a second mutable
gate database or add a new HID fact for the phase or pilot outcome.

## Model Plan, Limits, And Approval Capsule

Manual routing proposal: GPT-6 Astra / HIGH for Architecture, Security review,
and final assessment; GPT-5.6 Terra / HIGH for implementation and validation.
Escalation to Astra / XHIGH is reserved for material authoritative contradictions
or observed blockers requiring Control Core review. Record actual selections,
not the plan as execution telemetry. Optimize rework and human effort as well
as model usage; automatic routing remains disabled.

This pilot cannot establish cross-project generalization, untrusted-operator
safety, large-scale concurrency, autonomous governance, multi-agent operation,
production reliability, or Model Router effectiveness. Phase numbering remains
undefined beyond the approved POST_PHASE_0_VALIDATION relationship.

Proposed future capsule: WHAT evaluate shared evidence on a real repair;
WHY measure confidence and friction; SUBJECT the registered scan-repair ID;
SCOPE/EXCLUDES as above; HYPOTHESES/METRICS as proposed; RISKS CI dependency,
privacy and insufficient evidence; HUMAN GATES as above; PASS/PARTIAL/FAIL/ABORT
as defined; EXPECTED COST the unapproved estimate/budget; MODEL PLAN manual;
ACTION decide activation only after entry criteria are satisfied.
Current decision covers onboarding preparation only. This document is neither
an activation decision nor a persisted HID Human Merge Approval Capsule.
