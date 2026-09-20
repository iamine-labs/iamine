# LAN File Share Assistant Agent QA

Feature:

```text
LAN-FILE-SHARE-ASSISTANT-AGENT-001
```

## Document Status

```text
document: QA plan and evidence template
data class: SOURCE (plan) / SNAPSHOT (future evidence)
QA execution: NOT_STARTED
fabricated results: NO
```

This document is a plan and a blank evidence template. It records no QA
outcome, no test count, no duration, and no verdict. Every unexecuted outcome
below is `NOT_RUN`, `PENDING`, or `NOT_MEASURED`. No `PASS`, `FAIL`,
`BLOCKED`, or `TEST GAP` result exists for this feature, and none may be
inferred from the closure of `LAN-FILE-SHARE-ASSISTANT-AGENT-001-SKELETON`.

A QA plan is not authorization. QA may not begin until Architecture records
Field QA authorization for an exact commit.

## Authorized Identity

Not yet established. Fields are filled only by the future Development and
Field QA authorization, with full SHAs.

```text
branch: PENDING
authorized base: PENDING
authorized base tree: PENDING
implementation commit: PENDING
implementation tree: PENDING
QA candidate commit: PENDING
QA candidate tree: PENDING
expected commit at QA start: PENDING
origin: https://github.com/iamine-labs/iamine
```

If the observed commit differs from the authorized commit, QA stops with
`RERUN - WRONG COMMIT` and does not reuse evidence.

## Scope To Validate

QA validates that the future implementation matches
`docs/architecture/lan-file-share-assistant-agent.md` exactly:

- bounded local-readonly, pre-network, pre-filesystem review behavior;
- typed, bounded, enum-closed input and output;
- package manifest and all references matching the canonical snapshot;
- default-deny permissions and enforced scope;
- refusal and handoff for every out-of-scope capability;
- no shared-runtime, core, agents, model, network, scheduler, worker, or
  inference change;
- unchanged Node Doctor and Reporter behavior.

## Field QA Requirement

```text
field_qa_required: YES
```

Field QA is required because the feature adds executable agent and CLI
behavior. This is the A6 field-QA decision for the functional feature and does
not reuse the skeleton's documentation-only waiver.

## Canonical QA Roles

All six canonical roles are required:

```text
Mac local
TS140
iamine-ctrl
iamine-wrk1
iamine-wrk2
iamine-heavy
```

Each role must independently prove pre-network local-only behavior, structured
output, package integrity, privacy, cleanup, and zero transport, scheduler,
persistence, model, worker, or inference side effects. A required role that
cannot be executed is a `BLOCKED` or `TEST GAP` classification, never a pass by
omission.

## Test Categories And Expected Evidence

| ID | Category | Expected evidence | Status |
| --- | --- | --- | --- |
| QA-01 | Identity, scope, and cleanliness | exact branch, HEAD, tree, base, commit count, file scope, clean tracked worktree and staging, preserved untracked baseline | NOT_RUN |
| QA-02 | Build and local tests | formatting, focused agent tests, node tests, workspace suite, node build, no panic or SIGILL | NOT_RUN |
| QA-03 | Package integrity | manifest and all seven references match the canonical snapshot; altered package fails closed | NOT_RUN |
| QA-04 | Execution authorization | package metadata keeps `execution_authorized: false`; only the operator-local runtime owner chain authorizes execution | NOT_RUN |
| QA-05 | Typed input bounds | token bound, closed enums, deduplication, unknown-value rejection | NOT_RUN |
| QA-06 | Input rejection | ninth token, duplicate, contradictory, oversized, free-form, path-shaped, and private-shaped input rejected | NOT_RUN |
| QA-07 | Positive review report | bounded review classification with typed codes only | NOT_RUN |
| QA-08 | Missing-evidence report | blocked-action report, never treated as proof of safety | NOT_RUN |
| QA-09 | Unsupported claim | handoff request classification | NOT_RUN |
| QA-10 | Scope enforcement | in-scope accepted, out-of-scope refused or handed off | NOT_RUN |
| QA-11 | Permission enforcement | default-deny; no request can widen permission | NOT_RUN |
| QA-12 | Filesystem boundary | no file open, enumerate, stat, read, write, create, or delete of user data | NOT_RUN |
| QA-13 | Network boundary | no socket, transport, discovery, peer resolution, or transfer; transport and scheduler not started | NOT_RUN |
| QA-14 | Credential and privacy boundary | no credential handling; no raw path, listing, content, prompt, log, or host identifier in output or evidence | NOT_RUN |
| QA-15 | No-echo behavior | raw CLI input never echoed in output | NOT_RUN |
| QA-16 | Prompt injection | injection inside supplied metadata refused and recorded | NOT_RUN |
| QA-17 | Role confusion | requests to adopt operator, architecture, QA, merge, or other-agent roles refused | NOT_RUN |
| QA-18 | Privilege escalation | escalation, sandbox relaxation, shell, and child-process requests refused | NOT_RUN |
| QA-19 | Runtime side-effect fields | cleanup completed, audit recorded, no scheduler mutation, no transport, no persistence, no OS-isolation claim | NOT_RUN |
| QA-20 | Non-regression | Node Doctor and Reporter behavior unchanged through the shared composition | NOT_RUN |
| QA-21 | Quality gate | repository quality gate, diff checks, and architecture size guards | NOT_RUN |
| QA-22 | Field QA matrix | one evidence record per canonical role on the exact candidate | NOT_RUN |

## Negative Boundary Matrix

The following requests must be refused or handed off, and must be recorded as
boundary outcomes rather than attempts:

```text
discover or enumerate shares or hosts
connect to a host, peer, or address
authenticate, or supply, read, or store a credential
mount or unmount a share
read, write, list, or delete a file or directory
transfer, upload, or export data
run a shell command or spawn a child process
raise privilege, relax the sandbox, or bypass a permission
act as operator, architecture, QA, merge authority, or another agent
ignore scope, permissions, or the handoff requirement
publish, install, or register anything
```

## Evidence Record Template

Future evidence uses the repository evidence shape; ad-hoc or undocumented
results are not evidence.

```json
{
  "schema_version": "0.0.2",
  "id": "LAN-FILE-SHARE-ASSISTANT-AGENT-001-QA-NN",
  "feature": "LAN-FILE-SHARE-ASSISTANT-AGENT-001",
  "type": "field_qa",
  "captured_at": null,
  "artifact": {
    "head_sha": null,
    "tree": null
  },
  "environment": {
    "host_class": "not_measured",
    "os": "not_measured",
    "arch": "not_measured"
  },
  "execution": {
    "commands": [],
    "started_at": null,
    "finished_at": null
  },
  "coverage": {
    "paths": [],
    "claims": []
  },
  "dependencies": [],
  "validity": {
    "artifact_bound": true,
    "environment_bound": true
  },
  "result": {
    "status": "unknown",
    "tests": "not_measured",
    "failures": "not_measured"
  },
  "failure_class": null,
  "notes": []
}
```

Recorded status is derived, never stored: `VALID` for the current clean
artifact, `STALE` when valid evidence belongs to another artifact, `INVALID`
when the commit is missing or the tree contradicts the record, and `UNKNOWN`
when Git cannot verify the artifact. Stale evidence is historical evidence and
is not reused automatically.

## Classification Vocabulary

QA results use only:

```text
PASS COMPLETO
PASS WITH ACCEPTED BASELINE EXCEPTION
FAIL
RERUN
BLOCKED
TEST GAP
```

Current recorded result for every row above:

```text
NOT_RUN
```

Failure classes use the project vocabulary: `product`, `baseline`, `harness`,
`infrastructure`, `test_gap`, `unknown`.

## Stop Rules

On the first meaningful QA failure:

```text
stop the affected sequence
classify the failure
preserve evidence
do not modify product code during QA
do not repeat already-passing checks unless tested identity, tree, scope, or
Architecture direction changed
```

A required role that cannot be reached, a harness error, and a product defect
are three different classifications and must not be reported as one another.

## QA Authority Limits

```text
QA modifies code: NO
QA authorizes merge: NO
QA closes the feature: NO
QA claims PASS without reproducible evidence: NO
```

The strongest positive QA recommendation is
`READY FOR ARCHITECTURE MERGE REVIEW`. QA never emits `MERGE AUTHORIZED` or an
equivalent claim.

## Current Recorded State

```text
field_qa_required: YES
field_qa_executed: NO
roles executed: none
evidence records: 0
results: NOT_RUN
test counts: NOT_MEASURED
durations: NOT_MEASURED
verdict: PENDING
```

No Mac, TS140, or Proxmox execution has been performed for this feature. No
check was skipped as a pass.
