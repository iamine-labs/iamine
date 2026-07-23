# AGENT-BOUNDARY-EVAL-VALIDATOR-001

## State

```text
READY FOR MERGE REVIEW
branch: feature/agent-boundary-eval-validator-001
base: 20c070c75dadc80febc2494c85eadd03138f0c59
base tree: 3c2d0e04423c63c796af6163eb6fc58a1742fdf6
runtime behavior change: none
field QA: not required; pure in-memory declaration validation only
quality gate: PASS WITH WARNINGS
recommendation: READY FOR ARCHITECTURE MERGE REVIEW
```

## Objective

Implement a typed, fail-closed YAML validator for the positive and negative
boundary eval declarations referenced by an IAMINE agent package. The feature
validates review metadata only. It does not read package files, execute an
eval, call a model, score a response, authorize package loading, advance local
registry review, enforce scope, grant permissions, or start an agent runtime.

## Owner Boundaries

The feature adds one focused owner under
`iamine-agents/src/boundary_eval/`:

```text
schema.rs      -> typed suite, cases, classes, actions, routes, and policies
validation.rs  -> coverage, coherence, privacy, reference, and review rules
error.rs       -> redacted error and semantic violation types
mod.rs         -> bounded structural parser integration and public exports
```

`iamine-agents/src/metadata_parser.rs` continues to own the shared structural
sequence. It performs bounded YAML parsing and generated JSON Schema
validation, but it does not know boundary-eval semantics.

`iamine-agents/src/package_load.rs` remains unchanged and always blocked. The
static `BoundaryEvalValidatorUnavailable` blocker is intentionally preserved:
this parser validates caller-supplied content, while the future package
reference resolver and evidence integration features must establish where the
content came from and how a result becomes trusted evidence.

## Parsing Contract

The public content boundary is:

```text
bounded caller-supplied YAML
-> structural parse
-> generated JSON Schema validation
-> typed deserialization
-> boundary-eval semantic validation
-> typed declaration or redacted failure
```

The maximum input size is `64 KiB`. Unknown fields fail at the generated
schema boundary. Public errors expose stable codes, static field names, and
static messages; they do not retain or echo supplied fixture text, private
paths, host identity, credentials, or secrets.

Public entry points:

```text
parse_boundary_eval_yaml
boundary_eval_json_schema
```

Schema identifier:

```text
iamine.agent.boundary_evals.draft-0.1
```

Historical TOML examples remain planning artifacts. As established by the
root manifest and the preceding Policy and Descriptive metadata validators,
this implementation owns a typed YAML content boundary only. It does not
select, open, or resolve `evals/agent-boundary-tests.yaml` or any other path.

## Required Coverage

Every suite must declare and provide at least one case for all nine classes:

```text
in_scope_positive
out_of_scope_negative
ambiguous_task
dangerous_task
cross_domain_task
permission_escalation
prompt_injection
role_confusion
handoff_to_orchestrator
```

Case identifiers must be bounded and unique. Suites are capped at 64 cases.
Synthetic inputs are capped at 512 bytes, single-line ASCII, and checked for
recognizable private path, IP address, email address, MAC address, private-key,
or assigned-secret shapes. These checks are a metadata safety boundary, not
proof that an input originated synthetically or that all possible private data
was detected. Independent human and QA review therefore remain mandatory.

## Expected Semantics

The complete bounded action vocabulary is:

```text
allow_review_response
refuse
clarify
handoff_to_orchestrator
refuse_or_handoff
```

The validator enforces class/action/route coherence:

- `in_scope_positive` may only expect a narrow review response;
- `ambiguous_task` must clarify or hand off;
- `cross_domain_task` and `handoff_to_orchestrator` must return control;
- negative, dangerous, permission, injection, and role-confusion classes must
  refuse, hand off, or use the explicit refuse-or-handoff label;
- handoff actions must route to the orchestrator;
- allow, clarify, and direct refusal labels remain agent responses.

These values are declaration labels. A successfully parsed suite has not run,
passed, failed, produced evidence, enforced a policy, or earned approval.

## Fail-Closed Policies

The schema requires explicit block policies for:

- blocked actions;
- scope expansion;
- permission grants;
- private-data collection;
- runtime-execution claims;
- registry-publication claims;
- missing suites or required classes;
- unsafe expected actions;
- unredacted evidence;
- contradictory scope results.

Redaction metadata must require synthetic inputs and block raw user prompts,
raw outputs, private paths, host identifiers, and credentials. Review metadata
must require both human review and QA evidence, reject self-approval, and use
bounded package-relative evidence references.

## Non-Bypass Rules

A successful parse cannot:

- authorize installation, package loading, or execution;
- remove a package-load blocker;
- prove an eval ran or passed;
- enforce scope or permissions;
- grant permissions or expand declared scope;
- resolve package references;
- advance local registry review;
- create runtime, scheduler, worker, model, or network behavior;
- imply trust, reputation, certification, rewards, marketplace publication, or
  public beta readiness.

## Integration Sequence

```text
AGENT-POLICY-METADATA-VALIDATORS-001
-> AGENT-DESCRIPTIVE-METADATA-VALIDATORS-001
-> AGENT-BOUNDARY-EVAL-VALIDATOR-001
-> AGENT-RUNTIME-CORE-001
-> AGENT-PACKAGE-REFERENCE-RESOLVER-001
-> AGENT-PACKAGE-REVIEW-EVIDENCE-001
```

The next feature may consume the public declaration types, but it must not
reinterpret a parser success as trusted package evidence.

## Risks

- Removing the static package-load blocker here would trust caller-supplied
  content without provenance.
- Treating a declaration as an executed eval would create false QA evidence.
- Accepting an allow action for a negative class would fail open.
- Echoing synthetic fixture text in errors could disclose private data.
- Resolving paths here would mix pure schema ownership with package I/O.
- Adding eval behavior to `iamine-node/src/main.rs` would violate the runtime
  ownership boundary.

## Field QA Decision

Field QA is not required because this feature has no filesystem, platform,
process, runtime, worker, scheduler, P2P, model, inference, hardware, installer,
or service behavior. The future resolver, runtime, sandbox, loader, and
executor features require the Mac, TS140, and Proxmox/R5500 matrix before merge
review.
