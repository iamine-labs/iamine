# LAN File Share Assistant Agent Architecture

Feature:

```text
LAN-FILE-SHARE-ASSISTANT-AGENT-001
```

## Document Status

```text
document: functional Architecture contract
data class: SOURCE
architecture analysis: COMPLETE
A6 Option A human decision: RECORDED
functional architecture contract: RECORDED
architecture lifecycle approval: HID-EVENT-0085
architecture checkpoint: HID-EVENT-0088
final review: HID-EVENT-0090
development authorization: RECORDED (HID-EVENT-0085)
implementation: HID-EVENT-0086
local validation: HID-EVENT-0087
field QA: HID-EVENT-0089 (six roles, PASS)
human merge authorization: HID-EVENT-0092
merge: 2232f83bb1fc6c4a0bf53e77d6050b1bfc2570db
post-merge validation: HID-EVENT-0094
closure: HID-EVENT-0095
feature state: MERGED / VALIDATED / CLOSED
```

This document records the functional contract only: problem, scope, contracts,
ownership, restrictions, non-regression rules, and the implementation
allow-list. Recording a contract is not a lifecycle decision, and this document
does not itself approve Architecture or authorize Development. That later,
separate Architecture verdict was recorded as `HID-EVENT-0085`, and the
subsequent checkpoint, final review, human merge authorization, merge,
post-merge validation and closure facts are recorded in the Control Ledger.

The contract is owned by Architecture. Development may not widen it, and no
prompt, agent output, operator confirmation, roadmap text, or role instruction
may expand it.

## Roadmap And Milestone Position

Milestone `v0.12.0` - P0 Official Agents. The closed verticals
`NODE-DOCTOR-AGENT-001` and `REPORTER-AGENT-001` precede this row, and
`LAN-FILE-SHARE-ASSISTANT-AGENT-001` is `MERGED / VALIDATED / CLOSED`.

Skeleton contract `LAN-FILE-SHARE-ASSISTANT-AGENT-001-SKELETON` is `CLOSED`.
The functional row is `MERGED / VALIDATED / CLOSED`: candidate `457370e7`,
tree `4b68b5ab`, controlled `--no-ff` merge `2232f83b`, six-role Field QA
`HID-EVENT-0089`, post-merge validation `HID-EVENT-0094`, and closure
`HID-EVENT-0095`. The next sequential P0 functional candidate is
`PHOTO-LIBRARY-ORGANIZER-AGENT-001`, which is `NOT AUTHORIZED`.

Preserved, unmodified skeleton documents:

```text
docs/agents/lan-file-share-assistant-agent-skeleton.md
docs/architecture/lan-file-share-assistant-agent-skeleton.md
docs/qa/lan-file-share-assistant-agent-skeleton.md
```

## A6 Option A Decision

The operating-mode decision for this subject is fixed. It is not reopened here.

```text
A6 OPTION A

agent.earliest_mode:
local_readonly

runtime operating modes:
[local_readonly]

shared runtime change:
NOT AUTHORIZED

iamine-agent-runtime change:
NOT AUTHORIZED

iamine-agents change:
NOT AUTHORIZED

official_agent_execution.rs change:
NOT AUTHORIZED

local_planning runtime support:
OUT OF SCOPE

lan_readonly:
DEFERRED
```

`local_readonly` is a runtime envelope, not filesystem authority. It does not
grant, imply, or anticipate discovery, file access, share mounting, network
reach, credential handling, or privilege expansion.

## Skeleton Supersession

```text
The closed skeleton reserved local_planning.

A6 Architecture subsequently selected local_readonly
for the functional feature because the current executable
runtime supports LocalReadonly and the bounded functional
behavior requires no shared-runtime expansion.

The skeleton reservation is historical and superseded for
LAN-FILE-SHARE-ASSISTANT-AGENT-001.

This does not alter the closed skeleton evidence.
```

Handling rule:

```text
skeleton handling: PRESERVE_AND_SUPERSEDE
```

The three closed skeleton artifacts are preserved exactly as merged. They are
not edited, rewritten, or reinterpreted to replace their historical
`local_planning` reservation. The stale `ARCHITECTURE IN PROGRESS` header in
`docs/architecture/lan-file-share-assistant-agent-skeleton.md` remains a
historical documentation observation. It is not corrected silently and is not
repaired by this contract.

The supersession is scoped to this functional feature. It changes no closed
skeleton evidence, no milestone closure, and no other P0 row.

## User Problem

A household or small-office operator already selected a file share and already
holds a redacted summary of it, but cannot reliably tell whether the declared
read-only usage is safe, complete, or contradictory. The operator needs a
bounded explanation of metadata they already selected, plus an explicit handoff
when the request crosses into discovery, connection, authentication, mounting,
file access, or transfer.

## Supported Behavior

The future agent is a local, pre-network, pre-filesystem reasoning surface. It
may only:

```text
summarize_operator_approved_share_inventory
explain_declared_readonly_boundary
highlight_missing_or_unsafe_share_metadata
suggest_non_destructive_next_steps
request_clarification
handoff_for_file_or_network_action
```

Retained reserved identity, originally recorded by the closed skeleton:

```text
package_id: iamine.beta.lan-file-share-assistant
task_type: file_share_readonly_review
scope_id: lan_file_share_readonly_review
mode: local_readonly
deferred_mode: lan_readonly
execution_authorized: false
```

The mode value is the A6 Option A supersession. The remaining identifiers are
planning and package metadata, not a permission grant, not an executable
manifest field by themselves, and not a runtime capability.

## Explicit Non-Scope

```text
filesystem discovery, enumeration, or listing
file read, file write, or file modification
share discovery, probing, or enumeration
share mounting or unmounting
network connections, transfers, or protocol clients
authentication, credentials, or secret handling
network mutation or configuration change
shell execution or child processes
privilege expansion or sandbox relaxation
persistence, export, upload, or publication
model-backed free-form prose generation
telemetry, analytics, or third-party contact
LAN or remote execution
installer, registry, marketplace, or catalog publication
v0.12.0 milestone closure
authorization of any later P0, P1, or P2 product feature
```

`lan_readonly` stays deferred. It requires its own dedicated implementation,
Architecture decision, and QA evidence before any executable LAN surface may
exist. `local_planning` runtime support stays out of scope for this thread.

## Typed Input Contract

Proposed identifiers, recorded for approval and not yet approved:

```text
schema: iamine.agent.lan-file-share-assistant.input-0.1
```

Input is bounded, typed, operator-supplied, and already redacted. It is
expressed as a bounded number of repeated typed tokens parsed into enums. The
functional shape is:

```text
iamine-node agents lan-file-share --package-root PATH \
  [--share SHARE_ID:STATE:FLAG]... [--json]
```

Required properties:

- a hard upper bound on the number of tokens;
- closed enums for every token field, so unknown values fail closed;
- deduplication of repeated tokens;
- rejection of free-form text, raw paths, hostnames, addresses, identifiers,
  credentials, logs, prompts, and unredacted evidence inside every supplied
  metadata token, while `--package-root PATH` and `--share SHARE:STATUS:CLAIM`
  remain the bounded, strictly validated CLI surface described above;
- explicit, distinguishable states for supplied, missing, and unsupported
  metadata;
- deterministic structured parsing followed by independent re-parsing inside
  the official Rust program.

Empty input is valid only to produce a bounded missing-evidence report. It must
never be interpreted as proof that a share is safe.

## Output Contract

Proposed identifier, recorded for approval and not yet approved:

```text
schema: iamine.agent.lan-file-share-assistant.output-0.1
```

The output contains only the schema identifier, a stable classification, the
typed metadata codes echoed back as enums, and a bounded next-step code. It
never echoes raw CLI input, never emits free-form prose, and never states a
fact about the filesystem or the network that the input did not carry.

Classification policy:

- complete supported metadata produces a bounded review report;
- absent or explicitly missing metadata produces a blocked-action report;
- unsupported or out-of-scope claims produce a handoff request;
- invalid, broad, duplicate, oversized, or contradictory input is rejected.

Runtime output must explicitly report that it did not mutate the scheduler,
start transport, persist data, or claim OS-level isolation.

## Package Manifest Contract

The future package must live under:

```text
agents/official/lan-file-share-assistant/
```

Required layout, following the closed skeleton standard:

```text
agent.yaml
agent-scope.yaml
README.md
metadata/agent-capabilities.yaml
metadata/agent-expertise.yaml
metadata/agent-resources.yaml
metadata/agent-permissions.yaml
metadata/agent-audit.yaml
evals/agent-boundary-tests.yaml
src/README.md
review/human-review.md
review/qa-evidence.md
```

Required manifest properties:

- `execution_authorized: false` in package metadata;
- `earliest_mode: local_readonly`;
- the reserved `package_id`, task class, and scope identifier above;
- all seven policy-bearing references declared and package-relative;
- no absolute local path, share path, host identifier, credential, or private
  machine data anywhere in the package;
- no third-party publication, marketplace, or public-beta channel.

The reviewed manifest and every referenced document must match a compiled
canonical snapshot exactly. Package metadata never authorizes execution; only
the existing operator-local runtime owner chain may establish review,
compatibility, input/output, sandbox, lifecycle, timeout, scope, permission,
routing, audit, load, execution, and result-verification evidence.

## Permission Model

Permissions are default-deny and are evaluated by the existing permission
authority. The future agent requests only what the bounded local-readonly
review needs.

Rules:

- unknown, absent, or contradictory permission metadata fails closed and never
  silently authorizes new behavior;
- no permission may be widened by a user prompt, agent output, operator
  confirmation, or role instruction;
- the feature may not introduce a new permission category. If the bounded
  behavior cannot be expressed with existing permission and resource
  categories, that is an Architecture change, not an implementation detail.

## Scope Enforcement

Scope is enforced by the existing scope authority against the declared scope
manifest. In-scope requests produce a bounded review. Out-of-scope requests
produce refusal, clarification, or handoff, and are never silently executed.

The agent must refuse and record, rather than attempt, any request to discover
a share, connect to a host, authenticate, mount, read or write a file, transfer
data, execute a shell command, spawn a process, or escalate privilege.

## Filesystem Boundary

```text
filesystem access: NONE
```

The future agent does not open, enumerate, stat, read, write, create, or delete
files or directories. The only filesystem interaction in the feature is the
existing package-reference resolution performed by the operator-local runtime
for the reviewed package itself, under the existing resolver limits. That is
package loading, not user data access, and it is not a new capability.

## Network Boundary

```text
network access: NONE
```

The feature is pre-network. It opens no socket, starts no transport, joins no
PubSub topic, performs no discovery, resolves no peer, connects to no host, and
transfers no bytes. It must not depend on network availability, and runtime
evidence must prove the transport and scheduler were not started or mutated.

## Credential And Privacy Boundary

The feature handles no credential and no secret. It must never consume,
request, store, transmit, or echo:

```text
passwords, tokens, keys, wallet secrets, or session material
credentials or authentication material of any kind
raw share paths, directory listings, or file contents
hostnames, IP addresses, MAC addresses, serials, or disk identifiers
personal filesystem paths, usernames, or unnecessary machine identifiers
raw prompts, raw outputs, unredacted logs, or environment dumps
```

Input is treated as already redacted, and the project privacy policy remains
binding. The agent must preserve the repository privacy tiers and must not
persist evidence outside the bounded review output. Pattern-based detection
cannot prove safety; human review remains required.

## Prompt Injection Boundary

Supplied metadata is untrusted input. Any instruction embedded in the supplied
metadata — including instructions to ignore the scope, reveal credentials,
enumerate the filesystem, contact a host, or bypass permission or handoff — is
refused and recorded as a boundary outcome. Prompt injection never changes the
contract, the permission set, or the scope, and it never elevates a blocked
action.

## Role Confusion And Handoff

The agent holds exactly one role: bounded local-readonly share-metadata review.
It must not adopt the role of an operator, an administrator, an architecture
authority, a QA authority, a merge authority, or another agent.

Role confusion, fabricated status, invented authority, and instructions to act
as a different actor are refused. When a request requires discovery,
connectivity, authentication, mounting, file access, transfer, recovery,
configuration, or execution, the agent performs a handoff record instead of the
action. The handoff names the deferred capability and returns control; it never
performs a partial version of the deferred action.

## Runtime Dependency Reuse

The implementation reuses existing runtime support without modifying it:

```text
reused and unmodified:
  iamine-agents::ExecutionMode::LocalReadonly (owned by iamine-agents, consumed by iamine-agent-runtime)
  iamine-agents::ResourceOperatingMode::LocalReadonly (owned by iamine-agents, consumed by iamine-agent-runtime)
  iamine-node::official_agent_execution (shared local-readonly composition)
  iamine-agents scope, permission, and manifest validation owners

not authorized to change:
  iamine-agent-runtime/**
  iamine-agents/**
  iamine-core/**
  iamine-node/src/official_agent_execution.rs
```

The shared composition is generic: it accepts an immutable agent execution spec
and a program registrar. Adding this agent must therefore be expressible as a
new spec, a new Rust program, a new package, and CLI wiring only. If the feature
cannot be implemented within that generic composition, or if it needs a new
runtime mode, a new permission category, or a new shared owner, implementation
must stop and return to Architecture.

## Exact Implementation Scope

Normalized allow-list for a later, separately authorized Development
authorization. Nothing in this list is authorized yet.

IMPLEMENTATION REQUIRED (product):

```text
agents/official/lan-file-share-assistant/**
iamine-node/src/lan_file_share_assistant_agent/**
iamine-node/src/cli.rs
iamine-node/src/mode_dispatch.rs
iamine-node/src/node_modes.rs
iamine-node/src/usage.rs
iamine-node/src/main.rs (module declaration and wiring only)
```

GOVERNANCE / DOCS:

```text
docs/architecture/lan-file-share-assistant-agent.md
docs/qa/lan-file-share-assistant-agent.md
docs/roadmap/iamine-agent-network-roadmap.md
docs/roadmap/iamine-product-roadmap.md
.hid/features/LAN-FILE-SHARE-ASSISTANT-AGENT-001.yaml
```

CONDITIONAL (requires separate governance authority in its own iteration):

```text
.hid/project.yaml mandate feature lists only — the subject-registration extension
recorded by the governance onboarding iteration; never part of Development and
never a product-code change
.hid/tests/** (only if a governance iteration explicitly admits them)
```

FORBIDDEN in this feature:

```text
iamine-agent-runtime/**
iamine-agents/**
iamine-core/**
iamine-node/src/official_agent_execution.rs
iamine-models/**
iamine-network/**
iamine-hardware/**
dashboard/**
client-rust/**
contracts-solana/**
packaging/**
scripts/quality-gate.sh
iamine-node/src/cluster_registry.rs
scheduler, P2P, PubSub, worker lifecycle, model selection, inference,
model storage, reputation, reward, and settlement code
```

`.hid/project.yaml` mandate feature-list extensions are governed by the
CONDITIONAL entry above; they are never product-code or Development edits.

`iamine-node/src/main.rs` must remain wiring only. `cluster_registry.rs` must
not grow. No unrelated refactor, formatting sweep, dependency change, or
cleanup may ride along.

## Test Architecture

Test classes the implementation must cover to claim local validation (all
delivered and exercised in the recorded candidate):

```text
package manifest and all seven referenced metadata documents
canonical snapshot equality for every referenced document
typed input parsing, bounds, and enum rejection
duplicate, oversized, contradictory, and ninth-token rejection
positive bounded review report
missing-evidence blocked report
unsupported-claim handoff report
privacy redaction and no-echo behavior
altered package and altered reference fail-closed behavior
scope enforcement and refusal
permission enforcement and default-deny behavior
out-of-scope refusal, clarification, and handoff
prompt injection and role confusion boundary outcomes
credential request, share discovery, mount, file-content, and network refusal
runtime authorization, audit, cleanup, and no-side-effect fields
Node Doctor and Reporter non-regression through the shared composition
quality gate, formatting, Clippy, and architecture size guards
```

Tests are deterministic and must not require real model loads, real network
access, real shares, or real credentials.

## Field QA

```text
field_qa_required: YES
```

Field QA is required because this feature adds executable agent and CLI
behavior. The exact authorized commit must be validated on all six canonical
roles:

```text
Mac local
TS140
iamine-ctrl
iamine-wrk1
iamine-wrk2
iamine-heavy
```

Every role must prove pre-network local-only behavior, structured output,
package integrity, privacy, cleanup, and zero transport, scheduler,
persistence, model, worker, or inference side effects. The QA plan and evidence
template live in `docs/qa/lan-file-share-assistant-agent.md`.

## Acceptance Criteria

The feature is acceptable only when all of the following hold:

```text
the diff stays inside the authorized allow-list
no shared runtime, core, or agents path changes
the package and all references match their canonical snapshot
execution_authorized stays false in package metadata
input and output stay typed, bounded, and enum-closed
no filesystem, file, network, credential, shell, process, or privilege behavior exists
prompt injection, role confusion, and escalation attempts are refused
evidence, logs, and output remain redacted and local
Node Doctor and Reporter behavior is unchanged
local validation, all six field-QA roles, and Architecture review pass
no milestone, registry, marketplace, or publication state changes
```

Failure of any item is a blocking finding, not an accepted variance.

## Next-Iteration (B2) Stop Conditions

The future B2 preparation and Development-authorization iteration must stop,
before any branch or commit, if any of the following holds:

```text
live origin/develop differs from the authorized base SHA
the authorized base tree differs from the recorded tree
the control ledger head differs from the recorded ledger commit
the repository is not clean
Architecture approval is not recorded as a governance fact
Development authorization is not recorded
the scope allow-list is missing, unbounded, or not path-exact
the HID feature manifest identity is absent or unresolved
the implementation would require a shared-runtime, core, or agents change
the implementation would require a new permission or resource category
the implementation would absorb unrelated local changes
the field-QA matrix is undefined or field QA is waived without authority
```

No fetch, branch, worktree, commit, push, or merge is performed by this
contract-recording iteration.

## Rollback

The feature is additive and local. Rollback is the removal of the future
package, the future agent module, the CLI wiring, the documentation, and the
HID feature manifest, returning the node to the closed behavior of the
preceding release. No data migration, no persistent state, no network
configuration, and no external system is involved, so rollback requires no
recovery procedure beyond reverting the feature and rebuilding.

Because the feature never mutates shared state, rollback cannot strand
credentials, model storage, scheduler state, or peer state.

## Authority Boundary

This document is a recorder artifact produced by an Architecture contract
recording role. It establishes no lifecycle fact by itself.

```text
architecture analysis: COMPLETE
A6 Option A human decision: RECORDED
architecture contract: RECORDED
architecture lifecycle approval: HID-EVENT-0085
architecture checkpoint: HID-EVENT-0088
final review: HID-EVENT-0090
development authorization: RECORDED (HID-EVENT-0085)
implementation: HID-EVENT-0086
local validation: HID-EVENT-0087
field QA: HID-EVENT-0089 (six roles, PASS)
human merge authorization: HID-EVENT-0092
merge: 2232f83bb1fc6c4a0bf53e77d6050b1bfc2570db
post-merge validation: HID-EVENT-0094
closure: HID-EVENT-0095
feature state: MERGED / VALIDATED / CLOSED
```

The explicit Architecture decision this document previously awaited is recorded
as `HID-EVENT-0085`; the checkpoint, final review, human merge authorization,
merge, post-merge validation and closure facts are recorded in the Control
Ledger.

The HEC Adapter Contract v1.0 remains frozen and productive IAMINE HEC
execution is not authorized. This feature carries no HEC requirement, creates
no HEC schema, does not modify Hermes, and does not block MAIN work on HEC.

Next candidate recorded by the roadmap after this row (not authorized by this
reconciliation):

```text
PHOTO-LIBRARY-ORGANIZER-AGENT-001 (PROPOSED / NOT AUTHORIZED)
```
