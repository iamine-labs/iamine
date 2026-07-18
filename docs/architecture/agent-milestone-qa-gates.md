# AGENT-MILESTONE-QA-GATES-001

## State

```text
ACTIVE
```

## Objective

Make exhaustive milestone QA a registered, auditable closure dependency for
the IAMINE Agent Network roadmap. A milestone may not transition to `CLOSED`
from feature-local validation, documentation-only contracts, or an old QA
snapshot whose scope no longer matches the roadmap.

This feature formalizes process and evidence. It does not execute a milestone
gate or close a product milestone.

## Ownership

```text
workflow policy: docs/process/iamine-canonical-workflow.md
gate registry: docs/roadmap/iamine-agent-network-roadmap.md
product state: docs/roadmap/iamine-product-roadmap.md
QA contract: docs/qa/agent-milestone-qa-gates.md
milestone evidence: docs/qa/<milestone>-milestone.md
```

The gate is a process and Architecture release boundary. It does not count as
a product feature and cannot reorder the official milestone sequence.

## Closure Invariant

```text
all milestone features merged and validated
AND all promised runtime behavior has executable evidence
AND exhaustive milestone QA is current for exact HEAD/tree
AND required field environments pass
AND milestone gate evidence is merged and post-merge validated
-> Architecture may close the milestone
```

If any term is false or unknown, the milestone remains open.

## Registration And Execution Timing

Register a stable gate ID when a milestone becomes `ACTIVE`, or before its last
feature starts at the latest. The gate remains `PROPOSED` or blocked while
milestone-owned work is incomplete.

Execute the gate only after the final in-scope feature is merged, validated,
and closed. This prevents an early snapshot from being reused after the
milestone grows.

## Evidence Boundary

These statements are not equivalent:

```text
feature implemented != feature validated
feature validation != milestone regression
local QA != field QA
documentation contract != executable behavior
historical QA snapshot != current milestone gate
gate registered != gate authorized
gate passed != milestone closed
```

Every gate binds evidence to a full commit SHA and tree. Architecture must
invalidate or rerun the gate when HEAD, tree, milestone scope, required field
matrix, or an accepted exception changes.

## Required Review Surfaces

For agent milestones, the gate must aggregate:

- package and manifest validation;
- scope, permissions, blocked actions, and handoff;
- positive and negative capability behavior;
- boundary, unsafe-action, prompt-injection, and role-confusion tests;
- privacy redaction and bounded audit evidence;
- timeout, cancellation, cleanup, and resource limits;
- local, LAN, and remote-mode restrictions;
- agent-to-agent and agent-to-runtime regression where applicable;
- all existing IAMINE behavior the milestone depends on.

## Field QA Rule

Documentation-only milestones may omit TS140 and Proxmox only with an explicit
Architecture decision that identifies the absence of runtime behavior.

Mac, TS140, and Proxmox/R5500 evidence is mandatory when a milestone changes or
claims executable behavior in runtime, workers, scheduler, networking,
inference, hardware, packaging, installation, services, or operations.

Field QA cannot be replaced by mocks for a claim about a real environment.
Mocks remain required for bounded negative and no-side-effect paths.

## Exception Rule

An accepted baseline exception must:

- reproduce on the exact prior baseline;
- be outside milestone ownership;
- preserve the milestone's primary contract;
- identify impact and residual risk;
- remain visible in the closure report.

Architecture cannot accept an exception for missing core milestone behavior,
missing required field QA, a privacy violation, an unsafe permission path, or
scope enforcement bypass.

## Current Reconciliation

The near-term Agent Network milestones require these explicit gates:

```text
v0.11.1 -> V0.11.1-AGENT-ARCHITECTURE-FOUNDATION-MILESTONE-QA-001
v0.11.2 -> V0.11.2-AGENT-RUNTIME-BASELINE-MILESTONE-QA-001
v0.11.3 -> V0.11.3-AGENT-CREATION-ASSISTANTS-MILESTONE-QA-001
v0.12.0 -> V0.12.0-P0-OFFICIAL-AGENTS-MILESTONE-QA-001
```

The prior v0.11.2 milestone report validated an earlier documentation-only
scope. The current roadmap later added package loading and executable
enforcement work, so that report remains historical evidence but cannot close
the current v0.11.2 milestone.

The v0.11.3 gate remains valid for its closed documentation-only assistant
contracts. It does not prove functional agents or runtime execution.

## Out Of Scope

This feature does not:

- run exhaustive QA;
- close v0.11.1, v0.11.2, or v0.12.0;
- reopen historically closed milestones;
- implement agents, validators, enforcement, sandboxing, audit, or runtime;
- change scheduler, P2P, PubSub, inference, hardware, packaging, or CLI;
- require TS140 or Proxmox field execution.

## Next Gate

After this policy merges and closes, canonical order requires:

```text
V0.11.1-AGENT-ARCHITECTURE-FOUNDATION-MILESTONE-QA-001
```

Only after Architecture closes v0.11.1 may delivery continue into the next
unresolved v0.11.2 runtime feature.
