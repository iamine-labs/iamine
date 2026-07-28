# AGENT-ROUTING-CANDIDATE-SELECTOR-001 QA

## State

```text
IMPLEMENTATION COMPLETE
LOCAL VALIDATION PASSED
FIELD QA AUTHORIZED
READY FOR MERGE REVIEW
focused integration tests: PASS, 10/10
runtime regression: PASS, 93/93
strict crate clippy: PASS
Architecture checkpoint: PASS
field QA: PASS, 6/6 platform roles
```

## Identity

```text
branch: feature/agent-routing-candidate-selector-001
base: a4fd73311009de46ebb434caf501888e3b2794d3
base tree: 7d3f51ab76705df62a204612d7d8a76bf44cdb3f
source commit: fbf91c419ed5b13d3351bbcf47f8d28c319c88cc
source tree: 7561da0d1763a3e7afe37bd8473bac0359a485f6
```

## Expected Scope

```text
iamine-agent-runtime/src/routing_candidate_selector/
iamine-agent-runtime/src/lib.rs
iamine-agent-runtime/tests/routing_candidate_selector.rs
iamine-agent-runtime/tests/support/routing_candidate_chain.rs
iamine-agent-runtime/tests/support/routing_policy.rs
docs/architecture/agent-routing-candidate-selector.md
docs/qa/agent-routing-candidate-selector.md
docs/roadmap/iamine-agent-network-roadmap.md
docs/roadmap/iamine-product-roadmap.md
```

Forbidden:

```text
iamine-node/src/main.rs
iamine-node/src/cluster_registry.rs
Cargo.toml
Cargo.lock
Scope or Permission evaluation behavior
runtime compatibility or sandbox evaluation behavior
scheduler, peer discovery, transport, scoring, model selection, or MoE
package loading, execution authorization, persistence, or audit emission
```

## Check 1: Identity And Scope

Verify branch, full HEAD/tree/base, tracked/staged state, exact changed files,
origin, Git author identity, and baseline untracked artifacts.

## Check 2: Bounded Input

Validate:

- at most 64 candidates;
- candidate ids are non-empty, unique, bounded identifiers;
- task types are non-empty bounded identifiers;
- resource minimums are non-zero;
- duplicate ids fail before selection;
- no host, peer, user, path, credential, or hardware fingerprint enters the
  public contract.

## Check 3: Deterministic Outcomes

Confirm exact outcomes:

```text
candidate_selected
multiple_candidates
no_candidate
handoff_required
blocked
```

Validate:

- one eligible candidate is selected;
- multiple eligible candidates produce no selected id;
- reversing candidate order cannot select a winner;
- an empty set returns `no_candidate`;
- clarification/confirmation maps to `handoff_required`;
- refusal, prohibited risk, policy conflict, and unknown metadata block.

## Check 4: Independent Exclusions

Confirm exact reasons:

```text
scope_mismatch
permission_mismatch
resource_mismatch
risk_too_high
node_incompatible
sandbox_unavailable
policy_conflict
metadata_unknown
```

Each candidate contributes at most one exclusion using a fixed evaluation
order. Resource dimensions, execution mode, risk, availability, compatibility,
and sandbox state remain independent.

## Check 5: Evidence Integrity And Privacy

Validate:

- compatibility evidence verifies against the exact authority and subject;
- sandbox evidence verifies against the exact authority and subject;
- foreign evidence is rejected, not downgraded to an ordinary exclusion;
- debug output omits candidate id, task type, package id, subject, and
  authority identity;
- only a uniquely selected opaque candidate id is exposed by the evidence API.

Every result must report false for:

```text
execution_authorized
concrete_route_created
scheduler_mutated
model_selected
distributed_moe_used
transport_started
persisted
audit_event_emitted
```

`RuntimeOwner::RoutingCandidateSelector` and the package runtime remain
`Unavailable`.

## Check 6: Local Validation

```bash
cargo fmt --all -- --check
cargo test -p iamine-agent-runtime --test routing_candidate_selector
cargo test -p iamine-agent-runtime
cargo clippy -p iamine-agent-runtime --all-targets -- -D warnings
cargo test -p iamine-agents
./scripts/quality-gate.sh
git diff --check
git diff --cached --check
```

Current focused results:

```text
selector integration tests: PASS, 10/10
runtime total: PASS, 93/93
strict crate clippy: PASS
```

## Check 7: Field QA

Run on the exact source commit and tree:

| Platform role | Required | Result |
| --- | --- | --- |
| macOS development | yes | PASS, 10/10 + 4/4 |
| physical Linux | yes | PASS, 10/10 + 4/4 |
| Linux VM control | yes | PASS, 10/10 + 4/4 |
| Linux VM worker A | yes | PASS, 10/10 + 4/4 |
| Linux VM worker B | yes | PASS, 10/10 + 4/4 |
| Linux VM heavy | yes | PASS, 10/10 + 4/4 |

For each role:

```bash
cargo test -p iamine-agent-runtime --test routing_candidate_selector
cargo test -p iamine-agent-runtime --lib
```

Expected:

```text
integration: 10/10 PASS
library: 4/4 PASS
worktree: clean
runtime side effects: none
```

On the first failure, stop, classify product/environment/harness/baseline, do
not modify code during QA, and do not continue later roles.

## Current Result

```text
implementation: complete
focused validation: PASS
runtime regression: PASS
strict crate clippy: PASS
Architecture checkpoint: PASS
field QA: PASS, 6/6 platform roles, 60/60 focused + 24/24 library
field product failures: none
field environment failures: none
field harness failures: none
known limitation: Scope/Permission evaluations are typed but not authority-bound
execution/runtime availability change: none
recommendation: READY FOR ARCHITECTURE MERGE REVIEW
```
