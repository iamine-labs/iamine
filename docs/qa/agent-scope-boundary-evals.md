# IAMINE Agent Scope Boundary Evals QA

Feature:

```text
AGENT-SCOPE-BOUNDARY-EVALS-001
```

## Objective

Validate that the agent scope boundary eval contract is roadmap-aligned,
documentation-only, scope-bound, privacy-safe, blocked by default, and does not
authorize package installation, runtime eval execution, runtime scope
enforcement, sandboxing, permission enforcement, scheduler placement, worker
startup, model loading, public registry publication, marketplace behavior, or
distributed model MoE behavior.

## Identity

Record before QA:

```text
Branch: feature/agent-scope-boundary-evals-001
HEAD before implementation: c582431085a5dcb30fae1295e60911e55b04d980
Tree before implementation: 0b670a91139dc21e4cfb0d1f730150a836cafeb3
Base: origin/develop
origin/develop: c582431085a5dcb30fae1295e60911e55b04d980
tracked clean: yes before implementation; no after expected documentation delta
staging clean: yes before final staging
untracked baseline: expected new feature docs only
untracked files:
- docs/agents/agent-scope-boundary-evals.md
- docs/architecture/agent-scope-boundary-evals.md
- docs/qa/agent-scope-boundary-evals.md
```

## Scope Checks

Expected changed paths:

```text
docs/agents/agent-scope-boundary-evals.md
docs/architecture/agent-scope-boundary-evals.md
docs/qa/agent-scope-boundary-evals.md
docs/agents/agent-package-manifest.md
docs/architecture/agent-package-manifest.md
docs/roadmap/iamine-agent-network-roadmap.md
```

Expected runtime behavior change:

```text
none
```

This feature must not modify Rust source, tests, scripts, runtime, executable
agent packages, package installation, skeleton generator, package parser, scope
parser, capability parser, expertise parser, resource parser, permission
parser, audit parser, boundary eval parser, eval runner, eval scoring, eval
report generation, registry storage implementation, registry synchronization,
registry publication, permission enforcement, sandboxing, runtime audit
logging, router, scheduler, worker behavior, hardware profiler, model policy,
inference execution, installer, updater, rollback, reputation, rewards, wallet,
marketplace, public beta, or mainnet behavior.

## Required Local Validation

```bash
git diff --check
git diff --cached --check
cargo fmt --all -- --check
rg -n "AGENT-SCOPE-BOUNDARY-EVALS-001" docs/agents/agent-scope-boundary-evals.md docs/architecture/agent-scope-boundary-evals.md docs/qa/agent-scope-boundary-evals.md docs/roadmap/iamine-agent-network-roadmap.md
rg -n "documentation-only|does not authorize|does not modify|runtime behavior change" docs/agents/agent-scope-boundary-evals.md docs/architecture/agent-scope-boundary-evals.md docs/qa/agent-scope-boundary-evals.md
rg -n "iamine.agent.boundary_evals.draft-0.1|evals/agent-boundary-tests.toml|required_classes|cases|expected_actions|forbidden_successes|redaction_policy|failure_policy" docs/agents/agent-scope-boundary-evals.md docs/architecture/agent-scope-boundary-evals.md docs/agents/agent-package-manifest.md docs/architecture/agent-package-manifest.md
rg -n "in_scope_positive|out_of_scope_negative|ambiguous_task|dangerous_task|cross_domain_task|permission_escalation|prompt_injection|role_confusion|handoff_to_orchestrator" docs/agents/agent-scope-boundary-evals.md docs/architecture/agent-scope-boundary-evals.md
rg -n "cannot authorize package installation|cannot authorize runtime execution|cannot enforce scope at runtime|cannot grant permissions|cannot replace audit evidence|cannot expand scope|cannot create capabilities|cannot select nodes or models" docs/architecture/agent-scope-boundary-evals.md
rg -n "credentials|private keys|wallet keys|full hostnames|IP addresses|MAC addresses|serial numbers|machine IDs|private paths|raw user prompts|raw outputs|raw process lists|permanent hardware fingerprints" docs/agents/agent-scope-boundary-evals.md docs/architecture/agent-scope-boundary-evals.md
rg -n "boundary_tests = \"evals/agent-boundary-tests.toml\"|evals/agent-boundary-tests.toml" docs/agents/agent-package-manifest.md docs/architecture/agent-package-manifest.md docs/agents/agent-skeleton-standard.md docs/architecture/agent-skeleton-standard.md
rg -n "AGENT-SCOPE-BOUNDARY-EVALS-001 \\| ACTIVE|AGENT-LANGUAGE-POLICY-001 \\| PROPOSED" docs/roadmap/iamine-agent-network-roadmap.md
git diff --name-only origin/develop..HEAD
```

Expected:

- roadmap marks this feature active while implementation evidence is being
  prepared;
- boundary eval draft schema and skeleton path are documented;
- package manifest references use `evals/agent-boundary-tests.toml`;
- all required eval classes are documented;
- positive eval cases remain narrow and review-only;
- negative eval cases refuse, clarify, or hand off unsafe requests;
- passing evals cannot authorize install, runtime execution, registry
  publication, marketplace publication, scheduler routing, worker startup,
  model loading, trust, reputation, rewards, or mainnet behavior;
- privacy-sensitive identifiers, raw user prompts, raw outputs, and secrets
  remain prohibited;
- no source code or dependency changes are present.

## Quality Gate Policy

This feature is documentation-only. Use focused documentation checks plus
`cargo fmt` unless Architecture asks for the full workspace gate.

## Observed Local Results

```text
git diff --check: PASS
git diff --cached --check: PASS after final staging
cargo fmt --all -- --check: PASS
feature presence scan: PASS
runtime boundary scan: PASS
boundary eval schema and path scan: PASS
required eval class scan: PASS
non-bypass scan: PASS
privacy and blocked-claim scan: PASS
package manifest reference scan: PASS
roadmap ACTIVE state scan: PASS
file size guard: PASS; no Rust source growth
main.rs: unchanged
cluster_registry.rs: unchanged
```

## Field QA Decision

Field QA is not required for this documentation-only boundary eval feature
because no runtime, package installation, agent execution, installer, updater,
P2P, worker, scheduler, hardware profiler, inference, model, service-manager,
marketplace, reward, or public-beta behavior changes.

Proxmox/R5500 remains relevant for later runtime and operational features.

## Expected Results

- the scope boundary eval contract is documented;
- boundary evals remain separate from runtime execution, package installation,
  permission enforcement, sandboxing, scope, capabilities, expertise,
  resources, audit, local registry, hardware profiling, scheduler, model gates,
  reputation, routing, and runtime contracts;
- execution, parsing, generation, validation, scheduling, eval running, eval
  scoring, eval reporting, shell execution, private file access, model loading,
  sandboxing, registry publication, marketplace publication, and runtime
  integration stay unauthorized;
- unsafe, broad, missing, contradictory, stale, unverifiable, public, or
  privacy-invasive eval metadata blocks local registry review advancement,
  install, and execution by default;
- next feature remains `AGENT-LANGUAGE-POLICY-001`;
- agent runtime remains blocked;
- public beta remains blocked.
