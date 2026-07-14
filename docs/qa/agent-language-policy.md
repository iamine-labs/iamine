# IAMINE Agent Language Policy QA

Feature:

```text
AGENT-LANGUAGE-POLICY-001
```

## Objective

Validate that the agent language policy contract is roadmap-aligned,
documentation-only, phase-bound, privacy-safe, blocked by default, and does not
authorize package installation, runtime language execution, dependency
installation, package manager execution, sandboxing, permission enforcement,
scheduler placement, worker startup, model loading, public registry
publication, marketplace behavior, or distributed model MoE behavior.

## Identity

Record before QA:

```text
Branch: feature/agent-language-policy-001
HEAD before implementation: 4e249c108d5f2d30b9678d1393cbc2a1cb18128d
Tree before implementation: cb822e9d3bed68c1001ebc8a2b79ea25cc1cfc3f
Base: origin/develop
origin/develop: 4e249c108d5f2d30b9678d1393cbc2a1cb18128d
tracked clean: yes before implementation; no after expected documentation delta
staging clean: yes before final staging
untracked baseline: expected new feature docs only
untracked files:
- docs/agents/agent-language-policy.md
- docs/architecture/agent-language-policy.md
- docs/qa/agent-language-policy.md
```

## Scope Checks

Expected changed paths:

```text
docs/agents/agent-language-policy.md
docs/architecture/agent-language-policy.md
docs/qa/agent-language-policy.md
docs/roadmap/iamine-agent-network-roadmap.md
```

Expected runtime behavior change:

```text
none
```

This feature must not modify Rust source, tests, scripts, runtime, executable
agent packages, package installation, package manager integration, dependency
installation, skeleton generator, package parser, scope parser, capability
parser, expertise parser, resource parser, permission parser, audit parser,
boundary eval parser, registry storage implementation, registry
synchronization, registry publication, permission enforcement, sandboxing,
runtime audit logging, router, scheduler, worker behavior, hardware profiler,
model policy, inference execution, installer, updater, rollback, reputation,
rewards, wallet, marketplace, public beta, or mainnet behavior.

## Required Local Validation

```bash
git diff --check
git diff --cached --check
cargo fmt --all -- --check
rg -n "AGENT-LANGUAGE-POLICY-001" docs/agents/agent-language-policy.md docs/architecture/agent-language-policy.md docs/qa/agent-language-policy.md docs/roadmap/iamine-agent-network-roadmap.md
rg -n "documentation-only|does not authorize|does not modify|runtime behavior change" docs/agents/agent-language-policy.md docs/architecture/agent-language-policy.md docs/qa/agent-language-policy.md
rg -n "iamine.agent.language_policy.draft-0.1|Rust|Python|TypeScript|WASM/WASI|Containers|runtime_available|dependency_policy_required|sandbox_policy_required" docs/agents/agent-language-policy.md docs/architecture/agent-language-policy.md
rg -n "Authoring: YAML|Internal representation: Rust structs|Validation: generated JSON Schema|Runtime/API payloads: JSON|Source of truth: Rust types" docs/agents/agent-language-policy.md docs/architecture/agent-language-policy.md docs/roadmap/iamine-agent-network-roadmap.md
rg -n "cannot authorize package installation|cannot authorize runtime execution|cannot select a runtime mode|cannot install dependencies|cannot authorize package managers|cannot create sandbox availability|cannot grant permissions|cannot expand scope" docs/architecture/agent-language-policy.md
rg -n "credentials|private keys|wallet keys|full hostnames|IP addresses|MAC addresses|serial numbers|machine IDs|private paths|raw user prompts|raw outputs|raw process lists|permanent hardware fingerprints" docs/agents/agent-language-policy.md docs/architecture/agent-language-policy.md
rg -n "AGENT-LANGUAGE-POLICY-001 \\| ACTIVE|AGENT-DEPENDENCY-POLICY-001 \\| PROPOSED" docs/roadmap/iamine-agent-network-roadmap.md
git diff --name-only origin/develop..HEAD
```

Expected:

- roadmap marks this feature active while implementation evidence is being
  prepared;
- language policy draft identifier is documented;
- Rust is limited to IAMINE-owned core/runtime/validator/official-agent
  implementation contexts;
- Python, TypeScript, WASM/WASI, and containers remain deferred;
- language allowance cannot authorize install, runtime execution, package
  manager execution, dependency installation, sandbox availability, registry
  publication, marketplace publication, scheduler routing, worker startup,
  model loading, trust, reputation, rewards, or mainnet behavior;
- metadata format policy remains explicit;
- privacy-sensitive identifiers, raw user prompts, raw outputs, and secrets
  remain prohibited;
- no source code, dependency, or package manager changes are present.

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
language placement scan: PASS
metadata format policy scan: PASS
non-bypass scan: PASS
privacy and blocked-claim scan: PASS
roadmap ACTIVE state scan: PASS
file size guard: PASS; no Rust source growth
main.rs: unchanged
cluster_registry.rs: unchanged
```

## Field QA Decision

Field QA is not required for this documentation-only language policy feature
because no runtime, package installation, package manager, dependency
installation, agent execution, installer, updater, P2P, worker, scheduler,
hardware profiler, inference, model, service-manager, marketplace, reward, or
public-beta behavior changes.

Proxmox/R5500 remains relevant for later runtime and operational features.

## Expected Results

- the agent language policy contract is documented;
- language policy remains separate from runtime execution, package
  installation, package managers, dependency installation, sandboxing,
  permission enforcement, scope, capabilities, expertise, resources, audit,
  boundary evals, local registry, hardware profiling, scheduler, model gates,
  reputation, routing, and runtime contracts;
- execution, parsing, generation, validation, scheduling, shell execution,
  private file access, model loading, sandboxing, registry publication,
  marketplace publication, and runtime integration stay unauthorized;
- unsafe, broad, missing, contradictory, stale, unverifiable, public, or
  privacy-invasive language metadata blocks local registry review advancement,
  install, and execution by default;
- next feature remains `AGENT-DEPENDENCY-POLICY-001`;
- agent runtime remains blocked;
- public beta remains blocked.
