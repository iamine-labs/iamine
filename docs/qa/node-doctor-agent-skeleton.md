# IAMINE Node Doctor Agent Skeleton QA

Feature:

```text
NODE-DOCTOR-AGENT-001-SKELETON
```

## Objective

Validate that the first official P0 Node Doctor skeleton is roadmap-aligned,
documentation-only, local-readonly by default, privacy-safe, scope-bound, and
does not authorize an executable package, runtime integration, command
execution, filesystem access, network access, mutation, publication, model
activity, or distributed model MoE.

## Identity

```text
Branch: feature/node-doctor-agent-001-skeleton
HEAD before implementation: 27d81e3e334ad129b4ab24c0b2fc84155e99e72f
Base: origin/develop
Runtime behavior change: none
```

## Scope Checks

Expected changed paths:

```text
docs/agents/node-doctor-agent-skeleton.md
docs/architecture/node-doctor-agent-skeleton.md
docs/qa/node-doctor-agent-skeleton.md
docs/roadmap/iamine-agent-network-roadmap.md
```

The feature must not modify Rust source, Cargo manifests, package artifacts,
runtime startup, agent execution, sandboxing, workers, schedulers, queues,
P2P, PubSub, model selection, model loading, downloads, inference, hardware
profiling, persistence, CLI behavior, registry storage, marketplace behavior,
installer, updater, rewards, wallet, settlement, public beta, or mainnet.

## Required Local Validation

```bash
git diff --check
git diff --cached --check
cargo fmt --all -- --check
rg -n "NODE-DOCTOR-AGENT-001-SKELETON" docs/agents/node-doctor-agent-skeleton.md docs/architecture/node-doctor-agent-skeleton.md docs/qa/node-doctor-agent-skeleton.md docs/roadmap/iamine-agent-network-roadmap.md
rg -n "iamine.beta.node-doctor|node_readiness_diagnostic_report|local_readonly|execution_authorized: false" docs/agents/node-doctor-agent-skeleton.md
rg -n "iamine_node_status_summary|iamine_readiness_checklist|redacted_hardware_profile_summary|user_provided_error_text" docs/agents/node-doctor-agent-skeleton.md
rg -n "diagnostic_report|result_summary|clarification_request|handoff_request|refusal_report|blocked_action_report" docs/agents/node-doctor-agent-skeleton.md
rg -n "review_started|scope_checked|permission_checked|redaction_checked|handoff_required|refusal_recorded" docs/agents/node-doctor-agent-skeleton.md
rg -n "execute shell commands|read arbitrary files|start, stop, restart|scan the LAN|model loads|dynamic hardware probes|publish to a registry" docs/agents/node-doctor-agent-skeleton.md
rg -n "documentation-only|does not modify Rust source|local_readonly|User confirmation cannot elevate a blocked action|iamine-node lan doctor" docs/architecture/node-doctor-agent-skeleton.md
rg -n "credentials|private keys|wallet material|full hostnames|IP addresses|MAC addresses|serial numbers|machine IDs|private paths|raw process lists|unredacted logs|permanent hardware fingerprints" docs/agents/node-doctor-agent-skeleton.md
rg -n "NODE-DOCTOR-AGENT-001-SKELETON \\| ACTIVE|REPORTER-AGENT-001-SKELETON \\| PROPOSED" docs/roadmap/iamine-agent-network-roadmap.md
```

## Acceptance Criteria

- The skeleton describes one narrow node-readiness report, not a general
  system-administration agent.
- The only planned mode is `local_readonly`.
- Inputs and outputs are classified and privacy-safe by default.
- Required future audit evidence is redacted, operator-local, and review-only.
- Blocked actions include shell, arbitrary filesystem, mutation, process or
  service control, network or LAN activity, model activity, and publication.
- Private machine data and secrets are explicitly blocked.
- Ambiguous, unsafe, cross-domain, permission-escalation, prompt-injection,
  and role-confusion requests require refusal, clarification, or handoff.
- The existing `iamine-node lan doctor` CLI remains out of scope and unchanged.
- `main.rs` and `cluster_registry.rs` remain unchanged.

## Observed Local Results

```text
git diff --check: PASS
git diff --cached --check: PASS
cargo fmt --all -- --check: PASS
feature presence scan: PASS
identity and planned-mode scan: PASS
input and output boundary scans: PASS
audit-boundary scan: PASS
blocked-action scan: PASS
architecture-boundary scan: PASS
privacy scan: PASS
roadmap ACTIVE and next-feature scan: PASS
staged scope: PASS; exactly four documentation paths
main.rs: unchanged
cluster_registry.rs: unchanged
```

## Field QA Decision

Field QA is not required for this documentation-only P0 skeleton feature. It
does not change runtime, agent execution, CLI behavior, worker behavior,
scheduler behavior, capabilities, cluster status, P2P, hardware profiling,
model execution, or any Mac, TS140, or Proxmox/R5500 surface.

Any later functional Node Doctor implementation must receive a fresh QA plan
and field-QA decision based on its actual runtime scope.
