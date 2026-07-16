# IAMINE Reporter Agent Skeleton QA

Feature:

```text
REPORTER-AGENT-001-SKELETON
```

## Objective

Validate that the second official P0 Reporter skeleton is roadmap-aligned,
documentation-only, local-readonly by default, privacy-safe, scope-bound, and
does not authorize an executable package, evidence collection, report export,
runtime integration, command execution, filesystem access, network access,
mutation, publication, model activity, or distributed model MoE.

## Identity

```text
Branch: feature/reporter-agent-001-skeleton
HEAD before implementation: beb5075bc44c978e440abfee4984e7150ccd52b3
Base: origin/develop
Runtime behavior change: none
```

## Scope Checks

Expected changed paths:

```text
docs/agents/reporter-agent-skeleton.md
docs/architecture/reporter-agent-skeleton.md
docs/qa/reporter-agent-skeleton.md
docs/roadmap/iamine-agent-network-roadmap.md
```

The feature must not modify Rust source, Cargo manifests, package artifacts,
runtime startup, agent execution, evidence collection, report generation,
redaction implementation, persistence, export, network transfer, sandboxing,
workers, schedulers, queues, P2P, PubSub, model selection, model loading,
downloads, inference, hardware profiling, CLI behavior, registry storage,
marketplace behavior, installer, updater, rewards, wallet, settlement, public
beta, or mainnet.

## Required Local Validation

```bash
git diff --check
git diff --cached --check
cargo fmt --all -- --check
rg -n "REPORTER-AGENT-001-SKELETON" docs/agents/reporter-agent-skeleton.md docs/architecture/reporter-agent-skeleton.md docs/qa/reporter-agent-skeleton.md docs/roadmap/iamine-agent-network-roadmap.md
rg -n "iamine.beta.support-reporter|privacy_safe_support_report|local_readonly|execution_authorized: false" docs/agents/reporter-agent-skeleton.md
rg -n "operator_provided_symptom_summary|redacted_diagnostic_snippet|redacted_iamine_support_bundle_summary|operator_report_intent" docs/agents/reporter-agent-skeleton.md
rg -n "support_report|result_summary|clarification_request|handoff_request|refusal_report|blocked_action_report" docs/agents/reporter-agent-skeleton.md
rg -n "review_started|scope_checked|permission_checked|redaction_checked|unsupported_claim_blocked|handoff_required|refusal_recorded" docs/agents/reporter-agent-skeleton.md
rg -n "collect files|read arbitrary files|execute shell commands|export|scan the LAN|contact third-party support|model loads|dynamic hardware probes|publish to a registry" docs/agents/reporter-agent-skeleton.md
rg -n "documentation-only|does not modify Rust source|local_readonly|User confirmation cannot elevate a blocked action|iamine-node support bundle" docs/architecture/reporter-agent-skeleton.md
rg -n "credentials|private keys|wallet material|full hostnames|IP addresses|MAC addresses|serial numbers|machine IDs|private paths|raw process lists|unredacted logs|permanent hardware fingerprints" docs/agents/reporter-agent-skeleton.md
rg -n "REPORTER-AGENT-001-SKELETON \\| ACTIVE|LAN-FILE-SHARE-ASSISTANT-AGENT-001-SKELETON \\| PROPOSED" docs/roadmap/iamine-agent-network-roadmap.md
```

## Acceptance Criteria

- The skeleton describes one narrow local support report, not a general support
  automation or evidence-collection agent.
- The only planned mode is `local_readonly`.
- Inputs are operator-provided, redacted, and evidence-limited by default.
- Outputs distinguish supplied evidence, missing evidence, and unsupported
  claims.
- Required future audit evidence is redacted, operator-local, and review-only.
- Blocked actions include collection, shell, arbitrary filesystem, mutation,
  process or service control, network or LAN activity, report export, third-
  party contact, model activity, and publication.
- Private machine data and secrets are explicitly blocked.
- Ambiguous, unsafe, cross-domain, permission-escalation, prompt-injection,
  role-confusion, and unsupported-claim requests require refusal,
  clarification, or handoff.
- The existing `iamine-node support bundle` command remains out of scope and
  unchanged.
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
does not change runtime, agent execution, report generation, evidence
collection, CLI behavior, worker behavior, scheduler behavior, capabilities,
cluster status, P2P, hardware profiling, model execution, or any Mac, TS140,
or Proxmox/R5500 surface.

Any later functional Reporter implementation must receive a fresh QA plan and
field-QA decision based on its actual runtime scope.
