# IAMINE Agent OS Diagnostic Template QA

Feature:

```text
AGENT-TEMPLATE-OS-DIAGNOSTIC-001
```

## Objective

Validate that OS diagnostic template policy is roadmap-aligned,
documentation-only, privacy-safe, evidence-bound, blocked by default, and does
not authorize system probes, command execution, file reads, process inspection,
network probes, persistence, publication, model loading, or distributed model
MoE.

## Identity

```text
Branch: feature/agent-template-os-diagnostic-001
HEAD before implementation: 94ddac67e8c9c0e32f094c0acf89f17309512916
Base: origin/develop
Runtime behavior change: none
```

## Required Local Validation

```bash
git diff --check
git diff --cached --check
cargo fmt --all -- --check
rg -n "AGENT-TEMPLATE-OS-DIAGNOSTIC-001" docs/agents/agent-template-os-diagnostic.md docs/architecture/agent-template-os-diagnostic.md docs/qa/agent-template-os-diagnostic.md docs/roadmap/iamine-agent-network-roadmap.md
rg -n "summarize_operator_provided_os_facts|classify_missing_os_context|identify_privacy_sensitive_fields|handoff_for_operator_approved_probe" docs/agents/agent-template-os-diagnostic.md
rg -n "os_metadata_source_policy|platform_scope_policy|unsupported_probe_policy|identity_redaction_policy|process_data_policy|network_metadata_policy" docs/agents/agent-template-os-diagnostic.md
rg -n "documentation-only|does not authorize|Runtime behavior change: none" docs/agents/agent-template-os-diagnostic.md docs/architecture/agent-template-os-diagnostic.md docs/qa/agent-template-os-diagnostic.md
rg -n "cannot authorize runtime execution|cannot implement probes or shell adapters|cannot collect host identity|cannot read arbitrary files or inspect|cannot probe networks or mutate state" docs/architecture/agent-template-os-diagnostic.md
rg -n "credentials|private keys|wallet keys|full hostnames|IP addresses|MAC addresses|serial numbers|machine IDs|private paths|raw user prompts|raw outputs|raw process lists|permanent hardware fingerprints" docs/agents/agent-template-os-diagnostic.md
rg -n "AGENT-TEMPLATE-OS-DIAGNOSTIC-001 \\| ACTIVE|IAMINE-DEV-SETUP-AGENT-001-INTERNAL \\| PROPOSED" docs/roadmap/iamine-agent-network-roadmap.md
```

## Observed Local Results

```text
git diff --check: PASS
git diff --cached --check: PASS after final staging
cargo fmt --all -- --check: PASS
feature presence scan: PASS
os diagnostic scope scan: PASS
required guard scan: PASS
runtime boundary scan: PASS
non-bypass scan: PASS
privacy scan: PASS
roadmap ACTIVE state scan: PASS
file size guard: PASS
main.rs: unchanged
cluster_registry.rs: unchanged
```

## Field QA Decision

Field QA is not required for this documentation-only OS diagnostic template
policy feature because no runtime, system probe, command execution, file
access, process inspection, network probe, agent execution, installer, updater,
P2P, worker, scheduler, hardware profiler, inference, model, marketplace,
reward, or public-beta behavior changes.
