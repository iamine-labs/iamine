# Testnet Load Resilience

Feature:

```text
TESTNET-LOAD-RESILIENCE-001
```

## Purpose

Private testnet operation needs bounded evidence that the existing cluster
stress runner can prove concurrent request handling, partial outage recovery,
retry/fallback evidence, lifecycle integrity, and duplicate-result protection.

This feature adds a testnet resilience profile to the existing `cluster stress`
QA runner. It does not add a runtime mode, alter scheduler selection, change
peer admission, change secure transport, change remote inference request
formats, start workers outside the existing stress runner, load models,
download models, or change result acceptance.

## Contract

The standard stress profile remains the default:

```bash
./target/debug/iamine-node cluster stress --requests 0 --json
```

The new profile is explicit:

```bash
./target/debug/iamine-node cluster stress \
  --profile testnet-load-resilience \
  --requests 20 \
  --concurrency 4 \
  --json
```

The profile adds structured resilience evidence to the existing JSON and human
summary:

```text
resilience.profile
resilience.all_requests_accounted
resilience.concurrency_exercised
resilience.no_failed_requests
resilience.no_timed_out_requests
resilience.no_duplicate_results
resilience.no_duplicate_executions
resilience.no_duplicate_identities
resilience.no_incompatible_assignments
resilience.lifecycle_validated
resilience.recovery_evidence_required
resilience.recovery_evidence_observed
resilience.blocking_failures
resilience.passed
```

When the testnet profile is selected, the runner rejects ambiguous shapes:

- `--requests 1`;
- non-zero request counts with `--concurrency` below `2`;
- `--stop-on-first-failure`;
- `--require-recovery-evidence` without the testnet profile.

`--requests 0` remains a validation-only smoke that starts no Broadcast child
processes.

## Integration

The resilience rules live in:

```text
iamine-node/src/cluster_stress_resilience.rs
```

The existing stress runner owns argument parsing, runtime execution, metrics,
and output. The new module only evaluates the collected `ClusterStressMetrics`,
request observations, and lifecycle validation failures.

The summary pass condition remains additive:

```text
base stress pass
AND resilience.passed
```

## Boundaries

This feature must not reimplement scheduler, admission, transport, model
eligibility, worker execution, retry policy, fallback policy, or result
acceptance. It only turns observed evidence into a blocking QA report when the
testnet profile is requested.

This feature must not log usernames, home directories, full hostnames, MAC
addresses, IP addresses, serial numbers, disk UUIDs, machine IDs, process
lists, personal paths, keys, tokens, secrets, or credentials.

## QA Notes

Local validation must prove:

- standard profile output remains compatible;
- the testnet profile reports resilience evidence;
- invalid profile combinations fail before runtime execution;
- recovery evidence is required only when requested;
- lifecycle validation failures block the testnet profile;
- `main.rs` growth is wiring only and `cluster_registry.rs` does not change.

Because this feature extends runtime QA behavior, TS140 and Proxmox field QA are
required before merge review.
