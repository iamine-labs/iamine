# NODE-UPGRADE-ROLLBACK-001

## Objective

Define a fail-closed node upgrade rollback policy before public beta. The
feature adds explicit rollback eligibility rules for failed or incompatible node
upgrades without executing rollback, replacing binaries, restarting services, or
changing runtime behavior.

## Scope

This feature adds a pure policy module:

```text
iamine-core/src/node_upgrade_rollback.rs
```

The module defines:

- disabled-by-default rollback policy;
- controlled recovery mode;
- bounded trusted signing key allowlist;
- bounded allowed rollback version list;
- explicit failed or incompatible upgrade evidence;
- operator confirmation, drained tasks, snapshot, and config backup
  requirements;
- verified rollback artifact requirements;
- stable decision reason codes;
- a rollback plan that names the accepted target version and restorable
  artifacts.

## Integration

The policy lives in `iamine-core` because rollback eligibility is a shared
release and operations contract, not node runtime, P2P, scheduler, worker,
model, installer, updater, or service-manager execution logic.

Release tooling, installers, or future updater code may call the policy after
they have already collected local upgrade failure evidence, drained work, and
verified rollback artifact signatures and digests. The module records the
verification result supplied by that caller; it does not implement
cryptographic signature verification, package extraction, service control, or
filesystem rollback.

## Rollback Gate

Default mode is:

```text
disabled
```

Controlled recovery accepts rollback only when all required conditions pass:

```text
policy is controlled recovery
AND trusted signing keys are configured within the policy limit
AND allowed rollback versions are configured within the policy limit
AND current, failed, and rollback versions are present
AND current version matches the failed upgrade version
AND rollback version differs from the current failed version
AND rollback version is explicitly allowed by policy
AND upgrade state is failed or incompatible
AND operator confirmation is present when required
AND active tasks are drained
AND pre-upgrade snapshot is available
AND config backup is available
AND rollback artifacts are present within the artifact limit
AND artifact versions match the rollback version
AND artifact digests are valid SHA-256 hex values
AND artifact signatures are verified with trusted keys
AND at least one artifact is restorable
-> node upgrade rollback accepted
```

Any failed condition produces a stable rejection reason code.

## Boundary

This feature intentionally does not:

- rollback node binaries;
- download rollback manifests or artifacts;
- install, stop, start, enable, disable, or restart services;
- mutate node config or restore backups;
- change signed auto-update rollout policy;
- change supply-chain release policy;
- change LAN beta packaging scripts;
- change `iamine-node` runtime startup;
- change scheduler, worker, P2P, PubSub, inference, model policy, reputation, or
  rewards behavior;
- decide public beta readiness.

Runtime or installer activation must be a separate feature with field QA.

## Privacy and Security

The policy stores release versions, artifact IDs, artifact kinds, SHA-256
digests, signature verification status, signing key IDs, upgrade state, and
boolean recovery readiness evidence.

It must not collect, persist, or log usernames, home directories, hostnames, IP
addresses, MAC addresses, serial numbers, disk UUIDs, machine IDs, process
lists, personal paths, private keys, wallet keys, tokens, credentials, or
permanent hardware fingerprints.

## Risks

- Treating the policy module as an active rollback executor would be incorrect.
  It only evaluates caller-supplied rollback evidence.
- Rolling back while tasks are active can interrupt inference; controlled
  recovery requires drained tasks.
- Rolling back without a pre-upgrade snapshot or config backup can strand nodes
  in a partially downgraded state.
- Allowing arbitrary rollback versions risks downgrading past known fixed
  versions; the policy requires an explicit bounded allowlist.
- Future runtime wiring must avoid rollback checks during active inference,
  downloads, installs, worker startup, or constrained-host operation.
