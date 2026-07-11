# SIGNED-AUTOUPDATE-001

## Objective

Define a fail-closed signed auto-update policy before public beta. The feature
adds authenticated update eligibility rules and explicit rollout controls
without enabling background downloads, binary replacement, service restarts, or
runtime self-update behavior.

## Scope

This feature adds a pure policy module:

```text
iamine-core/src/signed_autoupdate.rs
```

The module defines:

- disabled-by-default update policy;
- controlled rollout mode;
- trusted signing key allowlist;
- per-release rollout percentage bounds;
- verified artifact and rollback requirements;
- stable decision reason codes.

## Integration

The policy lives in `iamine-core` because update eligibility is a shared release
contract, not node runtime, P2P, scheduler, worker, model, or installer domain
logic.

Release tooling or a future installer/updater may call the policy after it has
already verified release signatures and artifact digests. The module records
the verification result supplied by that caller; it does not implement
cryptographic signature verification and does not fetch remote artifacts.

## Update Gate

Default mode is:

```text
disabled
```

Controlled rollout accepts an update only when all required conditions pass:

```text
policy is controlled rollout
AND trusted signing keys are configured
AND release version is present
AND requested rollout percent is between 1 and policy maximum
AND at least one release artifact is present
AND artifact digests are valid SHA-256 hex values
AND artifact signatures are verified with trusted keys
AND rollback artifact is present and authenticated
-> signed auto-update candidate accepted
```

Any failed condition produces a stable rejection reason code.

## Runtime Boundary

This feature intentionally does not:

- download update manifests or artifacts;
- replace binaries;
- install, stop, start, enable, or restart services;
- change LAN beta packaging scripts;
- change `iamine-node` runtime startup;
- change scheduler, worker, P2P, PubSub, inference, model policy, reputation, or
  rewards behavior;
- decide public beta readiness.

Runtime activation must be a separate feature with field QA.

## Privacy and Security

The policy stores release versions, artifact IDs, artifact kinds, SHA-256
digests, signature verification status, signing key IDs, rollout percentage,
and rollback availability.

It must not collect, persist, or log usernames, home directories, hostnames, IP
addresses, MAC addresses, serial numbers, disk UUIDs, machine IDs, process
lists, personal paths, private keys, wallet keys, tokens, credentials, or
permanent hardware fingerprints.

## Risks

- Treating the policy module as an active updater would be incorrect. It only
  evaluates caller-supplied update evidence.
- Accepting artifacts with merely present signatures would be unsafe; the gate
  requires caller-supplied `Verified` signature status and trusted key IDs.
- Enabling rollout without authenticated rollback would risk stranding nodes on
  a bad release.
- Future runtime wiring must avoid update checks during active inference,
  downloads, installs, worker startup, or constrained-host operation.
