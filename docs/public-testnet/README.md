# IAMINE Public Testnet Documentation Baseline

## Status

IAMINE is in pre-public infrastructure work.

This document is a public documentation baseline. It does not launch a public
testnet, open public onboarding, publish release artifacts, or authorize
operators to connect unattended nodes.

Public beta remains blocked until the IAMINE pre-public readiness gate is
closed by Architecture and QA.

## Current Public Scope

The current public scope is documentation only:

- explain the intended public-testnet boundary;
- identify the controls that must exist before public onboarding;
- state what prospective operators should not assume yet;
- provide a privacy-safe support and safety baseline;
- preserve the official roadmap rule that IAMINE v1.0 is the IAMINE Agent
  Network Public Beta, not an inference-only public beta.

## What Exists Before Public Launch

The repository now contains pre-public infrastructure contracts for:

- controlled public-testnet admission;
- signed auto-update eligibility;
- privacy-safe user diagnostics;
- supply-chain release evidence;
- node upgrade rollback eligibility;
- private-testnet and LAN validation history.

These contracts are prerequisites. They are not a public beta launch.

## Operator Boundary

Prospective operators should treat IAMINE public-testnet participation as closed
until admission, release, and readiness instructions are published by the
project.

Do not assume that IAMINE currently provides:

- public signup;
- public admission records;
- public bootnodes;
- public release packages;
- automatic update rollout;
- automatic rollback execution;
- public reward eligibility;
- public support intake;
- mainnet settlement;
- production uptime guarantees.

## Safety Requirements Before Public Onboarding

Public onboarding requires all of these to be true:

```text
public-testnet admission is controlled and revocable
AND node identity is operator-controlled
AND secure transport is required
AND release artifacts are authenticated
AND supply-chain evidence is accepted
AND rollback eligibility is available
AND diagnostics are privacy-safe
AND public documentation is explicit
AND Architecture readiness gate is closed
AND QA evidence is current
-> public onboarding may be considered
```

Any missing or unknown condition blocks public onboarding by default.

## Privacy Boundary

IAMINE public-testnet documentation must not ask operators to publish or commit:

- usernames;
- home directories;
- full hostnames;
- MAC addresses;
- IP addresses;
- serial numbers;
- disk UUIDs;
- machine IDs;
- user process lists;
- personal paths;
- wallet keys, private keys, tokens, secrets, or credentials.

Support evidence must use privacy-safe diagnostics and redact local paths and
host identifiers.

## Installation and Updates

Public installation and update instructions are intentionally not published in
this baseline. A future release package must include:

- authenticated artifact identity;
- checksum and signature verification;
- explicit install prefix;
- explicit service-management behavior;
- rollback plan;
- privacy-safe diagnostics;
- versioned release notes;
- QA evidence for the supported platforms.

Until those instructions exist, users should not treat local source checkout
commands as a supported public-testnet install path.

## Rollback and Recovery

Rollback is an eligibility contract in the current repository, not an active
rollback executor. A future installer or updater must decide how to collect
rollback evidence, drain active work, restore artifacts, and restart services.

Rollback must not run during active inference, model downloads, installs,
worker startup, or other constrained-host operations.

## Support

Public support intake is not open in this baseline.

When support intake opens, operators should use privacy-safe diagnostics rather
than raw logs or host-level dumps. Support bundles must not include secrets,
local paths, host identifiers, process lists, wallet keys, private keys, tokens,
or credentials.

## Roadmap Boundary

The public beta target remains:

```text
IAMINE Agent Network Public Beta
```

IAMINE must not be described as an inference-only public beta. Public-testnet
documentation is pre-public infrastructure for the later Agent Network launch.
