# IAMINE Model License Acceptance

MODEL-LICENSE-ACCEPTANCE-001 adds an explicit local acceptance gate for model
licenses whose policy class is `RequiresAcceptance`.

This gate complements, but does not replace:

- download policy
- trusted registry integrity
- license metadata policy
- hardware compatibility

## Contract

A `RequiresAcceptance` license can permit download, install, or existing
execution only after local acceptance is recorded for:

- `model_id`
- `license_id`
- `revision`

Changing the license revision invalidates the previous acceptance. Missing
license id or revision cannot be accepted.

## Local Store

Default path:

```text
~/.iamine/license_acceptance.json
```

Schema version:

```text
1.0.0
```

The store records only model id, license id, and revision. It must not record
usernames, home directories, hostnames, IP addresses, machine identifiers,
secrets, tokens, or wallet keys.

## CLI

```bash
iamine-node models license accept <model_id> --yes
```

Without `--yes`, the CLI must not write an acceptance record.

## Non-Goals

Acceptance is not legal advice, artifact trust, checksum verification, hardware
compatibility, scheduler policy, reputation, rewards, or model execution.
