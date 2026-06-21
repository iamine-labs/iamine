# IAMINE Trusted Registry Integrity Policy

MODEL-TRUSTED-REGISTRY-INTEGRITY-001 adds a deterministic local gate for
registry descriptor integrity before IAMINE allows a new model download or
install.

This gate complements, but does not replace:

- download policy: source, format, size, and download transition status
- license policy: license metadata and operation permission
- hardware compatibility: local hardware eligibility

## Contract

A model descriptor is trusted for new download/install only when it has a real
SHA256 checksum in the registry descriptor.

Missing, placeholder, skipped, or malformed checksums block new download and
install flows. List operations remain read-only and report the integrity state
without creating files or contacting the network.

Legacy installed models may continue through existing-execution paths with an
explicit `legacy_installed_model` reason, but this does not promote the registry
descriptor to trusted.

## Statuses

- `trusted`
- `pending_integrity`
- `legacy_execution`
- `blocked`

## Reasons

- `trusted_registry_descriptor`
- `checksum_missing`
- `checksum_placeholder`
- `checksum_invalid`
- `legacy_installed_model`

## Non-Goals

The evaluator does not fetch remote manifests, scrape model pages, compute
artifact hashes before download, decide license policy, decide hardware
compatibility, start workers, or affect scheduler policy.
