# IAMINE Model License Policy

MODEL-LICENSE-GATE-001 adds a deterministic local gate for declared model
license metadata before IAMINE allows a new model download or install.

This gate complements the download policy. It does not replace checksum,
artifact integrity, hardware compatibility, or trusted registry validation.

## Contract

A model operation is admitted only when both gates permit it:

- download policy permits the model/source/format/size/checksum transition
- license policy permits the requested operation

An allowed license never overrides a blocked download policy. A permitted
download transition never overrides a blocked license policy.

## License Metadata

Each registry descriptor carries structured license metadata:

- `license_id`
- `license_url`
- `policy_class`
- `requires_acceptance`
- `revision`

Missing metadata is explicit. It is not treated as approved.

## Policy Classes

- `Allowed`: the license policy permits download, install, and existing
  execution when metadata is coherent.
- `RequiresAcceptance`: download and install are blocked until a future
  explicit acceptance feature exists.
- `Restricted`: download and install are blocked.

## Operations

- `List`
- `Download`
- `Install`
- `ExistingExecution`

The operation matters because legacy installed models may continue running
while their license metadata is still pending.

## Statuses

- `allowed`
- `requires_acceptance`
- `pending_metadata`
- `pending_review`
- `blocked`

## Reasons

- `license_allowed`
- `license_acceptance_required`
- `license_missing`
- `license_unknown`
- `license_blocked`
- `license_metadata_conflict`
- `license_id_invalid`
- `license_url_invalid`
- `legacy_installed_model`

## Transitional Compatibility

Existing installed models can continue executing when license metadata is
missing:

```text
operation = ExistingExecution
installed = true
license metadata missing
=> status = pending_metadata
=> reason = legacy_installed_model
=> permits_operation = true
```

This does not license-approve the model. It does not permit a new download,
reinstall, copy, or trust promotion.

For a new download or install:

```text
license metadata missing
=> status = pending_metadata
=> reason = license_missing
=> permits_operation = false
```

## URL Rules

When present, a license URL must be syntactically valid enough for local
policy evaluation and must use `https`.

The evaluator does not fetch license URLs, scrape external pages, or interpret
legal text.

## Deferred Gates

The following work is intentionally deferred:

- MODEL-LICENSE-ACCEPTANCE-001
- MODEL-HARDWARE-COMPATIBILITY-001
- MODEL-TRUSTED-REGISTRY-INTEGRITY-001

License approval is not artifact trust, checksum verification, registry
integrity, or legal advice.
