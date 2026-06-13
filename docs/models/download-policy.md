# IAMINE Model Download Policy

MODEL-DOWNLOAD-POLICY-001 defines the first explicit gate before a model can enter
the normal IAMINE download and storage flow.

This policy is intentionally not the final trusted-registry gate. It preserves the
current default registry models while marking missing trust data, such as missing
checksums, as pending instead of silently trusted.

## Allowed Inputs

- Registry-known model IDs only.
- HTTPS source URLs from approved model sources.
- GGUF artifacts.
- Model IDs using ASCII letters, numbers, `.`, `_`, or `-`.
- Version strings using ASCII letters, numbers, `.`, `_`, or `-`.
- Model sizes at or below the configured maximum policy size.

Current approved source:

- `huggingface.co`
- subdomains of `huggingface.co`
- `hf.co`

Current approved format:

- `gguf`

## Policy Status

The policy can return these statuses:

- `allowed`
- `blocked`
- `staged`
- `quarantined`
- `metadata_only`
- `pending_checksum`
- `pending_license`
- `pending_hardware_validation`

For this milestone, known registry models with missing SHA256 metadata are allowed
to proceed as `pending_checksum`. This keeps current TS140 and Proxmox workflows
compatible while making the missing trust requirement visible.

## Blocking Reasons

The policy can report:

- `unknown_model`
- `invalid_model_id`
- `invalid_version`
- `unsupported_format`
- `untrusted_source`
- `source_url_missing`
- `size_exceeds_policy`
- `checksum_missing`
- `checksum_mismatch`
- `manual_model_not_allowed`
- `manifest_missing`

Checksum mismatches are always blocking when an expected and actual checksum are
both available.

## Deferred Gates

The following gates are intentionally deferred:

- MODEL-LICENSE-GATE-001
- MODEL-HARDWARE-COMPATIBILITY-001
- MODEL-TRUSTED-REGISTRY-INTEGRITY-001

Until those gates land, `pending_checksum` and related pending states are not the
same thing as trusted execution. They are explicit markers for future hardening.
