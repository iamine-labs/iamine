# IAMINE Model Network Policy

MODEL-NETWORK-POLICY-GATE-001 defines the explicit model metadata gate that
decides whether a model may be used for distributed IAMINE network inference.

This gate is intentionally separate from:

- download policy
- trusted registry integrity
- license policy
- license acceptance
- hardware compatibility
- backend availability
- scheduler selection
- P2P transport startup
- rewards or reputation

## Metadata

Each registry descriptor may carry:

- `policy_class`
- `revision`

Supported policy classes:

- `DistributedAllowed`: local use and distributed network inference are allowed.
- `LocalOnly`: local download, install, and execution may proceed, but
  distributed network inference is blocked.
- `Blocked`: read-only listing may report the model, but new download, install,
  execution, and network inference are blocked.

Missing network policy metadata is not treated as network-approved.

## Operations

The evaluator distinguishes:

- `list`
- `download`
- `install`
- `existing_execution`
- `network_inference`

List operations are read-only and may expose pending metadata. New download and
install flows require explicit policy metadata. Legacy installed models with
missing policy metadata may continue through existing-execution paths with an
explicit `legacy_installed_model` reason, but they are not approved for
distributed network inference.

## Status

The gate returns:

- `allowed`
- `local_only`
- `pending_metadata`
- `legacy_execution`
- `blocked`

## Reasons

The gate returns stable reasons:

- `network_policy_allowed`
- `local_only`
- `network_policy_missing`
- `network_policy_blocked`
- `legacy_installed_model`

## Non-Goals

The evaluator does not start networking, subscribe to PubSub topics, contact
bootnodes, inspect peer state, download artifacts, load models, decide hardware
compatibility, decide backend availability, or alter scheduler policy.
