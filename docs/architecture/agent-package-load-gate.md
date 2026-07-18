# AGENT-PACKAGE-LOAD-GATE-001

## State

```text
CLOSED
implementation: 6e4da1392e986ea8728aa1cd9209a313573b54a2
develop merge: d56cbceb09e5b44514d06128115eb54743cb5b6b
tree: c047cb7183dcd28699eea32942a6fb07fa8639c1
```

Closure confirms only the fail-closed in-memory assessment described below.
It does not close any referenced metadata validator, enforcement gate, package
filesystem loader, or agent execution path.

## Objective

Implement the first executable package-load decision boundary after root
manifest validation. The gate must reject invalid root manifests and keep every
valid package blocked while referenced metadata validators, policy reviews, and
runtime enforcement prerequisites remain unavailable.

This feature implements a load assessment. It does not load a package.

## Ownership

The gate belongs to the existing `iamine-agents` crate:

```text
iamine-agents/src/package_load.rs
```

It consumes the root parser owned by:

```text
iamine-agents/src/manifest/
```

No logic is added to `iamine-node`, `iamine-network`, `iamine-models`, or
`iamine-hardware`.

## Public API

```text
assess_package_load_yaml
PackageLoadReport
PackageLoadStatus
PackageLoadBlockerCode
```

The only input is an in-memory YAML string. A path-shaped string is treated as
YAML content and is never opened.

## Decision Flow

```text
bounded agent.yaml input
-> canonical root parser and JSON Schema
-> semantic root validation
-> typed package-load assessment
-> BLOCKED with explicit unavailable prerequisites
```

An invalid root manifest returns the existing privacy-safe `ManifestError`.
A valid root manifest returns a `PackageLoadReport` with status `Blocked`.

There is intentionally no `Allowed` status in this feature. The report cannot
be constructed publicly and does not accept caller-provided evidence booleans.
This prevents a caller from fabricating downstream validation.

## Current Blockers

The bounded report identifies these unavailable prerequisites:

- scope manifest validator;
- capability metadata validator;
- expertise metadata validator;
- resource requirements validator;
- permission model validator;
- audit policy validator;
- boundary eval validator;
- local registry review;
- language policy review;
- dependency policy review;
- runtime language compatibility;
- resource compatibility;
- human review evidence;
- input/output enforcement;
- sandbox enforcement;
- scope enforcement;
- permission enforcement;
- audit event enforcement;
- execution authorization.

Blocker codes are stable lower-snake-case strings. Messages and reports do not
retain or echo package IDs, paths, manifest contents, or private values.

## Referenced Metadata Boundary

The root manifest reference values remain opaque package-relative paths. This
feature does not choose a child authoring format, open a reference, check its
existence, follow a symlink, canonicalize a package root, or deserialize child
metadata.

Historical child contracts still contain TOML planning examples while the
newer skeleton layout names YAML files. Their owner implementations must
reconcile those formats before their blocker can be removed.

## Future Eligibility Rule

A future feature may add a positive load decision only when it consumes typed,
non-forgeable evidence from every owner validator and enforcement gate. It must
not replace those gates with strings, caller booleans, file-existence checks,
or manifest self-claims.

Adding actual package-root I/O requires a separate architecture review for:

- symlink and traversal handling;
- bounded file count and file sizes;
- package-root containment;
- race-resistant reads;
- artifact integrity;
- cleanup and cancellation;
- privacy-safe diagnostics.

## Runtime Boundary

This feature does not:

- load, install, register, or execute a package;
- read or write files;
- follow manifest references;
- grant permissions;
- start a sandbox or lifecycle;
- emit audit events;
- select a node, worker, model, or backend;
- modify scheduler, P2P, PubSub, inference, hardware, or CLI behavior;
- authorize functional Node Doctor development.

## Field QA Decision

Field QA is not required. The API is deterministic, in-memory, and has no
runtime, filesystem, hardware, worker, scheduler, network, or inference wiring.

## Integration

This feature consumes:

```text
AGENT-MANIFEST-PARSER-VALIDATOR-001
AGENT-RUNTIME-BASELINE-001
```

It remains blocked on the validators and enforcement features represented by
its typed blocker list. The next runtime implementation should be selected from
those explicit blockers without changing the official roadmap order.
