# V1-SUPPLY-CHAIN-SECURITY-001

## Objective

Define a fail-closed supply-chain security gate for IAMINE release candidates
before public beta. The feature records the minimum evidence required for a
release artifact to be considered eligible for release review.

This feature does not publish, sign, upload, download, install, update, or
replace artifacts.

## Scope

This feature adds a pure policy module:

```text
iamine-core/src/supply_chain_security.rs
```

The module defines:

- disabled-by-default supply-chain policy;
- controlled release mode;
- bounded trusted builder allowlist;
- source commit and tree evidence;
- tracked and staged worktree cleanliness evidence;
- dependency lockfile digest evidence;
- cargo-audit and cargo-deny status evidence;
- secret-scan status evidence;
- isolated, reproducible, tested build evidence;
- build and artifact provenance verification evidence;
- artifact digest and source-matching evidence;
- stable decision reason codes.

## Integration

The policy lives in `iamine-core` because release provenance is a shared
release-engineering contract, not node runtime, P2P, scheduler, worker, model,
installer, or updater domain logic.

Release tooling may later call this policy after it has already produced or
verified provenance, dependency, secret-scan, and artifact evidence. This
feature only evaluates caller-supplied evidence.

`SIGNED-AUTOUPDATE-001` remains responsible for update eligibility and signed
rollout policy. This feature does not replace that gate. A future release flow
may require both gates:

```text
supply-chain evidence accepted
AND signed auto-update policy accepted
-> release candidate may proceed to release review
```

## Supply-Chain Gate

Default mode is:

```text
disabled
```

Controlled release accepts a candidate only when all required conditions pass:

```text
policy is controlled release
AND trusted builder list is present and bounded
AND release version is present
AND source commit SHA is valid
AND source tree SHA is valid
AND tracked worktree is clean
AND staging area is clean
AND Cargo.lock SHA-256 digest is valid
AND cargo-audit passed or has an accepted baseline exception
AND cargo-deny passed or has an accepted baseline exception
AND secret scan passed
AND builder is trusted
AND build source commit/tree match the candidate source
AND build was isolated
AND build was reproducible
AND build tests passed
AND build provenance is verified
AND at least one artifact is present
AND artifact count is bounded
AND each artifact has an ID
AND each artifact digest is valid SHA-256
AND each artifact source commit/tree match the candidate source
AND each artifact builder is trusted
AND each artifact provenance is verified
-> supply-chain candidate accepted
```

Missing, failing, skipped, invalid, untrusted, dirty, or mismatched evidence
rejects the candidate with a stable reason code. Skipped dependency checks are
accepted only when explicitly marked as an accepted baseline exception. Secret
scan evidence must pass and cannot be replaced by a baseline exception.

## Runtime Boundary

This feature intentionally does not:

- change `iamine-node` startup;
- start workers, P2P, PubSub, model downloads, model loads, inference, or
  service managers;
- change scheduler, worker, model, reputation, rewards, or task behavior;
- change installer scripts or update execution;
- fetch release manifests or remote artifacts;
- perform cryptographic signing or signature verification;
- upload packages;
- publish releases;
- decide public beta readiness.

Runtime activation, release publishing, package installation, and updater
execution must remain separate features with their own QA.

## Privacy and Security

The policy stores release versions, source commit/tree IDs, lockfile digests,
builder IDs, check statuses, artifact IDs, artifact kinds, artifact SHA-256
digests, and provenance verification statuses.

It must not collect, persist, or log usernames, home directories, full
hostnames, IP addresses, MAC addresses, serial numbers, disk UUIDs, machine
IDs, process lists, personal paths, private keys, wallet keys, tokens,
credentials, or permanent hardware fingerprints.

Builder IDs must be operator-defined release identities, not machine
hostnames.

## Risks

- Treating this policy as a release publisher would be incorrect. It only
  evaluates evidence.
- Duplicating signed auto-update logic would blur gate ownership. This feature
  validates provenance and artifact/source evidence, while signed auto-update
  validates signed rollout eligibility.
- Accepting skipped security checks without an explicit baseline exception
  would weaken release review.
- Accepting dirty source or mismatched build/artifact source would break
  provenance.
- Future wiring must avoid release checks during active inference, downloads,
  installs, model loads, worker startup, or constrained-host operation.
