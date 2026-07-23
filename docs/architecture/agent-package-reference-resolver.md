# AGENT-PACKAGE-REFERENCE-RESOLVER-001

## State

```text
MERGED / VALIDATED / CLOSED
branch: feature/agent-package-reference-resolver-001
base: c018e4a25aa054c23f2f5818f0f946eace47922f
base tree: 94a3a734e8d79b287af093765ccd0f9043487d0d
source commit: 47b0d3ecbb599b81cc8e97129f275028a8d87176
source tree: 7f6c42373df9781046ae1fefceddee293bcaec74
QA closeout commit: 77342969c7561ecd461d83c8a51396e51ab1c9a1
merge commit: c013f10f267ea13451ea205b8cb3a56b9ac12246
merged tree: 6084e7b3ea05df19471ec96292d7b7bc0e75a35f
runtime behavior change: bounded package filesystem reads
field QA: passed on Mac, TS140, and Proxmox/R5500
post-merge validation: accepted with explicit baseline exceptions
```

## Objective

Resolve the seven paths in a typed `ManifestReferences` value against an
explicit package-root capability. The resolver returns bounded bytes only. It
does not validate referenced metadata, create review evidence, remove a
package-load blocker, load a package, or execute an agent.

## Filesystem Architecture

The implementation uses `cap-std` and `cap-fs-ext` `4.0.2`.
`PackageReferenceResolver::open_ambient` is the only ambient filesystem
boundary. It opens the caller-selected root's parent and then opens the package
root with `open_dir_nofollow`. Every package-controlled path is subsequently
resolved relative to that root capability.

Each reference:

1. passes platform-independent lexical validation;
2. is limited to 512 bytes and 16 components;
3. rejects absolute, parent, current-directory, Windows-prefix, and backslash
   forms;
4. walks parent directories with `open_dir_nofollow`;
5. compares each directory name's device/inode identity with its open handle;
6. opens the final component with `FollowSymlinks::No`;
7. compares the file name's device/inode identity with its open handle;
8. requires a regular file with one hard link;
9. enforces pre-read metadata size;
10. reads at most `max_file_bytes + 1`;
11. seeks and reads the same file handle a second time;
12. rejects content or metadata changes observed across the bounded reads.

Directory handles keep traversal anchored if path names are renamed after
opening. The double read is race-aware, not an assertion that mutable files can
be made cryptographically immutable. Trusted review evidence remains a later
owner.

## Limits

Hard maximums:

```text
references: 7
reference bytes: 512
path components: 16
file bytes: 64 KiB
total bytes: 448 KiB
```

`ResolverLimits::try_new` may select stricter values but cannot exceed these
caps or use zero values.

## Public Contract

- `PackageReferenceResolver`: owns the root directory capability and limits.
- `ResolverLimits`: validated bounds.
- `PackageReferenceKind`: stable labels for the seven manifest references.
- `ResolvedReference`: kind and bytes with redacted Debug output.
- `ResolvedPackageReferences`: bounded collection and total byte count.
- `ResolverError`: stable code plus optional reference kind, never a host path
  or raw `io::Error`.

## Security And Privacy

- Package-controlled paths never become ambient paths.
- Symlinks in the root, parent components, or final component are rejected.
- Hard-linked files are rejected.
- Only regular files are read.
- Duplicate references fail closed.
- File contents, root paths, declared paths, host errors, and temporary paths
  do not appear in resolver Debug or error output.
- Missing, contradictory, unreadable, or changing inputs remain blocked.

## Explicitly Out Of Scope

- parsing or semantically validating referenced bytes;
- local registry, language, dependency, or human-review evidence;
- runtime or resource compatibility;
- input/output policy, sandboxing, lifecycle, timeout, handoff, routing, or
  audit enforcement;
- package-load evidence integration, package loading, or execution;
- node, worker, controller, scheduler, P2P, model, inference, installer,
  service, wallet, reward, reputation, marketplace, or public beta wiring.

## Integration Sequence

```text
AGENT-RUNTIME-CORE-001
-> AGENT-PACKAGE-REFERENCE-RESOLVER-001
-> AGENT-PACKAGE-REVIEW-EVIDENCE-001
-> AGENT-RUNTIME-COMPATIBILITY-GATE-001
```

Reading bytes is not review evidence. Later validators may consume a resolved
reference, but only the review-evidence owner may establish provenance and only
the package-load integration owner may consume independent evidence.

## Risks

- Following a symlink could expose data outside the intended package.
- A hard link could alias data not owned by the package tree.
- Reading from path names after validation could reopen a changed target.
- Unbounded reads could exhaust memory or stall QA hosts.
- Storing raw host errors could leak usernames or private paths.
- Treating resolved bytes as trusted would bypass independent gates.

## Success Criteria

- Capability-relative access is the only package read path.
- Traversal, cross-platform absolute forms, symlinks, hard links, directories,
  duplicate paths, oversized files, and oversized totals fail closed.
- Stable files resolve in exact manifest order.
- Errors and Debug output remain privacy-safe.
- Existing package-load behavior stays blocked.
- No node or execution behavior changes.
- Local and field matrices pass on Mac, TS140, and Proxmox/R5500.

## Final Architecture Review

```text
scope ownership: PASS
independent gates preserved: PASS
filesystem containment: PASS
bounded resource use: PASS
privacy-safe errors and Debug: PASS
anti-monolith guards: PASS
local validation: PASS
field QA: PASS
decision: APPROVED FOR MERGE
```

The resolver remains an additive `iamine-agent-runtime` boundary. It returns
untrusted bounded bytes and does not parse them, establish review evidence,
change the package-load blockers, or authorize execution. The source commit
received exact-identity field coverage on macOS and Linux before this
documentation-only closeout.

## Post-Merge Architecture Decision

The integrated resolver passed 12 crate tests and strict crate clippy. The full
quality gate reported failures in four stochastic TinyLlama inference
assertions and one daemon socket test. None of those files changed in this
feature.

Architecture accepted the post-merge result because:

- the exact base `c018e4a25aa054c23f2f5818f0f946eace47922f`
  reproduced `test_real_inference` with the same
  `result.success == false` assertion;
- both base and merge have an empty diff for all `iamine-models` files;
- the exact base reproduced the daemon socket `Operation not permitted` failure
  inside the sandbox;
- the daemon test passed on the merge when executed outside that sandbox;
- repository, architecture, format, diff, network, build, and clippy checks
  passed;
- the resolver passed every local and field assertion without source changes
  after QA.

These are accepted baseline/environment exceptions, not hidden passes. They
remain maintenance inputs outside this feature's ownership.
