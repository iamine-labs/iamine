# IAMINE Local QA Gate

## Feature

```text
PROCESS-QA-LOCAL-GATE-001
```

## Purpose

`scripts/qa-local-gate.sh` is the canonical local entry point before a
development checkpoint or merge review. It validates Git identity and scope,
then delegates the complete validation suite to `scripts/quality-gate.sh`.

The wrapper does not duplicate Rust tests, build commands, Clippy policy, size
guards, artifact checks, or secret checks.

## Default Run

```bash
./scripts/qa-local-gate.sh
```

Default base:

```text
origin/develop
```

## Fast Identity Precheck

```bash
./scripts/qa-local-gate.sh \
  --identity-only \
  --expected-branch <branch-name>
```

Require clean tracked worktree and staging:

```bash
./scripts/qa-local-gate.sh \
  --identity-only \
  --require-clean
```

Use an explicit base:

```bash
./scripts/qa-local-gate.sh \
  --base-ref <ref>
```

Environment equivalents:

```text
IAMINE_QA_BASE_REF
IAMINE_QA_EXPECTED_BRANCH
```

## Contract

The identity check fails when:

- the repository is not a Git worktree;
- the base ref is missing;
- HEAD is detached;
- the active branch is `main`;
- an expected branch does not match;
- the branch does not contain the selected base;
- `origin` is missing or is not the canonical IAMINE repository;
- `origin/main` contains commits absent from the selected base;
- `--require-clean` finds tracked or staged changes.

The report includes full commit and tree IDs. Changed, staged, untracked, and
committed-scope paths are represented by counts and Git object hashes instead
of being printed. This preserves reproducible baselines without exposing local
artifact names.

The full run passes only when the identity check and `quality-gate.sh` pass.

## Scope

This feature changes no Rust code, runtime, scheduler, networking, model policy,
worker behavior, or persistence. Field QA on TS140 and Proxmox is not required.
