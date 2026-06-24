# Legacy Backend Real Inference Closeout

## Feature

```text
LEGACY-BACKEND-REAL-INFERENCE-001
```

## Validated Fix

```text
branch: feature/legacy-backend-real-inference-001-fix
commit: bd0ebee45c648244387c52be507ebbd07df5fb7a
tree: c2b1936bce1db911fcb7efa705d6525c3d0ffbe7
base: 0df03f8f519095bdb11a6b1dcfe56746fe604ee8
```

## Field Matrix

- Mac: local tests, build, quality gate, and CLI smokes.
- TS140 with AVX2: mock/skip compatibility smoke.
- Proxmox/R5500 guests without AVX2: worker daemon-only startup on controller,
  worker 1, worker 2, and heavy.
- Proxmox/R5500 heavy: standard daemon guard, dedicated legacy daemon build,
  Ping/Pong, and real TinyLlama inference.

## Failure Found

The first real daemon inference on the standard build reproduced the historical
failure:

```text
daemon exit: 132
failure: SIGILL / illegal instruction
```

The llama.cpp CMake defaults enabled AVX, AVX2, FMA, F16C, and BMI2 even when
the Rust target did not request native CPU tuning.

## Fix

- `scripts/build-legacy-cpu-daemon.sh` builds into `target/legacy-cpu`.
- `cmake/iamine-legacy-cpu.cmake` forces the x86_64 baseline instruction set.
- The standard daemon binary rejects legacy x86 startup before backend
  initialization.
- The worker still never creates or falls back to a local real backend when
  daemon-only mode is active.

## Real Inference Evidence

```text
model: tinyllama-1b
daemon socket ready: yes
result success: true
tokens generated: 32
execution time: 101018 ms
accelerator: CPU
daemon exit: 0
SIGILL: no
```

No model was downloaded during QA.

## Combined Network Follow-Up

The combined Mac client to worker to daemon smoke is closed by
`LEGACY-BACKEND-WORKER-DAEMON-E2E-001`.

That follow-up found and fixed a blocked `Swarm` polling path during worker
inference, then validated real TinyLlama inference with live progress, no
retries, successful result delivery, and no SIGILL. See
`docs/qa/legacy-backend-worker-daemon-e2e.md`.

## Decision

The feature is safe to close with the combined network smoke retained as a
non-blocking regression check for the next inference-related field QA cycle.
