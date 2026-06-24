# Legacy Backend Worker-Daemon E2E

## Feature

```text
LEGACY-BACKEND-WORKER-DAEMON-E2E-001
```

## Goal

Close the remaining field regression from `LEGACY-BACKEND-REAL-INFERENCE-001`
with a repeatable Mac client to Proxmox worker to legacy CPU daemon inference
check.

The harness changes no runtime, scheduler, discovery, protocol, or model
eligibility behavior.

## Harness

```bash
./scripts/qa-legacy-cpu-daemon-e2e.sh
```

Defaults:

```text
remote SSH alias: iamine-heavy
remote repository: ~/code/iamine
worker port: 4103
model: tinyllama-1b
max tokens: 8
client timeout: 240 seconds
```

Useful overrides:

```bash
IAMINE_QA_EXPECTED_REMOTE_HEAD=<full-sha> \
IAMINE_QA_OUTPUT_DIR=/tmp/iamine-legacy-e2e-manual \
./scripts/qa-legacy-cpu-daemon-e2e.sh
```

Run only the identity and environment preflight:

```bash
IAMINE_QA_PREFLIGHT_ONLY=1 \
./scripts/qa-legacy-cpu-daemon-e2e.sh
```

## Safety

- Uses configured SSH aliases and never stores IP addresses or credentials.
- Requires clean tracked and staged state locally and remotely.
- Records remote branch, commit, tree, canonical origin, and hashes of
  untracked files.
- Stores the configured repository alias instead of the resolved home path.
- Requires a legacy x86 host without AVX2.
- Requires the prebuilt standard worker and dedicated legacy daemon binaries.
- Uses a unique `/tmp/iamine-legacy-e2e-<run-id>` evidence directory.
- Uses a unique daemon socket inside that directory.
- Refuses to use an occupied worker port.
- Records QA-owned PIDs and verifies `/proc/<pid>/exe` before sending `TERM`.
- Never kills a process whose executable does not match the recorded binary.
- Preserves local and remote evidence after cleanup.

## Required Evidence

The run passes only when all of these are present:

```text
worker_startup_ready:
  backend=real
  legacy_cpu_real_backend_mode=daemon_only
  real_inference_available=true
worker_model_load_attempt during startup: absent
task_message_received: present
task_completed success=true: present
result_published: present
client result_received: present
client final_outcome_success: present
client human completion marker: present
daemon real inference execution marker: present
SIGILL / illegal instruction: absent
```

## Field Result

Validated on the Proxmox/R5500 heavy guest:

```text
branch: feature/legacy-backend-worker-daemon-e2e-001
commit: 06b0908ab78c338634b46c71946e4c3ea2621f73
tree: 7518d2252e4fd7a58deba6f0fe7e24eabbb2b032
base: 4b823388f37e695e9d1db11528f40af714d708c1
```

The first E2E attempt exposed a product regression: the worker awaited the
complete inference while holding the mutable `Swarm`, so progress messages were
queued locally but were not delivered before the client watchdog expired.

The fix moves inference work behind a bounded worker event channel. The main
runtime loop continues polling the network and publishes progress, streamed
tokens, and the final result as events arrive. Scheduler timeouts and message
formats were not changed.

Final result:

```text
preflight: PASS
remote progress received: PASS
first attempt completed: PASS
retries: 0
model: tinyllama-1b
tokens generated: 53
daemon execution: 35026 ms
client final outcome: success
task_completed success=true: present
result_published: present
result_received: present
final_outcome_success: present
SIGILL / illegal instruction: absent
worker cleanup: PASS
daemon cleanup: PASS
worker port released: PASS
```

No model was downloaded and no scheduler, discovery, model eligibility, or
wire-format policy was changed.
