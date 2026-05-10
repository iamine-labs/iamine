# Broadcast Flow Architecture

## Purpose

Broadcast mode provides a LAN task lifecycle for simple tasks such as reverse_string. It is the baseline flow for Proxmox LAN validation before Cluster LAN auto discovery.

## Protocol Flow

```text
Controller
  -> TaskOffer on iamine-tasks
Workers
  -> TaskBid on iamine-bids
Controller
  -> TaskAssign on iamine-assign
Assigned worker
  -> TaskResult on iamine-results
Controller
  -> accept/reject result and finalize
```

## PubSub Topics

Broadcast runtime currently joins these topics:

- iamine-tasks
- iamine-bids
- iamine-assign
- iamine-results
- iamine-heartbeat
- iamine-capabilities
- iamine-direct-inference

The topic list is centralized in runtime constants so controller and worker can share the same subscription baseline.

## Controller Responsibilities

The controller:

- prepares one stable broadcast task id per run
- subscribes to required PubSub topics
- waits for iamine-tasks subscriber or mesh readiness
- publishes TaskOffer once readiness is real
- receives TaskBid messages
- selects one winning worker
- publishes TaskAssign
- records assigned_worker in broadcast state
- receives TaskResult
- accepts only the assigned worker result
- marks the scheduler task completed
- cancels recovery after accepted result
- emits final_outcome=success

## Worker Responsibilities

Each worker:

- starts safely in mock/skip mode when configured
- subscribes to required topics
- receives TaskOffer
- checks task support and available slots
- publishes TaskBid
- receives TaskAssign
- executes only if assigned_worker equals local peer id and the task was not already executed
- emits task_completed
- builds TaskResult
- publishes TaskResult to iamine-results

## Single Execution Rule

Only the assigned worker may execute. Non-assigned workers log task_assign_received with will_execute=false and do not execute.

Duplicate TaskAssign for the same task is ignored by the assigned worker after local execution tracking marks the task id as already executed.

## TaskResult Shape

TaskResult includes:

- type=TaskResult
- task_id
- task_type
- worker_id
- worker_peer_id
- origin_peer
- controller_peer_id
- success
- output
- elapsed_ms
- execution_ms
- attempts
- error
- source=broadcast_task_assign

## Result Acceptance Rule

The controller accepts a TaskResult only when:

- task_id matches the active broadcast task
- worker_peer_id equals assigned_worker
- success=true
- no result has already been accepted

Rejected results emit broadcast_result_rejected with one of:

- unknown_task_id
- wrong_worker
- duplicate_result
- task_not_assigned
- success_false

## Recovery Cancellation Rule

After a valid TaskResult is accepted:

- scheduler marks the task completed
- broadcast_result_accepted state is stored
- broadcast_recovery_cancelled is emitted
- final_outcome=success is emitted
- no recovery rebroadcast should happen for that task

## Code Quality Notes

Broadcast logic is currently functional but dense inside iamine-node/src/main.rs. The path spans helpers, runtime select arms, worker TaskAssign execution, and controller TaskResult acceptance. This is acceptable for the closeout baseline but should be extracted before large Cluster LAN work expands the same runtime loop.

Recommended future extraction:

- broadcast_protocol.rs for TaskOffer/TaskBid/TaskAssign/TaskResult builders and acceptance helpers
- broadcast_runtime.rs for controller state machine
- broadcast_worker.rs for worker-side offer/assign/result handling
- pubsub_readiness.rs for reusable topic readiness tracking
