# IAMINE-PREPUBLIC-READINESS-GATE-001

## Objective

Decide whether IAMINE has closed the v0.10 pre-public infrastructure package
well enough to proceed into v0.11 Agent Network foundations.

This is an Architecture and QA release-gate feature. It must not launch public
beta, open public onboarding, publish release artifacts, start public support
intake, change runtime behavior, or authorize agent execution.

## Scope

This feature adds:

```text
docs/architecture/iamine-prepublic-readiness-gate.md
docs/qa/iamine-prepublic-readiness-gate.md
docs/roadmap/v0.10-prepublic-readiness-gate.md
```

It also updates the product roadmap state for the active gate.

The implementation is documentation-only. It intentionally does not modify:

- Rust source;
- tests;
- packaging scripts;
- release scripts;
- service definitions;
- `iamine-node` runtime startup;
- P2P, PubSub, worker, scheduler, model, inference, reputation, reward, wallet,
  installer, updater, rollback, or service-manager behavior.

## Covered v0.10 Dependencies

The gate consumes already-closed v0.10 feature evidence:

| Feature | Required state | Gate responsibility |
| --- | --- | --- |
| PUBLIC-TESTNET-ADMISSION-001 | CLOSED | Public admission remains controlled, revocable, identity-bound, transport-bound, and not wired as public launch. |
| SIGNED-AUTOUPDATE-001 | CLOSED | Update eligibility is authenticated, rollout-bounded, rollback-aware, and disabled by default. |
| USER-DIAGNOSTICS-SUPPORT-001 | CLOSED | Support evidence remains privacy-safe, diagnostic-only, and does not start runtime services. |
| V1-SUPPLY-CHAIN-SECURITY-001 | CLOSED | Release evidence is fail-closed, provenance-bound, and separate from publishing. |
| NODE-UPGRADE-ROLLBACK-001 | CLOSED | Rollback eligibility is fail-closed, drained-task-aware, artifact-bound, and not an executor. |
| PUBLIC-TESTNET-DOCUMENTATION-001 | CLOSED | Public documentation states pre-public status and does not publish onboarding, bootnodes, rewards, or install paths. |

## Readiness Layers

The gate has three separate decisions:

```text
v0.10 repository pre-public infrastructure closure
AND no public beta launch claim
AND no runtime activation side effect
-> v0.10 repository gate may close
```

```text
v0.10 repository gate closed
AND official roadmap still targets IAMINE Agent Network Public Beta at v1.0
AND v0.11 features remain PROPOSED and scope-bound
-> v0.11 Agent Network foundations may begin
```

```text
public onboarding
OR public release publication
OR public bootnodes
OR public reward eligibility
OR public support intake
-> still blocked
```

Closing this gate authorizes only the next roadmap step:

```text
AGENT-MARKET-FIT-RESEARCH-001
```

It does not authorize public beta.

## Go Conditions

The gate may recommend moving into v0.11 Agent Network foundations only when:

- every v0.10 dependency is `CLOSED` in `develop`;
- each dependency has merge and post-merge validation evidence;
- public documentation continues to state pre-public status;
- README has no stale public-testnet launch or public reward claims;
- v1.0 remains `IAMINE Agent Network Public Beta`;
- v0.11 remains proposed and does not imply already-authorized agent runtime;
- no source code or runtime behavior changes are introduced by this gate;
- local QA validates roadmap state, privacy boundaries, and no-launch language;
- environmental warnings are explicitly classified and not hidden.

## No-Go Conditions

Do not close this gate if any of these are true:

- a v0.10 dependency is not `CLOSED`;
- public onboarding is described as open;
- public bootnodes, public release packages, public reward eligibility, or
  public support channels are published by this feature;
- v1.0 is described as an inference-only public beta;
- release, update, rollback, support, or admission policy is treated as active
  runtime execution without a separate feature;
- new runtime, scheduler, worker, P2P, inference, model, installer, updater, or
  service-manager behavior appears in the diff;
- QA requires uncommitted source changes to pass;
- repository evidence includes private host identifiers, local paths,
  credentials, secrets, or unresolved public operator data.

## Field QA Decision

This gate is documentation-only and does not require new Proxmox/R5500 field QA
by itself. It consumes prior field and post-merge evidence from the features it
closes over.

Future features that activate public onboarding, release packages, updater
execution, rollback execution, public bootnodes, runtime public admission, or
agent execution must run their own field QA. Proxmox/R5500 availability remains
relevant for those later runtime or operational gates.

## Decision

Architecture recommendation for this feature:

```text
READY TO PROCEED TO v0.11 AGENT NETWORK FOUNDATIONS
NOT READY FOR PUBLIC BETA
NOT READY FOR PUBLIC ONBOARDING
```

This recommendation is valid only if QA confirms the scoped documentation
checks and the controlled merge/post-merge validation remain clean.
