# IAMINE Agent Market Fit Research

Feature:

```text
AGENT-MARKET-FIT-RESEARCH-001
```

## Purpose

Identify the first IAMINE agent use cases that are likely to be useful,
understandable, safe, and testable for beta users.

This document is a research baseline. It does not claim completed user
validation, select the final beta agent pack, authorize agent runtime, or open
public beta.

## Research Boundary

The research must answer:

- which user segments have repeated local or LAN problems IAMINE can help with;
- which tasks are narrow enough for scope-bound agents;
- which tasks can be tested without destructive permissions;
- which tasks create trust quickly for non-technical users;
- which tasks can run local-only or LAN-only before public agent execution;
- which tasks should be blocked because they require unsafe permissions,
  credentials, public infrastructure mutation, or unreliable automation.

## Candidate Segments

| Segment | Candidate need | Safety posture |
| --- | --- | --- |
| Home users | Understand PC, network, storage, or photo-library issues. | Prefer read-only diagnostics and guided next steps. |
| Non-technical users | Explain problems in plain language and avoid risky commands. | Require confirmations and no destructive defaults. |
| Homelabs | Inspect LAN, Proxmox, Docker, and self-hosted services. | Read-only first; no service restarts or mutations. |
| Self-hosted users | Diagnose local services, ports, storage, and logs. | Local-only or LAN-only with redaction. |
| Content creators | Organize media and draft structured publishing plans. | File read-only before write actions exist. |
| Small businesses | Summarize device health, network issues, and support reports. | Privacy-safe reports and clear permission displays. |
| Multi-PC users | Compare readiness across several household or office PCs. | No host identifiers in committed evidence. |
| Future agent developers | Understand package, scope, permission, and test requirements. | Manual validation; no auto-publication. |

## Initial Hypotheses

These are hypotheses, not validated claims:

- users will trust agents faster when the first output is a diagnosis or report,
  not an automatic fix;
- read-only local diagnostics are the safest first wedge for beta adoption;
- non-technical users need plain-language explanations and explicit next
  actions more than hidden automation;
- homelab users may accept more technical output if the agent remains read-only
  and scope-bound;
- content and file agents can create value, but write actions must wait for a
  stronger permission model;
- public beta should not launch until scope, permissions, audit logs, and
  negative boundary tests exist.

## Research Questions

Each candidate agent should be evaluated against:

- What user problem is repeated and painful?
- Can the agent solve or explain the problem without destructive actions?
- What data must the agent read?
- What data must the agent never collect?
- Does the task require internet access, LAN access, or local-only execution?
- What permissions are required?
- What actions are blocked?
- What would a safe refusal look like?
- What should be handed off to the orchestrator or a human?
- How can success be measured in a beta?

## Evaluation Criteria

Candidate agents should score well on:

- clear user pain;
- narrow scope;
- read-only or low-risk first version;
- privacy-safe inputs and outputs;
- testable positive cases;
- testable negative cases;
- visible user value in under five minutes;
- compatibility with constrained hosts;
- local-only or LAN-only execution path;
- low dependency on external services;
- no need for credentials in v0.11 research.

## Exclusion Criteria

Do not select a candidate for early beta if it requires:

- unrestricted filesystem access;
- arbitrary shell execution;
- credential collection;
- service restarts by default;
- destructive file writes by default;
- public infrastructure mutation;
- payment, wallet, reward, or settlement logic;
- open marketplace behavior;
- third-party publication without review;
- solving tasks outside its declared scope.

## Evidence Plan

Research should progress through these evidence levels:

```text
repository hypothesis
-> structured interview or survey prompt
-> privacy-safe sample tasks
-> prototype scope manifest
-> positive and negative boundary tests
-> beta-pack selection review
```

This feature closes only the repository hypothesis and evaluation baseline.
Later features must record real user research, persona mapping, and beta-pack
selection evidence separately.

## Recommended Next Step

The next feature should map the candidate segments into explicit personas:

```text
AGENT-USER-PERSONA-MAPPING-001
```
