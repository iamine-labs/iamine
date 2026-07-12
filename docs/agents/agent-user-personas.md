# IAMINE Agent User Personas

Feature:

```text
AGENT-USER-PERSONA-MAPPING-001
```

## Purpose

Map the agent market-fit research segments into explicit user personas and task
contexts for later beta-pack selection.

This document is a repository research artifact. It does not claim completed
external user research, select the final official beta agent pack, authorize
agent runtime, or open public beta.

## Persona Mapping Rules

Every persona must identify:

- repeated user problem;
- likely task context;
- safe first-agent value;
- data the agent may inspect;
- data the agent must not collect;
- permissions that may be needed later;
- blocked actions;
- handoff or refusal triggers;
- measurable beta signal.

All persona-derived agents remain subject to the scope-bound agent rule.

## Personas

| Persona | Repeated problem | Safe first value |
| --- | --- | --- |
| Home Troubleshooter | A PC, storage, Wi-Fi, or local app behaves strangely and the user does not know where to start. | Explain likely causes and produce a safe next-action checklist. |
| Non-Technical Caretaker | The user supports a household device but avoids technical commands. | Translate diagnostics into plain language and ask before any risky step. |
| Homelab Operator | The user runs local services, VMs, or Proxmox and needs quick read-only inspection. | Summarize health, ports, resources, and obvious misconfiguration signals. |
| Self-Hosted Maintainer | The user manages local apps and wants privacy-safe status reports. | Produce a local-only report with redacted paths and no credentials. |
| Content Organizer | The user has many media files, drafts, or publishing ideas. | Classify, summarize, and propose organization without modifying files by default. |
| Small Office Helper | A small team needs a shareable support summary for devices or network symptoms. | Produce a privacy-safe report and clarify what a human should inspect. |
| Multi-PC Household Operator | The user has several PCs and wants to compare readiness or problems. | Compare declared capabilities without committing host identifiers. |
| Future Agent Builder | The user wants to create an IAMINE agent safely. | Explain manifest, scope, permission, and boundary-test requirements. |

## Persona Details

### Home Troubleshooter

Task context:

- local PC feels slow;
- storage is low;
- a local model or IAMINE node is not ready;
- Wi-Fi or LAN behavior is confusing.

Safe first-agent value:

- read diagnostic summaries;
- explain likely causes;
- suggest non-destructive next steps;
- hand off to a human before risky repair.

Blocked actions:

- deleting files;
- changing router settings;
- restarting services;
- executing shell commands without a future permission model.

Beta signal:

- user can understand the issue and pick a safe next step in under five minutes.

### Non-Technical Caretaker

Task context:

- helping a family member with a device;
- interpreting warning messages;
- deciding whether a device is safe to use for IAMINE.

Safe first-agent value:

- plain-language explanation;
- risk labels;
- confirmation prompts;
- no hidden automation.

Blocked actions:

- destructive cleanup;
- credential handling;
- system modification;
- broad filesystem access.

Beta signal:

- user reports lower confusion without giving the agent broad permissions.

### Homelab Operator

Task context:

- checking local services;
- reviewing Proxmox, Docker, or LAN symptoms;
- understanding resource pressure.

Safe first-agent value:

- read-only health summary;
- bounded resource overview;
- port and service-status explanation;
- suggested follow-up commands for the operator to run manually.

Blocked actions:

- VM mutation;
- container restart;
- firewall changes;
- unrestricted network scanning;
- credential extraction.

Beta signal:

- operator can identify the next manual action without the agent mutating
  infrastructure.

### Self-Hosted Maintainer

Task context:

- local app unhealthy;
- logs need summarization;
- ports or disk use need explanation.

Safe first-agent value:

- summarize local-only evidence;
- redact paths and identifiers;
- distinguish configuration issue from resource issue.

Blocked actions:

- uploading raw logs;
- exposing host identifiers;
- editing configs;
- restarting services.

Beta signal:

- support report is useful without leaking local identifiers.

### Content Organizer

Task context:

- many photos, files, drafts, or content ideas need structure;
- user wants suggestions before allowing writes.

Safe first-agent value:

- classify file names or user-provided metadata;
- propose organization plans;
- draft calendars or content outlines.

Blocked actions:

- moving, deleting, or renaming files by default;
- reading private folders without explicit future permissions;
- publishing content automatically.

Beta signal:

- user accepts suggested structure before any write-capable version exists.

### Small Office Helper

Task context:

- office PC or network issue needs a shareable report;
- non-technical staff need clear language.

Safe first-agent value:

- privacy-safe diagnostic report;
- role-separated summary for user and technician;
- clear blocked actions.

Blocked actions:

- collecting employee identifiers;
- collecting credentials;
- changing network or device configuration;
- acting as production support without human review.

Beta signal:

- report reduces back-and-forth while avoiding private data.

### Multi-PC Household Operator

Task context:

- several PCs may join IAMINE;
- user wants readiness comparison.

Safe first-agent value:

- compare capability classes and readiness summaries;
- identify missing prerequisites;
- avoid permanent hardware fingerprints.

Blocked actions:

- storing hostnames;
- storing MAC or IP addresses;
- collecting serials or machine IDs;
- starting remote work without operator approval.

Beta signal:

- user can choose which PC to configure next without exposing host identity.

### Future Agent Builder

Task context:

- developer wants to understand how to package a safe IAMINE agent;
- reviewer needs to assess scope and permissions.

Safe first-agent value:

- explain required manifest fields;
- generate a checklist for scope, permissions, blocked actions, handoffs, and
  tests;
- refuse generic do-anything scope.

Blocked actions:

- auto-publication;
- bypassing manual review;
- granting broad permissions;
- publishing untested agents.

Beta signal:

- developer produces a narrow, reviewable proposal instead of a broad agent.

## Persona To Future-Agent Mapping

| Persona | Candidate future agent families | Earliest safe mode |
| --- | --- | --- |
| Home Troubleshooter | Node Doctor, Home Network Assistant, OS Diagnostic Assistant | local-only read-only |
| Non-Technical Caretaker | Reporter, Guided Troubleshooter | local-only read-only |
| Homelab Operator | Homelab Doctor, Proxmox Readonly, Docker Readonly | LAN-only read-only |
| Self-Hosted Maintainer | Reporter, Service Status Inspector | local-only read-only |
| Content Organizer | Photo Library Organizer, Content Calendar | local-only read-only |
| Small Office Helper | Reporter, Network Summary Assistant | local/LAN read-only |
| Multi-PC Household Operator | Node Readiness Comparator, LAN Status Reporter | LAN read-only |
| Future Agent Builder | Manifest Wizard, Permission Review, Scope Review | local-only planning |

This mapping is not beta-pack selection. `AGENT-BETA-PACK-SELECTION-001` must
choose the first pack after personas, constraints, and test boundaries are
reviewed.

## Research Gaps

Before beta-pack selection, IAMINE still needs:

- interview or survey prompts;
- task samples per persona;
- positive and negative boundary examples;
- privacy redaction examples;
- refusal examples;
- measurable success criteria per candidate agent.

## Recommendation

Proceed to:

```text
AGENT-BETA-PACK-SELECTION-001
```

Only after this persona mapping is reviewed as a research input, not as
validated external market evidence.
