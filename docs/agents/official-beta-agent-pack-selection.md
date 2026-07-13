# IAMINE Official Beta Agent Pack Selection

Feature:

```text
AGENT-BETA-PACK-SELECTION-001
```

## Purpose

Select the first official IAMINE beta agent pack from the repository research
baseline and persona mapping.

This document is a product selection artifact. It does not authorize agent runtime.
It also does not authorize executable manifests, permission enforcement,
sandboxing, audit logs, marketplace publication, third-party agents, or public
beta launch.

## Selection Inputs

This selection consumes:

```text
AGENT-MARKET-FIT-RESEARCH-001
AGENT-USER-PERSONA-MAPPING-001
```

Selection criteria:

- repeated user problem;
- visible value in under five minutes;
- local-only or bounded LAN-only first mode;
- read-only default behavior;
- no credentials;
- no destructive writes;
- no infrastructure mutation;
- narrow scope;
- explicit blocked actions;
- testable positive and negative boundaries;
- compatibility with the future manifest, permission, audit, and boundary-eval
  work.

## Official Beta Pack

The first official beta pack is:

```text
IAMINE Local Readiness Beta Pack
```

It contains four candidate agents:

| Agent | Primary persona | Earliest safe mode | First value |
| --- | --- | --- | --- |
| Node Doctor | Home Troubleshooter | local-only read-only | Explain IAMINE node readiness and safe next steps. |
| Privacy-Safe Support Reporter | Self-Hosted Maintainer | local-only read-only | Produce a redacted support summary from allowed local evidence. |
| LAN Readiness Reporter | Multi-PC Household Operator | LAN read-only | Compare declared node readiness without committing host identifiers. |
| Agent Manifest Wizard | Future Agent Builder | local-only planning | Guide a developer toward a narrow manifest and boundary-test checklist. |

These names are product selections. They are not executable package names until
later manifest and runtime features define the final format.

## Agent Contracts

### Node Doctor

Personas covered:

- Home Troubleshooter;
- Non-Technical Caretaker;
- Multi-PC Household Operator for single-node readiness questions.

Allowed first surface:

- IAMINE node status summaries;
- hardware profile summaries already produced by IAMINE;
- install and readiness checklist output;
- user-provided error text.

Blocked actions:

- deleting files;
- changing settings;
- restarting services;
- loading or downloading models;
- running shell commands;
- collecting host identifiers, serials, MAC addresses, disk UUIDs, or machine
  IDs.

Required later gates:

- scope manifest;
- permission model;
- audit log;
- out-of-scope response policy;
- positive and negative boundary tests.

### Privacy-Safe Support Reporter

Personas covered:

- Self-Hosted Maintainer;
- Small Office Helper;
- Non-Technical Caretaker.

Allowed first surface:

- user-selected diagnostic snippets;
- IAMINE support bundle summaries after redaction rules exist;
- user-provided app or service symptoms;
- local-only report drafts.

Blocked actions:

- uploading raw logs;
- collecting credentials;
- collecting usernames, full hostnames, IP addresses, MAC addresses, serials,
  machine IDs, private paths, or secrets;
- editing configs;
- restarting services;
- contacting third-party support automatically.

Required later gates:

- redaction examples;
- support-report input contract;
- permission model;
- audit log;
- refusal examples for private data.

### LAN Readiness Reporter

Personas covered:

- Multi-PC Household Operator;
- Homelab Operator for IAMINE-only readiness;
- Small Office Helper for read-only IAMINE summaries.

Allowed first surface:

- declared IAMINE node capability summaries;
- IAMINE cluster status output;
- operator-provided node labels that are not host identifiers;
- local/LAN readiness differences.

Blocked actions:

- unrestricted network scanning;
- router changes;
- firewall changes;
- VM or container mutation;
- storing hostnames, IP addresses, MAC addresses, serials, disk UUIDs, or
  machine IDs;
- starting remote work without operator approval.

Required later gates:

- LAN-only permission boundary;
- privacy-safe node aliasing;
- scope boundary evals for cross-node requests;
- audit log;
- handoff policy.

### Agent Manifest Wizard

Personas covered:

- Future Agent Builder.

Allowed first surface:

- user-provided agent idea;
- repository documentation;
- manifest checklist once manifest schema exists;
- blocked-action and test-boundary prompts.

Blocked actions:

- generating do-anything scopes;
- auto-publishing agents;
- granting permissions;
- bypassing manual review;
- approving unsafe or untested agents.

Required later gates:

- package manifest schema;
- scope manifest schema;
- permission review checklist;
- boundary-test generator;
- registry review workflow.

## Deferred Candidates

The following candidates are intentionally not selected for the first beta pack:

| Candidate | Reason deferred |
| --- | --- |
| Photo Library Organizer | Needs stronger filesystem read/write permissions and private-folder boundaries. |
| Content Calendar | Useful, but less tied to IAMINE readiness and harder to validate as core beta value. |
| Proxmox Readonly | Requires careful credential and infrastructure-scope policy before beta. |
| Docker Readonly | Requires container boundary policy and service mutation refusal tests. |
| Home Network Assistant | Router and network-change requests create high refusal pressure. |
| OS Diagnostic Assistant | Broad OS scope must wait for more precise manifests and permission boundaries. |
| General Guided Troubleshooter | Too broad unless specialized under Node Doctor or Support Reporter. |

## Beta Pack Boundary

The selected pack is a planning contract only. It does not create:

- executable agents;
- package manifests;
- scope manifests;
- permission prompts;
- sandbox behavior;
- audit logs;
- runtime states;
- registry entries;
- marketplace entries;
- third-party publication;
- public beta access.

Before any selected agent can execute, the later v0.11 architecture and runtime
features must define scope, permissions, audit logs, boundary evals, runtime
lifecycle, input/output contracts, cancellation, handoff, and out-of-scope
responses.

## Acceptance Signals

The pack is ready to feed manifest work when each selected agent has:

- one narrow task statement;
- explicit in-scope inputs;
- explicit forbidden data;
- explicit blocked actions;
- refusal examples;
- handoff examples;
- positive tests;
- negative boundary tests;
- prompt-injection tests;
- role-confusion tests;
- a measurable beta success signal.

## Recommendation

Proceed to:

```text
AGENT-PACKAGE-MANIFEST-001
AGENT-SCOPE-MANIFEST-001
AGENT-CAPABILITY-METADATA-001
AGENT-RESOURCE-REQUIREMENTS-001
```

Do not implement runtime execution before manifests, permissions, audit logs,
and scope boundary evals are defined.
