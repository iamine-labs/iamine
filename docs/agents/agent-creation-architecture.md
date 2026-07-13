# IAMINE Agent Creation Architecture

Feature:

```text
AGENT-CREATION-ARCHITECTURE-001
```

## Purpose

Define how IAMINE agents move from product idea to reviewable package and,
later, to runtime eligibility.

This document is an architecture artifact. It does not authorize executable
agents, runtime execution, sandboxing, permission enforcement, registry
publication, marketplace publication, third-party agents, or public beta
launch.

## Creation Contract

An IAMINE agent is not a single prompt, script, or unrestricted assistant. It
is a scope-bound package that must pass independent review gates before it can
be considered for execution.

The creation contract requires:

- a validated persona and task fit;
- a package manifest;
- a canonical skeleton layout;
- a scope manifest;
- capability metadata;
- expertise metadata;
- resource requirements;
- permission requirements;
- audit policy;
- boundary evals;
- local registry review;
- runtime eligibility review before execution.

No single file or reviewer may collapse these gates into one approval.

## Pipeline

```text
1. Product fit
2. Package identity
3. Skeleton layout
4. Scope boundary
5. Capability and expertise metadata
6. Resource requirements
7. Permission requirements
8. Audit policy
9. Boundary evals
10. Local registry review
11. Runtime eligibility review
12. Execution lifecycle
```

Only steps 1, 2, and 4 have closed supporting contracts at the time of this
feature. The remaining steps are future roadmap gates.

## Scope-Bound Rule

Every agent must be scope-bound. A specialized agent must only handle tasks
inside its declared scope.

If a request is outside scope, ambiguous, dangerous, cross-domain, or asks for
unapproved permissions, the agent must:

- refuse;
- ask for clarification; or
- hand off to the orchestrator.

It must not silently expand its role.

## Agent Review Inputs

Review starts from package-relative metadata, not local machine state.

Allowed review inputs:

- roadmap feature ID;
- package manifest;
- scope manifest;
- persona mapping;
- selected beta pack membership;
- planned capability metadata;
- planned expertise metadata;
- planned resource requirements;
- planned permission categories;
- planned audit policy;
- planned boundary evals.

Forbidden review inputs:

- credentials;
- private keys;
- wallet keys;
- usernames;
- full hostnames;
- IP addresses;
- MAC addresses;
- serial numbers;
- machine IDs;
- private paths;
- unredacted user logs;
- permanent hardware fingerprints.

## Execution Eligibility

An agent is not executable merely because it has:

- a package ID;
- a display name;
- a scope document;
- a selected persona;
- a beta pack entry;
- a local registry entry;
- a human review note.

Execution eligibility requires later runtime features to verify scope,
permissions, resources, audit policy, boundary evals, sandbox constraints, and
lifecycle behavior.

## Phase Limits

Allowed during v0.11.1 architecture foundation:

- documentation contracts;
- schema planning;
- review responsibilities;
- non-bypass rules;
- local-only planning states;
- beta pack alignment.

Blocked during this feature:

- executable agents;
- agent package parser;
- skeleton generator;
- runtime scheduling;
- permission enforcement;
- sandbox execution;
- arbitrary shell;
- unrestricted filesystem;
- unrestricted network;
- destructive actions;
- service mutation;
- model downloads;
- wallet, reward, settlement, token, or mainnet behavior;
- public marketplace publication;
- third-party public publishing.

## Required Future Evidence

Before any agent can execute, later features must provide evidence for:

- positive in-scope tasks;
- negative out-of-scope tasks;
- ambiguous requests;
- dangerous requests;
- cross-domain requests;
- permission escalation attempts;
- prompt injection attempts;
- role confusion attempts;
- handoff behavior;
- privacy redaction;
- local-only behavior where applicable;
- timeout, cancel, failure, and blocked lifecycle states.

## User-Facing Safety

Agent creation must preserve conservative user expectations:

- no hidden execution;
- no surprise service changes;
- no destructive default behavior;
- no credential collection;
- no private host identity collection;
- no public publishing without later explicit gates;
- no claims that a planning artifact is a working agent.

## Next Roadmap Step

The next architecture feature after this contract is:

```text
AGENT-SKELETON-STANDARD-001
```
