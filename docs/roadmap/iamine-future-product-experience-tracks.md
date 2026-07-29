# IAMINE Future Product Experience Tracks

## State

```text
portfolio: IAMINE-FUTURE-PRODUCT-EXPERIENCE-TRACKS
state: PROPOSED / DEFERRED BY GROUP
milestone placement: unresolved
implementation authorization: none
```

This portfolio preserves accepted long-term product direction without turning
every candidate into an active roadmap row. It does not renumber existing
milestones, change the v1.0 IAMINE Agent Network Public Beta definition, or
interrupt the sequential v0.11.2 runtime track.

Architecture-only work may be promoted independently when it has a bounded
owner and does not touch runtime behavior. Executable product work remains
deferred until its dependencies and release placement are explicitly approved.

## Agent Installation and Shared Models

```text
group state: PROPOSED
activation: after the v0.11.2 runtime milestone and before dependent product flows
```

Candidate features:

```text
AGENT-LIGHTWEIGHT-PACKAGE-ARCHITECTURE-001
AGENT-MODEL-REQUIREMENTS-CONTRACT-001
AGENT-MODEL-CAPABILITY-MATCHING-001
AGENT-SHARED-MODEL-POOL-001
AGENT-MODEL-BACKEND-ABSTRACTION-001
AGENT-LOCAL-LAN-REMOTE-FALLBACK-POLICY-001
AGENT-MODEL-RESOURCE-NEGOTIATION-001
AGENT-ONE-CLICK-INSTALL-001
AGENT-DEPENDENCY-RESOLUTION-001
AGENT-INSTALL-PERMISSION-REVIEW-001
AGENT-UPDATE-COMPATIBILITY-001
AGENT-UNINSTALL-ROLLBACK-001
AGENT-INSTALLATION-E2E-001
```

`AGENT-MODEL-REQUIREMENTS-CONTRACT-001` must evolve the existing
`AGENT-RESOURCE-REQUIREMENTS-001` `model_dependencies` contract. It must not
create a second incompatible metadata authority. Model declarations do not
authorize download, load, selection, admission, placement, or remote use and
must continue through registration, integrity, license, hardware, backend,
network, permission, and runtime gates.

`AGENT-PACKAGE-LOADER-001` remains limited to loading an already eligible
package through bounded package ownership. This future group does not add model
selection, model download, or agent execution to that feature.

## Desktop Application Surfaces

```text
group state: DEFERRED
activation: stable shared contracts, Local Control API, authorization, audit, and service lifecycle
```

```text
IAMINE-DESKTOP-APPLICATION-SHELL-001
IAMINE-SYSTEM-TRAY-001
IAMINE-GLOBAL-QUICK-ACCESS-001
IAMINE-AGENT-LAUNCHER-001
IAMINE-AGENT-WORKSPACE-001
IAMINE-MULTIWINDOW-001
IAMINE-LOCAL-NOTIFICATIONS-001
IAMINE-DEEP-LINKING-001
IAMINE-DESKTOP-E2E-001
```

The Dashboard remains the control center. Desktop surfaces may later add
bounded quick access, workspaces, tray behavior, and local notifications. They
are not yet mandatory v1.0 scope.

## Personal Agent Experience

```text
track: IAMINE-PERSONAL-AGENT-ECOSYSTEM-TRACK
group state: DEFERRED
activation: functional agents, installer contracts, desktop foundation, permission enforcement, and audit
```

Architecture candidates:

```text
IAMINE-PERSONAL-AGENT-ECOSYSTEM-ARCHITECTURE-001
IAMINE-MULTI-SURFACE-UX-CONTRACT-001
AGENT-EXPERIENCE-METADATA-001
AGENT-INTERACTION-MODE-CONTRACT-001
IAMINE-UX-GATEWAY-001
IAMINE-DEVICE-PERMISSION-BROKER-001
```

`IAMINE-AGENT-EXPERIENCE-ARCHITECTURE-001` is consolidated into
`IAMINE-PERSONAL-AGENT-ECOSYSTEM-ARCHITECTURE-001` to avoid two competing
architecture authorities.

Presence candidates:

```text
IAMINE-AGENT-PRESENCE-MODES-001
IAMINE-NOTIFICATION-POLICY-001
IAMINE-DO-NOT-DISTURB-001
IAMINE-PROACTIVE-ACTION-CONSENT-001
IAMINE-NOTIFICATION-BUDGET-001
IAMINE-BACKGROUND-TASK-VISIBILITY-001
```

Agents are on-demand and silent by default. Proactive behavior requires
explicit, specific, visible, and revocable consent.

## Device Continuity, Mobile, and OS Integration

```text
group state: DEFERRED / POST-BETA
activation: client-device identity, trust, encrypted transport, per-device permissions, revocation, and minimal handoff
```

Device continuity candidates:

```text
IAMINE-CLIENT-DEVICE-IDENTITY-001
IAMINE-DEVICE-TRUST-001
IAMINE-PER-DEVICE-PERMISSIONS-001
IAMINE-CROSS-DEVICE-SESSION-001
IAMINE-CONTEXT-HANDOFF-001
IAMINE-AGENT-STATE-SYNC-001
IAMINE-MINIMAL-CONTEXT-TRANSFER-001
IAMINE-DEVICE-REVOCATION-001
IAMINE-CROSS-DEVICE-E2E-001
```

`IAMINE-CLIENT-DEVICE-IDENTITY-001` is intentionally distinct from the closed
network-node identity and registration features.

Mobile candidates:

```text
IAMINE-MOBILE-COMPANION-001
IAMINE-MOBILE-NODE-CONNECTION-001
IAMINE-MOBILE-VOICE-INTERACTION-001
IAMINE-MOBILE-CAMERA-HANDOFF-001
IAMINE-MOBILE-NOTIFICATIONS-001
IAMINE-MOBILE-WIDGETS-001
IAMINE-MOBILE-PERMISSION-APPROVAL-001
IAMINE-MOBILE-CROSS-DEVICE-CONTINUITY-001
```

OS integration candidates:

```text
IAMINE-OS-SHARE-INTEGRATION-001
IAMINE-OS-CONTEXT-MENU-001
IAMINE-OS-INTENTS-INTEGRATION-001
IAMINE-OS-SEARCH-INTEGRATION-001
IAMINE-OS-FILE-ACTION-INTEGRATION-001
IAMINE-BROWSER-EXTENSION-001
IAMINE-VOICE-SHORTCUT-INTEGRATION-001
```

These surfaces require visible invocation, explicit permissions, audit,
reversible configuration, and no hidden monitoring or unrestricted capture.
Cross-device behavior transfers the minimum authorized context, never all
personal memory by default.

## Personal Memory and Companion

```text
group state: DEFERRED / POST-BETA
activation: privacy architecture, encryption, consent, retention, export/delete, scoping, and audit
```

```text
IAMINE-PERSONAL-MEMORY-ARCHITECTURE-001
IAMINE-MEMORY-PRIVACY-BOUNDARY-001
IAMINE-MEMORY-RETENTION-POLICY-001
IAMINE-MEMORY-CONSENT-001
IAMINE-MEMORY-EXPORT-DELETE-001
IAMINE-MEMORY-PER-AGENT-SCOPING-001
IAMINE-MEMORY-PER-DEVICE-SCOPING-001
IAMINE-MEMORY-SYNC-POLICY-001
IAMINE-COMPANION-AGENT-ARCHITECTURE-001
IAMINE-PERSONAL-ORCHESTRATOR-001
IAMINE-COMPANION-AGENT-001
```

The companion and orchestrator do not receive universal scope, permission,
memory, or device access. Specialized agents retain independent scope,
permissions, memory policy, and audit.

## Family and Education

```text
track: IAMINE-FAMILY-EDUCATION-ECOSYSTEM-TRACK
group state: DEFERRED / MATURE PRODUCT PHASE
activation: profiles, guardian consent, child safety, age-appropriate UX, bounded memory, notification policy, and independent safety QA
```

Education candidates:

```text
IAMINE-EDUCATION-AGENT-ARCHITECTURE-001
IAMINE-LEARNER-PROFILE-001
IAMINE-CURRICULUM-MAPPING-001
IAMINE-LEARNING-GOAL-CONTRACT-001
IAMINE-ADAPTIVE-LEARNING-PLAN-001
IAMINE-LEARNING-SESSION-001
IAMINE-EXERCISE-GENERATOR-001
IAMINE-HINT-FIRST-POLICY-001
IAMINE-STEP-BY-STEP-EXPLANATION-001
IAMINE-MASTERY-ASSESSMENT-001
IAMINE-LEARNING-PROGRESS-EVIDENCE-001
IAMINE-EDUCATION-AGENT-001
```

Academic-integrity candidates:

```text
IAMINE-ACADEMIC-INTEGRITY-MODE-001
IAMINE-ATTEMPT-FIRST-POLICY-001
IAMINE-SOLUTION-DELAY-POLICY-001
IAMINE-HINT-LADDER-001
IAMINE-ANSWER-PROVENANCE-EVIDENCE-001
IAMINE-LEARNING-INTEGRITY-ASSURANCE-001
```

Family and child-safety candidates:

```text
IAMINE-FAMILY-PROFILE-001
IAMINE-GUARDIAN-CONSENT-001
IAMINE-CHILD-SAFETY-POLICY-001
IAMINE-AGE-APPROPRIATE-UX-001
IAMINE-CHILD-DATA-MINIMIZATION-001
IAMINE-PARENTAL-CONTROLS-001
IAMINE-CHILD-MEMORY-BOUNDARY-001
IAMINE-CHILD-NOTIFICATION-POLICY-001
IAMINE-CHILD-CROSS-DEVICE-CONTROLS-001
IAMINE-GUARDIAN-PROGRESS-SUMMARY-001
IAMINE-CHILD-SAFETY-MILESTONE-QA-001
```

Academic integrity uses attempt-first behavior, hints, delayed solutions,
follow-up exercises, and explicit uncertainty. Hidden screen, camera,
microphone, keyboard, message, or application surveillance is blocked.

Child-facing work is not part of the first public beta. It requires explicit
guardian authority, data minimization, age-appropriate interaction, bounded
memory, no manipulative engagement, and a dedicated independent safety gate.
IAMINE must not present these agents as replacements for parents, teachers,
medical professionals, mental-health professionals, or emergency services.
