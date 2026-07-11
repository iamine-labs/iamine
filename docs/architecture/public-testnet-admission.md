# PUBLIC-TESTNET-ADMISSION-001

## Objective

Define the controlled public-testnet admission policy required before public
operator onboarding. This is pre-public infrastructure. It must not launch a
public beta or weaken the existing private-testnet admission path.

## Scope

This feature adds a pure policy module:

```text
iamine-network/src/public_testnet_admission.rs
```

The module defines:

- default closed public-testnet admission;
- controlled admission for explicitly admitted public peer IDs;
- removal override for peers that must no longer participate;
- maximum node count per operator;
- required identity-registration and secure-transport controls;
- stable decision reason codes.

## Integration

The policy lives in `iamine-network` because admission semantics are network
domain logic. The feature exports the policy from `iamine-network/src/lib.rs`.

This feature intentionally does not wire the public admission policy into
`iamine-node` runtime startup, CLI flags, scheduler, worker behavior, remote
inference, model policy, reputation, rewards, or marketplace logic.

Future runtime activation must be a separate feature with field QA. That
activation must decide how operator admission records are provisioned, signed,
updated, revoked, audited, and synchronized without storing private host data.

## Admission Rules

Default mode is:

```text
closed
```

Controlled mode admits a candidate only when all required conditions pass:

```text
peer is explicitly admitted
AND peer is not removed
AND operator node count is below policy limit
AND identity registration is present
AND secure transport is authenticated
-> public testnet candidate admitted
```

Removal overrides admission. A peer present in both admitted and removed sets is
rejected with `peer_removed`.

## Privacy

The policy stores only public libp2p peer IDs and bounded counters supplied by
the caller. It must not collect, persist, or log usernames, home directories,
hostnames, MAC addresses, IP addresses, serial numbers, disk UUIDs, machine IDs,
process lists, personal paths, private keys, wallet keys, tokens, credentials,
or permanent hardware fingerprints.

## Non-Goals

This feature does not:

- launch public testnet;
- define public signup UX;
- define signed update distribution;
- add reputation, rewards, wallet, or economic admission;
- add public documentation;
- change private-testnet allowlist behavior;
- change runtime P2P behavior;
- make scheduler or model eligibility decisions.

## Risks

- Treating `controlled` policy availability as public launch readiness would be
  incorrect. Public beta remains blocked by the rest of v0.10.
- Runtime activation without signed/revocable admission records could create an
  abuse-control gap.
- Persisting operator contact or host metadata in admission records would
  violate IAMINE privacy policy.
