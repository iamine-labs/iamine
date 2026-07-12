# PUBLIC-TESTNET-DOCUMENTATION-001

## Objective

Provide a minimum public-testnet documentation baseline without launching public
beta, opening public onboarding, publishing release artifacts, or changing
runtime behavior.

## Scope

This feature adds public-facing documentation:

```text
docs/public-testnet/README.md
```

It also updates:

- repository README public-testnet status;
- roadmap state for the current v0.10 pre-public feature;
- QA evidence for this documentation baseline.

## Documentation Contract

The public documentation baseline must:

- state that IAMINE is pre-public;
- state that no public beta is launched by the document;
- identify required controls before public onboarding;
- preserve the official roadmap rule that v1.0 is the IAMINE Agent Network
  Public Beta, not an inference-only public beta;
- avoid publishing public bootnodes, release package instructions, support
  intake channels, admission records, or reward claims;
- explain privacy boundaries for prospective operators;
- point future activation toward Architecture and QA readiness gates.

## Integration

This feature is documentation-only. It intentionally does not modify:

- `iamine-node` runtime startup;
- P2P, PubSub, worker, scheduler, model, inference, reputation, reward, wallet,
  installer, updater, rollback, or service-manager behavior;
- private-testnet admission behavior;
- public-testnet admission policy code;
- packaging scripts or release artifacts.

Runtime or installer activation remains a separate feature with field QA.

## Public Readiness Boundary

Public onboarding is blocked until all required conditions are explicitly
available and current:

```text
public-testnet admission is controlled and revocable
AND node identity is operator-controlled
AND secure transport is required
AND release artifacts are authenticated
AND supply-chain evidence is accepted
AND rollback eligibility is available
AND diagnostics are privacy-safe
AND public documentation is explicit
AND Architecture readiness gate is closed
AND QA evidence is current
-> public onboarding may be considered
```

Missing or unknown evidence blocks public onboarding by default.

## Privacy

The documentation must not ask operators to publish, paste, commit, or upload:

- usernames;
- home directories;
- full hostnames;
- MAC addresses;
- IP addresses;
- serial numbers;
- disk UUIDs;
- machine IDs;
- user process lists;
- personal paths;
- wallet keys, private keys, tokens, secrets, or credentials.

## Risks

- Treating this document as a public beta launch would be incorrect.
- Publishing install, support, reward, or bootnode claims before readiness gate
  closure would create false operator expectations.
- Leaving the old README testnet wording in place would imply a public testnet
  exists before IAMINE has closed the readiness gate.
- Future public docs must remain aligned with signed update, supply-chain,
  rollback, diagnostics, and admission contracts.
