# PRIVATE-TESTNET-RELEASE-001

## Objective

Define the v0.9 private-testnet release gate from the already-merged Milestone 2
features. This feature is a release-gate and QA feature. It must not add new
runtime behavior or silently redefine the ownership of network, scheduler,
worker, model, identity, transport, admission, observability, or resilience
domains.

The output is an executable release-gate package:

- a roadmap closeout document for the v0.9 private testnet;
- a QA checklist with concrete local, TS140, and Proxmox/R5500 commands;
- roadmap state that reflects the real evidence state.

## Product Boundary

This gate may declare the private-testnet launch package ready only after local
and field QA pass. It may not declare the broader private testnet operationally
stable until a later operating window proves the roadmap milestone target:

```text
10-50 nodes
3-10 operators
multiple physical networks
2-4 weeks of stable operation
```

That operating window is not simulated by this repository change. It must be
recorded as future operational evidence before IAMINE v0.9 is treated as a
completed stable private testnet.

## Covered Capabilities

The release gate ties together these Milestone 2 boundaries:

- P2P protocol compatibility negotiation;
- durable node identity registration;
- explicit bootnode discovery;
- WAN peer discovery;
- bounded NAT traversal and relay policy;
- private-testnet node admission;
- authenticated secure transport policy;
- bounded remote inference API admission;
- private-testnet observability phases;
- bounded load-resilience stress profile.

## Required Evidence

The feature branch must prove:

- every Milestone 2 dependency is `CLOSED` in the roadmap;
- no runtime code changes are required for this release gate;
- `main.rs` and `cluster_registry.rs` do not grow;
- local validation passes;
- TS140 validation passes from an exact feature identity;
- Proxmox/R5500 validation passes from exact feature identities;
- testnet-related CLI surfaces are present and fail closed where expected;
- load-resilience profile still passes a bounded field stress;
- optional tools are reported as skipped when unavailable, not as executed.

## Non-Goals

This feature does not:

- add public-testnet admission;
- add signed autoupdate;
- add reputation, rewards, wallet, or economic policy;
- alter scheduler selection logic;
- alter worker startup behavior;
- alter model eligibility or download gates;
- weaken private-testnet admission, secure transport, or remote inference
  rejection rules;
- assert that the 2-4 week operational soak has already happened.

## Integration Rules

Changes should stay in documentation and release-gate evidence unless validation
finds a concrete product regression. If a product regression is found, stop and
split the correction into the owning module or crate.

Allowed files for the expected implementation:

- `docs/architecture/private-testnet-release.md`;
- `docs/qa/private-testnet-release.md`;
- `docs/roadmap/v0.9-private-testnet-release-gate.md`;
- `docs/roadmap/iamine-product-roadmap.md`.

## Risk Controls

- Do not mark the milestone as operationally stable without real multi-operator
  time-window evidence.
- Do not commit local QA logs, resolved host addresses, usernames, home
  directories, hostnames, MAC addresses, IP addresses, serials, disk UUIDs,
  machine IDs, keys, tokens, or credentials.
- Use SSH aliases only in documentation and command examples.
- Use disposable remote QA worktrees when canonical remote working copies are
  dirty or already on another feature.
- Preserve untracked QA artifacts unless cleanup is explicitly authorized.
