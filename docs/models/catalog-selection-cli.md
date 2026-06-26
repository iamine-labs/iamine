# Model Catalog Selection CLI

Feature:

```text
MODEL-CATALOG-SELECTION-CLI-001
```

## Contract

The model catalog is an explainable view over the approved registry and the
local node context. It does not discover arbitrary external models and it does
not bypass any admission gate.

Each catalog row reports:

- registry descriptor identity and size;
- local installation state;
- hardware compatibility;
- download policy;
- registry integrity;
- license policy;
- license acceptance;
- network policy;
- the resulting download action.

Download actions are stable strings:

```text
already_installed
ready
license_acceptance_required
incompatible
blocked
```

## CLI

```bash
iamine-node models catalog
iamine-node models select
iamine-node models recommend
iamine-node models download <model_id>
```

`models catalog` prints the full explainable catalog.

`models select` and `models recommend` choose the smallest compatible model
whose gates permit download. If no such model exists, they report no selection.

`models download <model_id>` performs a catalog preflight before any network
download. Requested models are not silently replaced with alternatives. A
blocked requested model reports the blocking action and gate details.

## Non-goals

This feature does not:

- download models during catalog or selection;
- load inference backends;
- start workers, P2P, PubSub, or scheduler paths;
- change runtime model execution;
- search Hugging Face outside the existing explicit `models search` command.

## QA Notes

The primary smokes are:

```bash
./target/debug/iamine-node models catalog
./target/debug/iamine-node models select
./target/debug/iamine-node models recommend
./target/debug/iamine-node models download llama3-3b
./target/debug/iamine-node models license accept llama3-3b --yes
./target/debug/iamine-node models catalog
```

Use an isolated `HOME` for QA so license acceptance state and model storage do
not touch the operator environment.
