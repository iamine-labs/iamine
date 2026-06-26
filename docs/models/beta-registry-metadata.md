# IAMINE Beta Registry Metadata

## Feature

```text
MODEL-BETA-REGISTRY-METADATA-001
```

## Contract

The v0.7 beta registry contains three approved GGUF artifacts:

- TinyLlama 1.1B Chat v1.0 Q4_K_M;
- Llama 3.2 3B Instruct Q4_K_M;
- Mistral 7B Instruct v0.2 Q4_K_M.

Every descriptor uses:

- an immutable Hugging Face repository commit in its download URL;
- the exact LFS object size;
- the exact SHA256 recorded by the upstream repository;
- explicit license metadata and revision;
- the existing distributed-network policy revision.

Changing an artifact, quantization, repository, or upstream commit requires a
new checksum and a registry metadata review.

## Provenance

Metadata was reconciled on 2026-06-25 against the official Hugging Face API.

| Model | GGUF repository revision | Size | SHA256 | License |
| --- | --- | ---: | --- | --- |
| TinyLlama 1.1B Chat v1.0 Q4_K_M | `52e7645ba7c309695bec7ac98f4f005b139cf465` | `668788096` | `9fecc3b3cd76bba89d504f29b616eedf7da85b96540e490ca5824d3f7d2776a0` | Apache-2.0 |
| Llama 3.2 3B Instruct Q4_K_M | `5ab33fa94d1d04e903623ae72c95d1696f09f9e8` | `2019377696` | `6c1a2b41161032677be168d354123594c0e6e67d2b9227c84f296ad037c728ff` | Llama 3.2 Community License |
| Mistral 7B Instruct v0.2 Q4_K_M | `3a6fbf4a41a1d52e415a4958cde6856d34b2db93` | `4368439584` | `3e0039fd0273fcbebb49228943b17831aadd55cbcbf56f0af00499be2040ccf9` | Apache-2.0 |

Source repositories:

- `TheBloke/TinyLlama-1.1B-Chat-v1.0-GGUF`;
- `bartowski/Llama-3.2-3B-Instruct-GGUF`;
- `TheBloke/Mistral-7B-Instruct-v0.2-GGUF`.

The corresponding base-model metadata reports Apache-2.0 for TinyLlama and
Mistral. Meta reports `llama3.2` for Llama 3.2 and requires explicit acceptance
of the license released on 2024-09-25.

## Gate Behavior

- TinyLlama and Mistral may pass license admission without local acceptance.
- Llama 3.2 remains blocked for download and install until the existing
  license-acceptance command records acceptance for license `llama3.2`,
  revision `2024-09-25`.
- Missing, malformed, placeholder, or mismatched hashes remain fail-closed.
- Listing remains side-effect free.

## Non-Goals

This feature does not download model files, alter hardware compatibility,
change backend availability, select a preferred model, change scheduler policy,
or start workers and inference.
