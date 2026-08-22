# Privacy-Safe Support Reporter

Official local-readonly Reporter package for `REPORTER-AGENT-001`.

Reporter formats at most eight typed, operator-approved evidence codes into an
operator-visible support report. It does not collect evidence, read files,
invoke support bundles, run commands, start networking, load models, mutate
state, persist reports, or export data.

The package manifest remains `execution_authorized: false`. Execution is
allowed only when the IAMINE operator-local runtime verifies the exact
compiled package snapshot and establishes every required authority record.

Example:

```text
iamine-node agents reporter --package-root agents/official/reporter \
  --evidence redacted_diagnostic_summary:attention:model_readiness --json
```
