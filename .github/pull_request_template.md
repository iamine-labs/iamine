# IAMINE Pull Request Checklist

## Required Local Checks

- [ ] `./scripts/quality-gate.sh` PASS or PASS WITH WARNINGS
- [ ] `cargo fmt --all -- --check` PASS
- [ ] `cargo test -p iamine-models` PASS
- [ ] `cargo test -p iamine-node` PASS
- [ ] `cargo test -p iamine-network` PASS
- [ ] `cargo build -p iamine-node` PASS
- [ ] `cargo test --workspace` PASS
- [ ] `git diff --check` PASS
- [ ] `git diff --cached --check` PASS

## Optional Checks

- [ ] Clippy result recorded
- [ ] cargo-audit result or local skip recorded
- [ ] cargo-deny result or local skip recorded
- [ ] GitLeaks result or local skip recorded

## Architecture And Runtime

- [ ] `main.rs` line count and delta recorded
- [ ] Large-file warnings reviewed
- [ ] Generated artifacts guard PASS
- [ ] Sensitive files guard PASS
- [ ] New debt marker warnings reviewed
- [ ] New logic lives in modules instead of being concentrated in `main.rs`
- [ ] Runtime behavior unchanged, or behavior change is explicitly declared
- [ ] Broadcast regression considered
- [ ] Worker mock/skip safety considered
- [ ] Cluster status impact considered if touched
- [ ] Result protocol / final outcome impact considered if touched
- [ ] Model/capability display impact considered if touched
- [ ] Metrics fallback impact considered if touched

## Field Impact

- [ ] Field QA required: yes/no
- [ ] TS140 impact considered
- [ ] Proxmox/R5500 impact considered
- [ ] Pending field QA is documented when hardware is unavailable

## Notes

- Quality gate result:
- Required checks:
- Optional checks / skips:
- `main.rs` delta:
- Large-file warnings:
- Secrets / artifacts guards:
- Runtime behavior changed: yes/no
- Field QA required: yes/no
- Field QA status:
- Known non-blockers:
