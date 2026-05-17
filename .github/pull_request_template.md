# IAMINE Pull Request Checklist

## Required Local Checks

- [ ] `cargo fmt --all` or `cargo fmt --all -- --check` PASS
- [ ] `cargo test -p iamine-node` PASS
- [ ] `cargo test -p iamine-network` PASS
- [ ] `cargo build -p iamine-node` PASS
- [ ] `git diff --check` PASS
- [ ] `git diff --cached --check` PASS

## Architecture And Runtime

- [ ] `main.rs` growth reviewed and justified
- [ ] New logic lives in modules instead of being concentrated in `main.rs`
- [ ] Runtime behavior unchanged, or behavior change is explicitly declared
- [ ] Broadcast regression considered
- [ ] Worker mock/skip safety considered
- [ ] Cluster status impact considered if touched
- [ ] Result protocol / final outcome impact considered if touched
- [ ] Model/capability display impact considered if touched
- [ ] Metrics fallback impact considered if touched

## Field Impact

- [ ] TS140 impact considered
- [ ] Proxmox/R5500 impact considered
- [ ] Pending field QA is documented when hardware is unavailable

## Notes

- Runtime behavior changed: yes/no
- Field QA status:
- Known non-blockers:
