# LAN Beta Installer Polish

Feature:

```text
LAN-BETA-INSTALLER-POLISH-001
```

## Purpose

Make the validated v0.8 LAN beta easier to try on additional PCs without
changing runtime, scheduler, networking, model policy, inference, or worker
behavior.

## Scope

In scope:

- package-local `install.sh` and `uninstall.sh` scripts;
- `--dry-run` previews and explicit `--yes` writes;
- prefix-based install into operator-controlled directories;
- copied README, install guide, environment example, service templates, and
  manifest;
- package manifest entries for installer artifacts and runtime side effects;
- QA that proves installer actions do not start services, workers, downloads,
  model loads, or inference.

Out of scope:

- privileged installation;
- package-manager publishing;
- remote installation;
- service enablement or startup;
- auto-update;
- signed release artifacts;
- model download or license acceptance.

## Safety Contract

The installer must not:

- start, stop, restart, load, or enable services;
- scan process lists;
- mutate node configuration;
- delete model stores, hardware profiles, logs, or service-manager state;
- write usernames, hostnames, IP addresses, tokens, keys, or local absolute
  paths into the package manifest.

The uninstaller removes only files that the installer copies under the selected
prefix. It requires `--yes` unless `--dry-run` is used.

## Validation

Required validation:

- package assembly succeeds into an isolated output path;
- `install.sh --dry-run` reports a plan without writing;
- `install.sh --yes` installs into a temporary prefix;
- installed binary responds to `--help`;
- `uninstall.sh --dry-run` reports a plan without deleting;
- `uninstall.sh --yes` removes package files from the temporary prefix;
- manifest JSON is parseable and lists installer artifacts;
- runtime side effects remain false;
- tracked worktree and staging remain clean after QA.
