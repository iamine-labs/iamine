# IAMINE LAN Beta Install Guide

This package is intended for controlled LAN beta testing. The installer copies
package files into an operator-controlled prefix and keeps service activation
manual.

## Preview

```bash
scripts/install.sh --prefix "$HOME/.local" --dry-run
```

## Install

```bash
scripts/install.sh --prefix "$HOME/.local" --yes
```

Then run:

```bash
$HOME/.local/bin/iamine-node --help
$HOME/.local/bin/iamine-node lan doctor --json
$HOME/.local/bin/iamine-node worker lifecycle readiness --json
```

## Services

Service templates are copied under:

```text
$HOME/.local/share/iamine/systemd/
$HOME/.local/share/iamine/launchd/
```

Review and edit them before loading them with systemd or launchd. The installer
does not load, enable, start, stop, or restart services.

## Uninstall

Preview:

```bash
scripts/uninstall.sh --prefix "$HOME/.local" --dry-run
```

Remove installed package files:

```bash
scripts/uninstall.sh --prefix "$HOME/.local" --yes
```

Uninstall preserves models, node configuration, hardware profiles, logs, and
service-manager state.
