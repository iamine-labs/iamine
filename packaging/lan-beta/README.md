# IAMINE LAN Beta Package

This directory contains artifacts for the IAMINE installable LAN beta. Package
assembly does not install, start, stop, or restart services. The generated
package includes explicit install and uninstall helpers for operator-controlled
prefixes.

## Clean Install

1. Build or obtain a LAN beta package.
2. Review `manifest.json` and the included checksum.
3. Preview the install:

```bash
scripts/install.sh --prefix "$HOME/.local" --dry-run
```

4. Install into an operator-controlled prefix:

```bash
scripts/install.sh --prefix "$HOME/.local" --yes
```

5. Run:

```bash
$HOME/.local/bin/iamine-node --help
$HOME/.local/share/iamine/scripts/first-run-preflight.sh --binary "$HOME/.local/bin/iamine-node"
```

Use `--skip-lan-smoke` if the operator wants to validate package readiness
without running the bounded `cluster status --json` LAN smoke.

6. Configure a service manager manually if desired.
7. Start the worker explicitly:

```bash
$HOME/.local/bin/iamine-node --worker --port=9000
```

## Service Templates

Linux systemd:

```text
systemd/iamine-worker@.service
```

macOS launchd:

```text
launchd/com.iamine.worker.plist.template
```

The launchd file intentionally uses placeholders. Replace them outside the
repository before loading the service.

The install helper copies templates for review only. It does not load, enable,
start, stop, or restart service-manager units.

## Upgrade

1. Create a new package.
2. Keep the currently working binary available for rollback.
3. Run `scripts/install.sh --prefix <prefix> --dry-run`.
4. Run the first-run preflight against the new binary before replacing the
   running binary.
5. Stop the operator-managed worker through the configured service manager.
6. Replace the binary.
7. Restart the worker.
8. Run readiness and LAN beta smokes.

## Rollback

1. Stop the operator-managed worker.
2. Restore the previous binary and previous service configuration.
3. Restart the worker.
4. Run:

```bash
iamine-node worker lifecycle readiness --json
iamine-node lan doctor --json
```

Rollback must not rewrite model stores, node configuration, hardware profiles,
or logs.

## Uninstall

Preview:

```bash
scripts/uninstall.sh --prefix "$HOME/.local" --dry-run
```

Remove package-installed files:

```bash
scripts/uninstall.sh --prefix "$HOME/.local" --yes
```

Uninstall preserves models, node configuration, hardware profiles, logs, and
service-manager state.

## Privacy

Do not write usernames, home directories, hostnames, IP addresses, MAC
addresses, serial numbers, machine IDs, wallet keys, tokens, or private keys
into package files.
