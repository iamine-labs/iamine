# IAMINE LAN Beta Package

This directory contains static artifacts for the IAMINE installable LAN beta.
They are templates and package inputs; they do not install, start, stop, or
restart services by themselves.

## Clean Install

1. Build or obtain a LAN beta package.
2. Review `manifest.json` and the included checksum.
3. Copy `bin/iamine-node` to an operator-controlled binary location.
4. Run:

```bash
iamine-node --help
iamine-node lan doctor --json
iamine-node worker lifecycle readiness --json
```

5. Configure a service manager manually if desired.
6. Start the worker explicitly:

```bash
iamine-node --worker --port=9000
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

## Upgrade

1. Create a new package.
2. Keep the currently working binary available for rollback.
3. Run `iamine-node --help` and `iamine-node lan doctor --json` from the new
   binary before replacing the running binary.
4. Stop the operator-managed worker through the configured service manager.
5. Replace the binary.
6. Restart the worker.
7. Run readiness and LAN beta smokes.

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

## Privacy

Do not write usernames, home directories, hostnames, IP addresses, MAC
addresses, serial numbers, machine IDs, wallet keys, tokens, or private keys
into package files.
