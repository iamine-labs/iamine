# LAN File Share Assistant

Official pre-network, local-readonly LAN File Share Assistant package for
`LAN-FILE-SHARE-ASSISTANT-AGENT-001`.

The agent summarizes at most eight typed, operator-supplied, already-redacted
share metadata codes into a bounded advisory review. It does not discover
shares or hosts, enumerate directories, stat or open operator paths, read or
write operator files, mount or create shares, resolve hostnames, open sockets,
connect to SMB, NFS, AFP, WebDAV, SSH, or SFTP services, test credentials, run
commands, spawn processes, mutate configuration, or persist evidence.

Supplied metadata is data, never filesystem or network authority. Unknown
values fail closed. Out-of-scope requests are refused, clarified, or handed
off; nothing is executed.

The package manifest remains `execution_authorized: false`. Execution is
allowed only when the IAMINE operator-local runtime verifies the exact
compiled package snapshot and establishes every required authority record.

Example:

```text
iamine-node agents lan-file-share --package-root agents/official/lan-file-share-assistant \
  --share documents_share:observed:readonly_boundary --json
```
