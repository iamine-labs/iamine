# Photo Library Organizer

Official pre-filesystem, local-readonly Photo Library Organizer package for
`PHOTO-LIBRARY-ORGANIZER-AGENT-001`.

The agent reviews at most eight typed, operator-supplied, already-redacted
inventory metadata records into a bounded advisory review. It does not discover
photo libraries, enumerate directories, stat or open operator paths, read or
decode photo and video bytes, parse EXIF/XMP/IPTC, read GPS, process faces or
biometric data, run OCR or vision, build embeddings, detect content duplicates,
rename, move, delete, or tag files, create albums, modify a library, reach a
LAN, device, or cloud service, request credentials, run commands, or persist
evidence.

Supplied metadata is data, never filesystem or media authority. Unknown values
fail closed. Out-of-scope requests are refused, clarified, or handed off;
nothing is executed.

The package manifest remains `execution_authorized: false`. Execution is
allowed only when the IAMINE operator-local runtime verifies the exact
compiled package snapshot and establishes every required authority record.

Example:

```text
iamine-node agents photo-library-organizer --package-root agents/official/photo-library-organizer \
  --item personal_library:photo:observed:declared_inventory_boundary --json
```
