# HID Evidence

This directory contains committed evidence records for exact Git artifacts.
New records use `.hid/templates/evidence.json` and name the exact subject
`head_sha`, tree, bounded coverage, relevant dependencies, capture time, and
artifact/environment validity.

Evidence status is derived as `VALID`, `STALE`, `INVALID`, or `UNKNOWN`; it is
not persisted as a manually editable conclusion. Evidence never certifies a
branch name. A different artifact makes a record stale. v0.0.3 does not carry
evidence forward through ancestry or changed coverage.

Do not store full logs, prompts, model responses, credentials, personal paths,
hostnames, addresses, machine identifiers, environment dumps, or secret-bearing
commands here. Follow `.hid/privacy.yaml`.

Templates are not evidence. Narrative QA remains under `docs/qa/` and remains
authoritative throughout Shadow Mode.
