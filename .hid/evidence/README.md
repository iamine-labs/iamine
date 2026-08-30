# HID Evidence

This directory contains committed evidence records for exact Git artifacts.
Each record must use `.hid/templates/evidence.json` as its bounded shape and
must name the exact subject `head_sha` and `tree`.

Evidence never certifies a branch name. A different tree makes a record stale
until Architecture or QA records an explicit carry-forward decision. Do not
store full logs, prompts, credentials, personal paths, hostnames, IP addresses,
machine identifiers, or secret-bearing commands here.

Templates are not evidence. Narrative QA remains under `docs/qa/` and remains
authoritative during HID v0.0.1.
