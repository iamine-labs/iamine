# IAMINE Agent File Read-Only Template

Feature:

```text
AGENT-TEMPLATE-FILE-READONLY-001
```

## Purpose

Define the future file read-only agent template boundary without implementing
file readers, path access, runtime execution, writes, deletes, indexing,
persistence, package installation, registry publication, marketplace
publication, or model inference.

This document does not authorize arbitrary filesystem access, private path
collection, shell execution, file mutation, unrestricted network access,
secret access, wallet access, rewards, settlement, mainnet behavior, or
distributed model MoE.

## Template Question

File read-only template policy answers:

```text
What boundaries must exist before an agent can inspect allowed files?
```

It does not answer whether file APIs, runtime adapters, approval UI, audit
logs, transports, or content extraction exists.

## Draft Schema

```text
iamine.agent.template.file_readonly.draft-0.1
```

## Allowed Scope

Future read-only templates may request only:

```text
read_operator_selected_files
summarize_allowed_content
extract_non_secret_metadata
request_clarification
handoff_for_write_action
```

The operator must explicitly select allowed files or approved content
pointers. Whole-home scans, hidden path traversal, secret discovery, write
operations, deletion, chmod/chown, indexing, and uploads are out of scope.

## Required Guards

Future templates must declare:

```text
allowed_path_policy
max_file_count
max_file_size
allowed_extensions
redaction_policy
operator_visible_summary
write_action_blocked
```

## Privacy Rules

File template metadata must not include credentials, private keys, wallet keys,
usernames, full hostnames, IP addresses, MAC addresses, serial numbers, disk
UUIDs, machine IDs, private paths, raw user prompts, raw outputs, raw process
lists, unredacted logs, or permanent hardware fingerprints.

## Boundary Rules

- File read-only templates cannot authorize runtime execution.
- File read-only templates cannot write, delete, move, rename, chmod, chown, or
  upload files.
- File read-only templates cannot read arbitrary files.
- File read-only templates cannot collect secrets or private paths.
- File read-only templates cannot bypass validation, scope review, permission
  review, boundary tests, manual review, audit, or local registry review.

## Next Roadmap Step

```text
AGENT-TEMPLATE-NETWORK-DIAGNOSTIC-001
```
