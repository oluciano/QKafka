---
name: qkafka-docs-truth
description: >-
  Keeps qKafka documentation equal to the code: README, docs/, ARCHITECTURE.md, ROADMAP.md, specs/, XML docs.
  Activate after any change to behavior, options, public API or schema, and before a release.
---

# qKafka Docs Truth

A doc line is true only if the code does it. Check, do not assume.

## Sources of truth

| Topic | Code | Docs to match |
|---|---|---|
| Options and defaults | `src/qKafka/QKafkaOptions.cs`, `ConsumerOptions.cs`, `RetryOptions.cs` | `README.md`, `docs/getting-started.md` |
| Consumer, pipeline, DLT | `src/qKafka/` | `docs/mental-model.md`, `ARCHITECTURE.md` |
| Outbox / Inbox, schema | `src/qKafka/Outbox/` | `docs/database-schema.md` |
| Sagas | `src/qKafka/Sagas/` | `docs/mental-model.md`, `specs/03-sagas.md` |
| Status of features | tests + code | `ROADMAP.md` checkboxes |

## Procedure

1. List what changed (`git diff main --stat`).
2. For each changed public type or option: grep the docs for its name and fix mismatches (names, defaults, signatures).
3. Compile every code sample in README and `docs/` mentally against the real API, or build a scratch project.
4. Check `docs/database-schema.md` against the real tables, columns and indexes.
5. ROADMAP: tick an item only if code and tests exist; untick if removed. Never claim a feature in README that is only planned (the comparison table too).
6. XML docs on every public member describe what the code does now.
7. Links and file paths in docs resolve.

## Style

Direct, developer to developer. No marketing words ("blazing", "seamless", "effortless"). State guarantees and their cost (at-least-once, ordering per key, what happens on crash). Mark planned features as planned.
