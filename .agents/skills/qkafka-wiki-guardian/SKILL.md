---
name: qkafka-wiki-guardian
description: >-
  Curates qKafka documentation structure so it stays small, canonical and discoverable: one home per topic, no duplicates,
  no fluff. Activate when adding or reorganizing docs (README, docs/, ARCHITECTURE.md) or when docs feel fragmented.
---

# qKafka Wiki Guardian

Docs layout: `README.md` (pitch, quick start), `docs/getting-started.md`, `docs/mental-model.md`, `docs/database-schema.md`, `ARCHITECTURE.md` (internals), `ROADMAP.md` (plan), `specs/` (design per feature).

## Rules

1. **One canonical home per topic.** Link to it from elsewhere; do not copy.
2. **README is short**: what it is, install, minimal example, comparison, links. Details go to `docs/`.
3. **Features must be findable**: every public capability (retry, DLT, outbox, inbox, sagas, dashboard, test harness) appears in `docs/` with an example and its guarantees.
4. **Specs are design, docs are usage.** Do not paste specs into docs; when a feature ships, write the usage page and keep the spec as history.
5. **No fluff**: no adjectives without a number or a guarantee behind them.
6. **Explain the Kafka way**: partitions, keys, offsets, consumer groups, rebalance, at-least-once. Do not describe qKafka as a queue.
7. **New page only if** the topic has no home and is larger than a section. Otherwise extend an existing page.

## Procedure

1. Search the docs for the topic (`grep -ri`). If it exists, edit there.
2. If duplicated, merge into the canonical page and replace the rest with a link.
3. Update README links and the docs index.
4. Hand off to `qkafka-docs-truth` to verify against the code.
