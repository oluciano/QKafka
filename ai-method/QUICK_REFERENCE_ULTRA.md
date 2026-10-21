# qKafka AI Router

**Rule:** Every task MUST have a corresponding `.md` spec file under `specs/` before coding.

## Core Workflows
- **feature:** foundation-minimal + `workflows/feature.md` + spec + validation matrices.
- **bugfix:** foundation-minimal + `workflows/bugfix.md` + bug analysis + regression test matrix.
- **refactor:** foundation-minimal + `workflows/refactor.md` + code quality rules + unit test check.
- **test / reliability / documentation / consumer:** `workflows/{test,reliability,documentation,consumer}.md`.

## Modes
- `modes/01-architect-mode.md` design (no code) → `02-execution-mode.md` implement → `03-validation-mode.md` verify → `04-release-mode.md` ship.
- Add `core/00-foundation-extended.md` only for consumer loop, Outbox/Inbox or Saga work.
- Templates for each handoff: `templates/`.

## Skills (`.agents/skills/`)
- `qkafka-task-cycle` full task lifecycle · `qkafka-release` release · `qkafka-docs-truth` docs vs code · `qkafka-wiki-guardian` docs curation.

## Non-Negotiable Invariants
- Separate poll thread & worker thread (rebalance protection).
- Sequential processing of matching keys.
- Idempotency guard via Inbox.
- Saga state + Outbox rows in one transaction.
- Poison Pills → DLT, never block.
- Zero warnings in Release build.
- 3N test matrix mandatory (Positive, Negative, Invalid Input).
