---
name: qkafka-task-cycle
description: >-
  Disciplined cycle for features, bugfixes and refactors in qKafka (Kafka messaging, outbox/inbox, sagas for .NET):
  grooming, spec before code, 3N test matrix (test first), atomic implementation, verification gate, handoff.
  Activate when implementing, fixing, refactoring or grooming a task in qKafka.
---

# qKafka Task Cycle

Workflow for **qKafka** (.NET 8+). Modes live in `ai-method/modes/`, workflows in `ai-method/workflows/`, invariants in `ai-method/core/00-foundation-minimal.md`.

```
Phase 0 Grooming ► Phase 1 Pre-flight ► Phase 2 3N tests ► Phase 3 Implementation
      ► Phase 4 Verification gate ► Phase 5 Docs & changelog ► Phase 6 Handoff / PR
```

## Phase 0 — Grooming (no production code)

Mode: `ai-method/modes/01-architect-mode.md`.

1. Read the request, `ROADMAP.md` (phase it belongs to) and the related `specs/NN-*.md`.
2. Inspect the relevant code read-only (use a research subagent if the area is large).
3. Produce: overview, classes and signatures, execution flow, edge cases, invariant compliance, 3N matrix.
4. **Spec before code:** every task has a `specs/NN-name.md` (see `ai-method/templates/`). Create or update it first. No spec, no code.
5. Ask the user only for real trade-offs (2 to 4 options, recommended first).
6. Optionally open a GitHub issue (English, `type(scope): description`) with context, target files, current vs expected behavior, DoD and 3N plan.

Deep questions (rebalance, ordering, offsets, outbox, saga): `references/grooming-guide.md`.

## Phase 1 — Pre-flight

- `git status` clean, branch from `main`: `<type>/<short-description>` (`feat/`, `fix/`, `refactor/`, `docs/`, `test/`).
- Check the target files on `main` (`git log -n 5 <path>`): is the task still valid, or already solved?
- Check the invariants it touches (poll loop, per-key order, DLT, atomic saga + outbox commit, DB as source of truth). If the change weakens one, stop and ask.

## Phase 2 — 3N test matrix (test first)

Workflows: `feature.md`, `bugfix.md`, `refactor.md`, `reliability.md`, `consumer.md`, `saga.md`, `test.md`.

| | Covers |
|---|---|
| **N1 Positive** | Happy path: message consumed, published, saga advanced |
| **N2 Negative** | Handler throws, broker disconnected, DB failure, rebalance during processing: retry, DLT or redelivery as specified |
| **N3 Invalid** | Null/empty/corrupt payload, unknown type, boundary values, duplicate message, concurrent same-key messages |

Write the tests, see them fail for the right reason, then implement. Never delete or weaken a passing test to make new code pass.

## Phase 3 — Atomic implementation

Mode: `ai-method/modes/02-execution-mode.md`.

- One concern per change. Follow the spec; do not widen scope.
- Library rules: `sealed` by default, XML docs on public API, `CancellationToken` everywhere, `.ConfigureAwait(false)` in `src/qKafka*`, no `.Result`/`.Wait()`, no `NotImplementedException`, `StringComparison.Ordinal`, StyleCop order.
- Never block the poll thread with business logic. Never commit an offset before the work (or its DLT routing) is durable.

## Phase 4 — Verification gate

Mode: `ai-method/modes/03-validation-mode.md`. Details: `references/verification-gate.md`.

```bash
dotnet format --verify-no-changes
dotnet build -c Release          # zero warnings
dotnet test -c Release           # all green
```

Fix production code, not tests. Report real output; if something was not run, say so.

## Phase 5 — Docs & changelog

- Add an entry under `## [Unreleased]` in `CHANGELOG.md` (create it if missing; Keep a Changelog format).
- Run the `qkafka-docs-truth` skill when behavior, options or public API changed (`README.md`, `docs/`, `ARCHITECTURE.md`, `ROADMAP.md` checkboxes).

## Phase 6 — Handoff

Use `ai-method/templates/execution-handoff-template.md` and `validation-report-template.md`. Commit with a conventional message (`feat(scope): ...`); open a PR to `main` only when the user asks. Tick the ROADMAP item only after the verification gate passes.
