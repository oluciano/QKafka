# Workflow: Saga

**When:** Implementing or changing sagas: `IStartSaga<T>`, `IHandleSaga<T>`, `ISagaContext`, `Saga<TState>`, `KafkaStateMachine<TState>`, correlation, compensation, `ISagaRepository<TState>`.
**Spec:** `specs/03-sagas.md` (+ `specs/02-outbox-inbox.md` for atomicity).
**Code:** `src/qKafka/Sagas/`, `src/qKafka/Outbox/`, `src/qKafka.EntityFrameworkCore/`.

---

## 1. Hard Rules

1. **State machine:** `NotStarted ──► Active ──► Completed | Failed`. Only these transitions; each persisted.
2. **Atomic commit:** saga state update + outgoing messages (outbox rows) + inbox record commit in ONE database transaction. No publish before commit.
3. **Correlation:** resolved via `ICorrelateSaga<T>` or property convention; unresolved correlation → DLT, never silently dropped, never creates a new saga unless the message implements `IStartSaga<T>`.
4. **Compensation is LIFO:** `context.RegisterCompensation(...)` actions run in reverse registration order on failure; a compensation that throws is recorded and does not stop the remaining ones.
5. **Optimistic concurrency:** concurrent updates of the same saga instance use a version column; the loser retries, never overwrites.
6. **Idempotent handlers:** a redelivered message (Inbox hit) never re-executes saga logic.
7. **Retention:** `Completed`/`Failed` records are removed or archived per retention rules.

---

## 2. Steps

### Step 1 — Design
- State class (`TState`) fields, correlation key, events that start/advance/complete.
- Transition table: event × state → next state + outbox messages + compensation registered.

### Step 2 — Tests First (3N)
- **N1:** start → advance → complete; state and outbox rows visible together after a single commit.
- **N2:** handler throws mid-saga → compensations run LIFO → state `Failed`; DB failure during commit → neither state nor outbox rows persisted; concurrent events on one instance → version conflict retried.
- **N3:** event with no correlation id, unknown saga, event for a `Completed` saga, duplicate start, null state, corrupt persisted state.
- Atomicity test: kill between state write and outbox write must be impossible (fault injection inside the transaction → both rolled back).

### Step 3 — Implementation
- `src/qKafka` has no dependency on EF Core; persistence goes through `ISagaRepository<TState>`; the EF implementation lives in `qKafka.EntityFrameworkCore`.
- Discovery by reflection at startup, failures at startup (fail fast), not at first message.

### Step 4 — Verification
- Standard gates (format, Release build 0 warnings, tests).
- Run saga tests against InMemory and at least PostgreSQL (Docker/Testcontainers) when the repository is touched.

---

## 3. Exit Criteria
- [ ] Transition table documented in the spec
- [ ] Atomic state+outbox proven by fault-injection test
- [ ] LIFO compensation and concurrency conflict tests green
- [ ] Unresolved correlation → DLT test green
- [ ] Zero warnings; `CHANGELOG.md` and `docs/` updated
