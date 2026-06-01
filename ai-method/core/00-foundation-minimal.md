# qKafka — AI Foundation (Minimal)

**Read this for every coding task in qKafka. Do not deviate from these rules.**

---

## Core Invariants

- **Storage is the single source of truth** for Outbox, Inbox, and Saga states.
- **Poll Loop never blocks** — separate execution pipeline, automatic heartbeat keeping.
- **Poison Pills never block** — serialization errors log, direct to DLT, and commit offset.
- **Order is guaranteed per Partition Key** — messages with same key run sequentially.
- **Atomic Saga State & Outbox Commit** — Saga state updates and outgoing commands must commit in a single database transaction.

---

## State Transitions (Saga)

```
NotStarted ──► Active ──► Completed OR Failed
```

- When Saga is `Completed` or `Failed`, the state record is either removed or archived based on retention rules.
- Transitions must be written atomically to the database.

---

## Code Quality Standards

- Zero compiler warnings in `Release` builds (`TreatWarningsAsErrors = true`).
- Zero `NotImplementedException`.
- Classes `sealed` by default.
- Public APIs must have XML documentation (`///`).
- Standard StyleCop ordering: properties -> constructors -> public methods -> private methods.
- Propagate `CancellationToken` in all asynchronous calls.
- Use `.ConfigureAwait(false)` in all library projects (`src/qKafka*`).

---

## Testing Matrix (3N Mandate)

Every new feature or bugfix must have tests covering:
1. **N1 — Positive:** The happy path executes successfully.
2. **N2 — Negative:** Errors (e.g. database failure, broker disconnected) behave as specified (retry, DLT, visibility).
3. **N3 — Invalid Input:** Boundary values, nulls, corrupt serializations are handled gracefully without crash.

Never delete or modify a passing test to make new code pass. If a test breaks, fix the production code.
