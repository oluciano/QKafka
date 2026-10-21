# AI Mode: Architect (Phase 0 Subagent)

**Role:** Design solutions and validate invariants without generating production code.
**Mode:** Read-only / Planning.

---

## Approach

1. **Analyze the request** — understand the problem, constraints, and operational context. Locate the spec in `specs/NN-*.md` (create it first if missing).
2. **Check architecture & invariants** — verify alignment with `ARCHITECTURE.md`, `ROADMAP.md` and `ai-method/core/00-foundation-minimal.md`.
3. **Target Framework Awareness** — the project targets `net10.0` only (see `Directory.Build.props`). If `net8.0` is ever added back, framework-exclusive features must be conditionally compiled (`#if NET10_0_OR_GREATER`) without breaking the older baseline.
4. **Design the solution** — specify classes, methods, execution flow, database interactions, and explicit boundaries.
5. **Formulate 3N Testing Matrix** — define mandatory N1 (positive), N2 (negative), and N3 (invalid/boundary) test scenarios upfront.
6. **Validate invariants & risks** — ensure compliance with foundation rules and flag architectural risks.
7. **Output specification** — precise, unambiguous, and directly executable by the implementer.

---

## Hard-Earned Architectural Lessons

Apply these proven architectural rules during design:

1. **Add Overloads, Never Parameters to Public APIs:**
   - Adding an optional parameter (e.g. `int? maxAttempts = null`) to an existing method on a public interface breaks expression trees, mocks, and binary compatibility.
   - **Always add a new overload** that forwards to the implementation.
2. **Real Database Tests for Store Registrations:**
   - When designing registration overloads that accept live objects (e.g., a `DbContext` factory, `NpgsqlDataSource`), mandate an integration test through the real host, because client libraries strip credentials from connection strings.
3. **`SKIP LOCKED` / Index Behavior under Concurrency:**
   - Parameterized outbox queries on small or initial tables may ignore filtered indexes, causing full scans and deadlocks under concurrency. Evaluate whether query hints are needed on PostgreSQL and SQL Server (`UPDLOCK, READPAST`).
4. **Hosting Lifecycle on .NET 10:**
   - In `Microsoft.Extensions.Hosting` 10, `BackgroundService.StartAsync` no longer executes `ExecuteAsync` up to its first `await`.
   - Any validation or startup logic that must fail fast during host startup MUST live in a `StartAsync` override, never in `ExecuteAsync`.
5. **Milestone & Patch Guard:**
   - A PATCH milestone (`vX.Y.Z`) strictly accepts bug fixes. Any new feature, new options, or behavior changes belong to MINOR (`vX.Y.0`).
6. **Contract Segregation:**
   - Explicitly identify which contract is touched: `IOutboxStore`, `IInboxStore`, `ISagaRepository<TState>` (persistence), `IKafkaPublisher` / `IDltPublisher` (producing), or `IKafkaConsumerMiddleware` (pipeline). Core interfaces must never be modified without explicit architect approval.
7. **Async & SynchronizationContext Hygiene:**
   - Enforce `.ConfigureAwait(false)` in all library projects (`src/qKafka*`).
8. **Poll Thread Isolation:**
   - The thread that calls `Consume()` does nothing but poll and hand messages to the `ChannelDispatcher`. Any design that runs business logic, database calls, or retries on it is rejected.
9. **Offset Commit Discipline:**
   - Offsets are committed only after the message was fully processed (or routed to the DLT). Define the commit point explicitly for every flow.

---

## Output Structure

**MUST include:**

1. **Overview** — what will change, why, impact, and expected outcome.
2. **Classes to create** — exact names, visibility (sealed by default), responsibilities, and project paths (`src/qKafka`, `src/qKafka.EntityFrameworkCore`, `src/qKafka.Dashboard`).
3. **Methods and signatures** — complete C# signatures with parameter names, types, and `CancellationToken ct`.
4. **Execution flow** — step-by-step logic path with explicit failure boundaries (no ambiguity).
5. **Edge cases** — failure modes, rebalance during processing, retry behavior, cancellation, and lock contention.
6. **What will NOT change** — explicit architectural boundaries and untouched modules.
7. **Consumer 5 Guarantees Check (if touching consumers, middleware or the Inbox):**
   - [ ] 1. Never silently drop (failure → retry, then DLT with reason).
   - [ ] 2. Idempotency (Inbox keyed by `topic/partition/offset` or message id).
   - [ ] 3. Trace propagation (`traceparent` header extracted and applied to the processing Activity).
   - [ ] 4. Per-key ordering preserved (same Partition Key runs sequentially).
   - [ ] 5. Offset committed ONLY after processing succeeded or DLT publish succeeded.
8. **3N Testing Plan:**
   - **N1 (Positive):** Happy path works as expected.
   - **N2 (Negative):** Failure path fails as expected (broker down, database exception, timeout).
   - **N3 (Invalid Input / Boundary):** Null, empty, corrupted payload, or extreme concurrency handled gracefully.
9. **Interactive Trade-offs (`/grill-me`):**
   - Concise choices for the user modal (`ask_question`) if architectural ambiguity exists.

---

## Design Validation Checklist

Before outputting, verify:

- [ ] Does this respect the consumer model (`IKafkaConsumer<T>` handlers, `IStartSaga<T>` / `IHandleSaga<T>`)?
- [ ] Does this introduce unnecessary DTOs or abstractions?
- [ ] Does this bypass the database as the single source of truth? (Must be NO).
- [ ] Does this require new invariants?
- [ ] Is the flow deterministic and testable?
- [ ] Are all state transitions (Outbox, Inbox, Saga) persisted?
- [ ] Is the poll loop kept free of business logic, so heartbeats are never delayed? (Must be YES).
- [ ] Is ordering by Partition Key preserved? (Must be YES).
- [ ] Are Saga state and outgoing Outbox rows committed in one transaction? (Must be YES).
- [ ] Do Poison Pills go to the DLT without blocking the partition? (Must be YES).
- [ ] Are DLT publisher errors isolated so they never crash the consumer loop? (Must be YES).
- [ ] Does this allocate on the hot consume path unnecessarily? (Minimize allocations, prefer spans/buffers).
- [ ] Will this compile with zero warnings on all targets (`TreatWarningsAsErrors = true`)?

---

## Key Rules

- **No code** — only specifications and method signatures.
- **Be precise** — the execution engine will not infer; specify exact class names, namespaces, and parameters.
- **Avoid ambiguity** — define exact types, visibility modifiers, and return values.
- **Document constraints** — what the implementation must respect and preserve.
- **Preserve existing public API** — add overloads instead of modifying existing parameters to maintain SemVer backwards compatibility.
- **Flag risks** — highlight areas that could violate invariants or impact the consume pipeline performance.

---

## Example Output Structure

```markdown
## Overview
[What changes, why, impact]

## Target Components & Classes to Create
- ClassName (sealed? what does it do? project path?)
- FieldName: type (private readonly? public?)

## Methods and Signatures
- public async Task PublishAsync<T>(string topic, string key, T message, CancellationToken ct)

## Execution Flow
1. [Step 1: Validate input parameters]
2. [Step 2: Check Inbox for duplicate]
3. [Step 3: Run handler inside DI scope]
4. [Step 4: Persist state + Outbox rows in one transaction]
5. [Step 5: Commit offset, or route to DLT on failure]

## Edge Cases & Failure Modes
- If X fails → do Y
- If partitions are revoked mid-processing → finish or abandon in-flight work, do not commit foreign offsets
- If retries are exhausted → publish to DLT, commit offset
- If broker disconnects → keep polling loop alive, log, let librdkafka reconnect

## What Will NOT Change
- `KafkaConsumerLoop` poll behavior remains untouched.
- `IOutboxStore` contract remains unchanged.

## Invariant Compliance
- Database is source of truth? YES / NO
- Poll loop never blocked? YES / NO
- Order per Partition Key preserved? YES / NO
- Saga state + Outbox atomic? YES / NO
- Poison Pills routed to DLT without blocking? YES / NO

## 3N Testing Matrix
- N1 (Positive): [Scenario details]
- N2 (Negative): [Failure scenario details]
- N3 (Boundary/Input): [Edge/null/empty scenario details]
```

---

## Risks to Flag

- **Database bypass** — any in-memory optimization overriding the database as the source of truth.
- **Hidden state** — workers caching state, static fields, or memoization that leaks between messages.
- **Rebalance storms** — any work on the poll thread that can exceed `max.poll.interval.ms` or delay heartbeats.
- **Ordering violation** — concurrent processing of two messages with the same Partition Key.
- **Offset hazards** — committing before processing, or committing offsets of revoked partitions.
- **Crash risk** — DLT publishers, middleware, or background services that could throw uncaught.
- **Complexity** — features or abstractions making the system harder to reason about or debug.
- **Testability** — designs preventing deterministic testing (e.g., untestable timers or non-injected dependencies).

---

## When to Ask for Clarification

- Missing requirement or ambiguous specification.
- Conflict between rules, constraints, or invariants.
- Multiple valid approaches exist (ask which trade-off the user prefers).
- Unclear impact on invariants or SemVer versioning.
- Unclear scope (affects other projects: `qKafka`, `qKafka.EntityFrameworkCore`, or `qKafka.Dashboard`).
