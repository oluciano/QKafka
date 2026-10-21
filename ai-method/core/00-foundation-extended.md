# qKafka — AI Foundation (Extended)

**Read this for complex scenarios: consumer loop internals, Outbox/Inbox, Sagas, or architectural compliance checks.**

---

## Architecture Reference

See `ARCHITECTURE.md` and `docs/` for the authoritative system design, and `specs/NN-*.md` for the feature in hand.

```
[ Kafka Consumer ] ──► [ Inbox ] ──► [ Saga State Machine ] ──► [ Outbox ] ──► [ Kafka Producer ]
```

---

## Consumer Loop & Dispatcher Architecture

### KafkaConsumerLoop owns the poll thread
`KafkaConsumerLoop` runs on a dedicated thread (not the thread pool) and does only:
- `Consume()` from the Confluent client, keeping heartbeats alive.
- Deserialize via `IKafkaMessageDeserializer` (failures are Poison Pills, never thrown into the loop).
- Hand each message to `ChannelDispatcher`, pausing partitions when channels are full (back-pressure) and resuming when drained.
- Commit offsets that workers report as completed.

**Rule:** No business logic, database call, or retry runs on the poll thread. This is what prevents Rebalance Storms.

### ChannelDispatcher routes by Partition Key
`ChannelDispatcher` owns a fixed pool of `PartitionWorker`s, each reading a bounded `Channel<T>`.
- The key-to-worker mapping is stable, so messages with the same Partition Key run sequentially.
- Different keys run concurrently across workers.

**Rule:** Never change the key-to-worker mapping in a way that can reorder messages with the same key.

### Rebalance handling
`IPartitionAssignmentListener` is notified on assign/revoke. On revoke: stop feeding the partition, drain or abandon in-flight work, and never commit offsets for partitions the consumer no longer owns.

### Hosting Lifecycle on .NET 10
In `Microsoft.Extensions.Hosting` 10, `BackgroundService.StartAsync` no longer executes `ExecuteAsync` up to its first `await`.
Any validation or startup check that must fail fast during host startup MUST live in an explicit `StartAsync` override, never in `ExecuteAsync`.

### Decision logging is mandatory at decision points
The pipeline must log the *reason* for every automatic decision:
- Partition paused/resumed (back-pressure) → `LogDebug`
- Retry scheduled → `LogInformation` with delay and attempt
- Routed to DLT → `LogError` with `DltReason`
- Duplicate skipped by Inbox → `LogDebug`

---

## Middleware Pipeline

Order: **Logging → OTel → DI Scope → Inbox → Retry → Execution** (`KafkaPipelineBuilder`).

- `ScopeMiddleware` creates a fresh DI scope per message; handler instances are never reused.
- `RetryMiddleware` applies `RetryOptions` with exponential backoff; exhaustion → DLT.
- `ExecutionMiddleware` is the terminal step that invokes the handler.

**Rule:** Never add inline cross-cutting logic to `ExecutionMiddleware`. Register an `IKafkaConsumerMiddleware` instead.

**Rule:** A middleware that throws is a processing failure; the normal retry and DLT flow applies. Do not swallow exceptions in the dispatcher.

---

## Persistence Architecture & Contract Segregation

### Segregated contracts
- `IOutboxStore` — outgoing messages claimed with `SKIP LOCKED` (`UPDLOCK, READPAST` on SQL Server).
- `IInboxStore` — idempotency guard for incoming messages.
- `ISagaRepository<TState>` — saga state with optimistic concurrency.
- `IKafkaPublisher` / `IDltPublisher` — producing (Direct Mode or Outbox Mode).

**Rule:** Core interfaces are stable contracts and must never be modified without architect approval. Add overloads instead.

### EF Core stores
PostgreSQL and SQL Server are supported through `AddEntityFrameworkOutbox<TContext>`. Keep provider-specific SQL isolated in the provider code, not in the generic stores.

### Atomicity
Saga state update and outgoing Outbox rows commit in a **single database transaction**. Inbox mark-as-processed participates in the same transaction when the handler writes to the same database.

### OutboxPublisher
An async background service that claims batches, publishes to Kafka, and marks rows as sent. At-least-once: a crash between publish and mark causes a re-publish, so consumers must be idempotent (Inbox).

---

## Consumer Implementation Contract (The 5 Guarantees)

Every consumer path (handlers, saga handlers, middleware) must satisfy:

1. **Never silently drop:** a failed message is retried, then routed to the DLT with a reason. Poison Pills go to the DLT immediately.
2. **Idempotency:** the Inbox key is deterministic (`topic/partition/offset` or a stable message id).
3. **Trace propagation:** `traceparent` is read from message headers and set on the processing Activity, and written to headers on publish.
4. **Per-key ordering:** same Partition Key → sequential execution.
5. **Commit after durability:** offsets are committed only after processing succeeded or the DLT publish succeeded.

---

## Saga Rules

- Discovery via reflection of `IStartSaga<T>` / `IHandleSaga<T>`; correlation via `ICorrelateSaga<T>` or property convention; no match → DLT.
- State machine: `NotStarted ──► Active ──► Completed OR Failed`; transitions are written atomically.
- Compensation: `context.RegisterCompensation` actions run in LIFO order on failure.
- Optimistic concurrency conflicts are retried or surfaced; never silently overwritten.

---

## Design Constraints (Runtime Guarantees)

1. Poll loop never blocks and heartbeats are never delayed by business logic
2. Order is guaranteed per Partition Key
3. DLT publisher errors must never crash the consumer loop (swallow exceptions, log only)
4. Simple handlers must remain simple (`IKafkaConsumer<T>` needs no extra ceremony)
5. No unnecessary DTO requirements (messages must be minimal and reproducible)
6. The database is authoritative for all state (no cache optimization overrides it)
7. Zero warnings in Release builds (`TreatWarningsAsErrors = true`) on every target framework

---

## Testing Strategy

### Test layout
- Project: `tests/qKafka.Tests` (one `FooUnitTests.cs` per production file `Foo.cs`, same relative path; reliability tests use `FooReliabilityTests.cs`; see `ai-method/workflows/test.md`).
- Prefer real behavior and in-memory/test doubles of the broker over mocks of internals; database-backed tests use real PostgreSQL/SQL Server (Docker) when they depend on locking semantics.
- Phase 5 adds `InMemoryKafkaTestHarness`; once it exists, prefer it for handler and saga tests.

### Test Integrity & No Fixed Delay
- Never wait with a fixed delay (`Task.Delay(50)`).
- Use a polling wait helper or `TaskCompletionSource`.
- Every feature or bug fix must provide a **3N Mandatory Matrix** (Positive, Negative, Invalid/Boundary).

---

## Architecture Compliance Checklist

Verify these during implementation/fix:
- Respects the consumer/saga handler model (`IKafkaConsumer<T>`, `IStartSaga<T>`, `IHandleSaga<T>`).
- Does not introduce unnecessary DTOs.
- Does not bypass the database as source of truth.
- Does not introduce static/global state.
- Designs don't prevent deterministic testing.
- No in-memory optimization overrides persisted truth.
- Compiles with 0 warnings on every target framework.

---

## Code Quality Requirements

### Public API
- All public types and members must have XML documentation (`///`).
- Add overloads, never optional parameters to public interfaces (preserves binary/mock compatibility).

### Async & Concurrency
- Never use `.Result` or `.Wait()`.
- Always use `async/await`.
- Always propagate `CancellationToken`.
- Never ignore cancellation.
- Always use `.ConfigureAwait(false)` in library projects (`src/qKafka*`).

### Class Design
- Classes must be `sealed` by default.
- Only allow inheritance when explicitly required by architecture.

### Exception Handling
- Never silently swallow exceptions (log or rethrow).
- DLT publisher errors are the one exception (swallow only, log always).
