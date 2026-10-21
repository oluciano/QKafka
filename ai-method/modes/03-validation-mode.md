# AI Mode: Validation (Phase 4 Subagent)

**Role:** Verify code compliance with architecture, invariants, testing integrity, and quality standards before opening a PR.
**Mode:** Read-only / Code Reviewer.

---

## Validation Gate

Execute this mode AFTER execution (Phase 4) to catch drift, hidden redesign, invariant violations, or compiler/analyzer regressions.

---

## Architecture Compliance Checks

### 1. Database Authority & Stateless Workers
- [ ] All state transitions persisted? (check before/after snapshots).
- [ ] In-memory caching overrides the database? (NO: the database is the single source of truth).
- [ ] `ChannelDispatcher` and `PartitionWorker` keep no state between messages beyond their queues?
- [ ] The database is consulted for every decision? (Inbox duplicate check, Saga state, Outbox claim).
- [ ] Execution scope isolated per message? (new DI scope per message, handler instances never reused).

### 2. Poll Loop & Ordering
- [ ] Poll thread only polls and hands off? (no handler, database, or retry work on it).
- [ ] Heartbeats can never be delayed by business logic? (bounded channels, back-pressure via `Pause`/`Resume`).
- [ ] Messages with the same Partition Key processed strictly sequentially?
- [ ] Rebalance handled? (`IPartitionAssignmentListener`: revoked partitions drained, no commits for partitions no longer owned).

### 3. Offsets & Delivery
- [ ] Offset committed ONLY after success or successful DLT publish?
- [ ] No auto-commit hiding failures?
- [ ] At-least-once preserved, duplicates absorbed by the Inbox?

### 4. Retry & DLT
- [ ] Retry policy applied with exponential backoff, attempt count not kept only in memory across restarts where it matters?
- [ ] Exhausted retries route to the DLT? (all paths covered).
- [ ] Poison Pills (deserialization errors) routed to the DLT without blocking the partition?
- [ ] DLT publisher exceptions swallowed/logged? (never propagate, never crash the consumer loop).
- [ ] `DltReason` set correctly for every route?

### 5. Outbox / Inbox / Saga (if touched)
- [ ] Saga state + outgoing Outbox rows committed in a single transaction?
- [ ] `OutboxPublisher` uses `SKIP LOCKED` (`UPDLOCK, READPAST` on SQL Server) so nodes never double-claim?
- [ ] Inbox guard runs before handler execution, keyed deterministically?
- [ ] Saga correlation falls back to the DLT when no saga is found?
- [ ] Compensations run LIFO on failure?
- [ ] Optimistic concurrency on `ISagaRepository<TState>` handled (retry or fail loudly)?

---

## Code Quality & Standards Checks

### 1. Compilation & Multi-Targeting
- [ ] Compiles with **0 warnings and 0 errors** on every target framework (`dotnet build -c Release`).
- [ ] `TreatWarningsAsErrors = true` strictly respected across the entire solution.
- [ ] Analyzers clean (those configured in `Directory.Build.props`, `.editorconfig`, `stylecop.json`).
- [ ] Banned APIs check (`BannedSymbols.txt`): NO `DateTime.Now` (use `DateTime.UtcNow`), NO `.Result`, NO `.Wait()`.

### 2. Async & Concurrency Integrity
- [ ] Pure `async/await` throughout (no `.Result` or `.Wait()`).
- [ ] `CancellationToken` propagated through all async calls without dropping or ignoring.
- [ ] `.ConfigureAwait(false)` present on all async calls in library projects (`src/qKafka*`).

### 3. StyleCop Compliance (SA Rules)
- [ ] **SA1202 / SA1204:** Member ordering (public members first, static helpers next, private instance methods last).
- [ ] **SA1413:** Multi-line initializers, parameter lists, and argument lists end with a trailing comma.
- [ ] **SA1508:** No redundant blank lines before closing braces `}`.
- [ ] **Variable Usage:** No unused local variables; deliberately unused marked with `_`.
- [ ] **Exception Handling:** All caught exceptions logged or rethrown; DLT publisher exceptions swallowed and logged.

### 4. Class Design & Documentation
- [ ] Classes `sealed` by default (unless inheritance is explicitly required).
- [ ] Dependencies injected, not static. No global state introduced.
- [ ] String comparisons specify `StringComparison.Ordinal` or `StringComparison.OrdinalIgnoreCase`.
- [ ] All public types and members have complete XML documentation (`///`). Docs describe behavior, not just repeat the name.
- [ ] Internal code simplified rather than accumulated (avoid `*WithX` / `*V2` sibling methods).

---

## Feature-Specific Checks

### 1. Middleware Pipeline
- [ ] Order preserved: Logging → OTel → DI Scope → Inbox → Retry → Execution.
- [ ] New cross-cutting logic added as an `IKafkaConsumerMiddleware`, not inline in `ExecutionMiddleware`.
- [ ] Middleware that throws is treated as a processing failure (retry/DLT applies).

### 2. Channel Dispatcher
- [ ] Channels bounded; back-pressure pauses partitions instead of growing memory.
- [ ] Fixed worker pool; key-to-worker mapping stable so ordering holds.
- [ ] Shutdown drains in-flight work and respects cancellation.

### 3. Observability & Telemetry
- [ ] Metrics reflect persisted/observed state (not transient in-memory counters).
- [ ] No derived or fake state exposed.
- [ ] OpenTelemetry activities and tags populated without excessive allocations on hot paths; `traceparent` propagated through headers.

---

## Integration & DI Checks

### 1. Dependency Injection
- [ ] Scoped services behave correctly (scope isolated per message).
- [ ] No shared state across executions.
- [ ] Handler instances never reused across multiple messages.

### 2. Serialization & Versioning
- [ ] `IKafkaMessageDeserializer` failures classified as Poison Pills, never thrown into the poll loop.
- [ ] Message type renaming won't silently break stored Outbox/Saga rows.
- [ ] Versioning strategy respected (add overloads instead of modifying existing public parameters).

### 3. Database Provider Differences
- [ ] Tolerates PostgreSQL and SQL Server via EF Core (`AddEntityFrameworkOutbox<TContext>`).
- [ ] Doesn't assume database-specific locking guarantees outside the provider-specific code.
- [ ] Handles timing differences and isolation level differences.

---

## Hidden Redesign Red Flags

Watch for subtle architectural violations:
- **In-memory caches** disguised as "performance optimization".
- **Worker/dispatcher state** (instance variables persisting state across messages).
- **Offset commits** moved before processing, or done from the poll thread without coordination.
- **DLT publisher exceptions** propagating uncaught and killing the consumer loop.
- **Database bypasses** via application-level memoization.
- **Synchronous blocking** in async paths.
- **Untraced state transitions** (persisted only partially, e.g. Saga state without its Outbox rows).
- **New DTOs** introduced silently.
- **Handler model complexity** growing unnecessarily.
- **Magic behavior** making system execution non-deterministic.

---

## Test Integrity & Rigor

### 1. Mandatory 3N Matrix
- [ ] **N1 (Positive):** Happy path operating as expected under normal conditions.
- [ ] **N2 (Negative):** Explicit failure path tested (broker failure, database exception, timeout).
- [ ] **N3 (Invalid Input / Boundary):** Null, empty, corrupt payload, or extreme concurrency handled gracefully.

### 2. Test Execution Integrity Rules
- [ ] **NO FIXED SLEEPS:** Absolutely no `Task.Delay(50)` in tests to wait for state. Must use a polling wait helper, `TaskCompletionSource`, or an observed signal. (A sleep is acceptable ONLY to assert that something did NOT happen).
- [ ] **Search Across ALL Test Projects:** When altering pinned values or behaviors, grep across every project in `tests/`, not just the local test project.
- [ ] **Existing Tests Are Contracts:** Existing assertions were NEVER weakened, inverted, or deleted to make new code pass. If behavior intentionally changed, a comment `// Behavior changed in vX.Y: <reason>` is required.
- [ ] **One Canonical Test File per Class:** Production file `<path>/Foo.cs` has exactly one unit test file `<path>/FooUnitTests.cs` (and, if a reliability project exists, one `<path>/FooReliabilityTests.cs`). Parallel files (`FooHardeningTests.cs`, `FooExtraTests.cs`) are strictly banned. `TestLayoutConventionTests` enforces it.
- [ ] **Prove Guard Tests Fail:** Concurrency or ordering guard tests must be proven by reverting the fix and watching the guard test fail.
- [ ] **Tie Concurrency Assertions to Specific Statement:** Avoid global counters that trip on unrelated rare events.
- [ ] **No Static State Leaks:** Test helpers with `static` state (e.g. recording DLT publishers) must not share state across parallel test fixtures.
- [ ] **Contract Test Self-Audit:** Run `git diff main -- tests` and ensure no assertion was weakened.

---

## Documentation & Release Readiness

- [ ] `CHANGELOG.md` (when present) updated under `## [Unreleased]` with a clear, concise bullet point.
- [ ] `ROADMAP.md` checkbox ticked for the delivered item; `specs/NN-*.md` status updated.
- [ ] `README.md` and `docs/` updated if new options, settings, or behaviors were added.
- [ ] **Compile and Run Doc Code:** Any code snippet placed in docs or READMEs must be compiled and verified.
- [ ] **No Unverified Performance Claims:** Do not claim a speed gain without a reproducible benchmark.

---

## Validation Output Format

The validation subagent must return a structured report:

```markdown
### Validation Verdict: [PASS | FAIL | CONDITIONAL]

#### 1. Invariants & Standards Audit
- [x] Zero warnings on all target frameworks
- [x] .ConfigureAwait(false) compliance
- [x] XML Docs & StyleCop rules
- [x] CancellationToken propagation
- [x] Database authority & poll loop isolation

#### 2. Spec & 3N Matrix Audit
- **N1 Positive:** [Test name & status]
- **N2 Negative:** [Test name & status]
- **N3 Invalid Input:** [Test name & status]

#### 3. Test Integrity Verification
- [x] No fixed sleeps (`Task.Delay` banned)
- [x] No static state leaks across tests
- [x] No existing assertion weakened

#### 4. Issues / Risks Detected (if FAIL)
- [File:Line] Specific violation and recommended minimal remediation.

#### 5. Recommendation for PR
- [Ready to open PR | Blocking fixes required]
```
