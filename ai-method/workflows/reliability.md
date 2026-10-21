# Workflow: Reliability Testing

**When:** Validating critical scenarios under failure, concurrency and distributed conditions: anything touching the consume path, offset commits, rebalance handling, outbox/inbox, or saga atomicity.

---

## Entry Criteria
- The scenario involves a core invariant (see `core/00-foundation-minimal.md`).
- Isolated unit tests cannot prove it; a real broker and/or real database is needed.

---

## Failure Matrix (each row needs at least one test)

| Failure | Expected behavior | Invariant |
| :--- | :--- | :--- |
| **Rebalance** during processing | Revoked partitions' in-flight work drained/discarded; completed offsets committed; no processing after revoke; no rebalance storm with slow handlers | Poll loop never blocks |
| **Broker down** (poll, commit, produce) | Consumer reconnects; uncommitted offsets redelivered; Direct publish fails visibly; Outbox publish keeps rows pending | At-least-once; Outbox as truth |
| **Database down** | Inbox/Saga/Outbox operations retried with backoff; offset NOT committed; when exhausted, message goes to retry/DLT policy, never lost | Storage is source of truth |
| **Poison pill** (corrupt payload, unknown type) | DLT with reason, offset committed, next message processed | Poison pills never block |
| **Duplicate delivery** (redelivery after crash/rebalance) | Inbox guard prevents second execution; saga state unchanged | Idempotency |
| **Crash between publish and commit** | Outbox row stays pending → republished (duplicate possible, consumer idempotent); never lost | At-least-once |
| **Crash between handler success and offset commit** | Message redelivered; Inbox hit skips handler | Exactly-once *processing* |
| **Crash mid-saga transaction** | State and outbox rows both rolled back or both present | Atomic saga+outbox |
| **Concurrent outbox publishers** | Each row claimed by exactly one (`SKIP LOCKED`) | Single publication per claim |
| **Hot key / many keys** | Same key strictly ordered; other keys not starved | Order per partition key |
| **Graceful shutdown** | In-flight handlers complete or are cancelled; offsets committed for completed work | No lost commit |
| **Handler exhausts retries** | DLT envelope contains original headers, reason, attempt count | DLT routing |

---

## Test Infrastructure
- Docker via Testcontainers: Apache Kafka (KRaft), PostgreSQL 16, SQL Server 2022.
- xUnit, full isolation per fixture (unique topic + consumer group per test).
- Fault injection: a proxy/wrapper at the broker and DB boundary (e.g. stop container, `ToxiProxy`-style, throwing `IOutboxStore` decorator) — never `Task.Delay` guesses.
- Dual target `net8.0` and `net10.0`.

**Location:** `tests/qKafka.ReliabilityTests/` (create when the first scenario lands).

---

## Steps

### 1. Define the Scenario
Pick the row(s) of the matrix. State the invariant, the fault, the observable proof (committed offset, DB rows, DLT record, handler call count).

### 2. Design
```
Test: CrashAfterHandlerBeforeCommit_Redelivered_HandlerRunsOnce
Given: handler wrote its Inbox record in the same transaction as its side effect
When:  the consumer process is killed before the offset commit, then restarted
Then:  the message is redelivered, Inbox rejects it, side effect count == 1
```
Crash simulation: kill the host (separate process or cancel without graceful shutdown), then start a new host with the same group id.

### 3. Implement (Execution Mode)
- Wait on observed state (`TestWait.UntilAsync`), never fixed sleeps.
- Assert on durable state (DB rows, committed offsets via admin client), not on in-memory flags.

### 4. Validate (Validation Mode)
- Run the suite ≥ 3 times; any flake is a failure of the test or the code, never ignored.

```bash
dotnet test tests/qKafka.ReliabilityTests -c Release
```

---

## Exit Criteria
- [ ] Every touched row of the failure matrix has a passing test
- [ ] Tests deterministic (3 consecutive green runs)
- [ ] Proof uses durable state, not in-memory flags
- [ ] Passes on `net8.0` and `net10.0`
- [ ] Known gaps (what is NOT tested) written down in the PR / docs
