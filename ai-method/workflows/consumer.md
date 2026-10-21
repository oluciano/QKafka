# Workflow: Consumer / Producer

**When:** Changing how messages are consumed or produced: `KafkaConsumerLoop`, `ChannelDispatcher`, `PartitionWorker`, middleware pipeline, serialization, key ordering, offset commit, DLT routing, `IKafkaPublisher` (Direct and Outbox mode).
**Specs:** `specs/01-core-engine.md`, `specs/06-retry-dlt.md`, `specs/07-producer.md`.
**Code:** `src/qKafka/` (Outbox mode lives in `src/qKafka/Outbox/`).

---

## 1. Hard Rules (never violated)

1. **Poll thread never runs business logic.** `KafkaConsumerLoop` only polls, hands records to `ChannelDispatcher` through a bounded channel, and commits. Backpressure = pause partitions, never block `Poll`.
2. **Key ordering.** Same partition key → same `PartitionWorker` → sequential. Different keys may run concurrently.
3. **Offsets commit only after the handler outcome is durable** (success, or routed to DLT). Never commit before.
4. **Poison pills never block:** deserialization failure → DLT (`IDltPublisher`, `DltReason`) → commit offset → continue.
5. **Rebalance safety:** on revoke (`IPartitionAssignmentListener`) drain or discard in-flight work for those partitions, commit what is complete, never process revoked partitions afterwards.
6. **Pipeline order:** Logging → OTel → DI Scope → Inbox → Retry → Execution (`KafkaPipelineBuilder`). Changing the order needs architect approval.
7. **Producer:** Direct Mode awaits the broker ack; Outbox Mode writes to `IOutboxStore` in the caller's transaction and never talks to Kafka inline.

---

## 2. Steps

### Step 1 — Spec & Design (Architect Mode)
- Which of the hard rules does the change touch? Name each and say how it is preserved.
- Public surface: new interface/option? Add overloads, don't alter existing signatures.
- Define headers/serialization contract (header names, DLT envelope, content type) if affected.

### Step 2 — Tests First (3N)
- **N1:** message consumed, handler runs once, offset committed after; publish returns ack.
- **N2:** handler throws → retry with backoff → DLT after exhaustion; broker disconnect during poll/commit; DB failure in Inbox/Outbox; rebalance mid-processing; `CancellationToken` on shutdown.
- **N3:** null/empty value, corrupt JSON, unknown type header, missing key (null key), huge payload, negative/zero options (`MaxConcurrency = 0`).
- Order test: N messages with the same key observed strictly in order; two keys observed concurrently.
- Heartbeat test: a handler blocking for longer than `max.poll.interval.ms` does not stall polling.

### Step 3 — Implementation (Execution Mode)
- No `Task.Run` fire-and-forget; every task is owned and awaited on shutdown.
- No `.Result`/`.Wait()`; `CancellationToken` everywhere; `.ConfigureAwait(false)`.
- Confluent types stay internal to `src/qKafka`.

### Step 4 — Verification
- `dotnet build -c Release` (0 warnings), `dotnet format --verify-no-changes`.
- `dotnet test tests/qKafka.Tests --no-build`; for ordering/rebalance/commit changes also run the reliability suite (see `reliability.md`).
- OTel: spans/metrics for consume, retry, DLT, publish updated.

---

## 3. Exit Criteria
- [ ] Every hard rule above verified, in writing, in the handoff
- [ ] 3N matrix + ordering + heartbeat tests green, no fixed sleeps
- [ ] Offset commit order proven by a test
- [ ] DLT envelope/headers documented
- [ ] Zero warnings; `dotnet format` clean; `CHANGELOG.md` updated
