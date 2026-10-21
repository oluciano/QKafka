# Workflow: Test Implementation

**When:** Adding tests for existing code or new scenarios.

---

## Testing Pyramid & Strategy

| Layer | Project | Focus | 3N Applied | Environment |
| :--- | :--- | :--- | :--- | :--- |
| **Unit** | `tests/qKafka.Tests` | Isolated logic: middleware, retry policy, key routing, saga transitions | Full (N1, N2, N3) | In-memory fakes |
| **Integration** | `tests/qKafka.IntegrationTests` (to create) | Real broker + real DB contracts | N1 | Docker (Testcontainers: Kafka, PostgreSQL, SQL Server) |
| **Reliability** | `tests/qKafka.ReliabilityTests` (to create) | Rebalance, crash recovery, duplicate delivery, outbox concurrency | N2 (complex failures) | Docker |

The in-memory `InMemoryKafkaTestHarness` (Phase 5, `specs/05-test-harness.md`) becomes the preferred unit/integration substrate once it exists.

---

## Test Integrity (3N Mandatory Matrix)

Every feature or bug fix produces at least 3 tests:
- **N1 — Positive:** happy path works.
- **N2 — Negative:** failure path behaves as specified (retry, DLT, rollback, no commit).
- **N3 — Invalid Input:** null, empty, boundary, corrupt serialization handled gracefully.

### Core Rules
- **No Fixed Sleeps:** `Task.Delay(50)` to wait for state is banned. Poll observed state (`TestWait.UntilAsync`), use `TaskCompletionSource` or an observed signal.
- **One Canonical Test File per Class:** `Foo` → `FooTests.cs`. No `FooHardeningTests.cs` twins.
- **Existing tests are contracts:** never weaken, invert or delete an assertion. Moving/merging into a `[Theory]` and deleting a proven duplicate are allowed.
- **Dual-Target:** tests pass on `net10.0`.
- **Real behavior over mocks:** fake the broker boundary (`IKafkaConsumer`, producer), not the code under test.

> **N2 case study: Outbox publisher concurrency.** A unit test can verify the catch block for a DB error. A *distributed reliability test* is still required to prove that with N publisher instances competing, each outbox row is claimed by exactly one (`SKIP LOCKED`).

---

## Steps

### 1. Define Scope
What behavior? Preconditions? Expected outcome? Edge cases? Layer (unit / integration / reliability)?

### 2. Design (before code)
```
Test: PoisonPill_Deserialization_RoutedToDltAndOffsetCommitted
Given: a record whose value is not valid JSON
When:  the consumer loop polls it
Then:  IDltPublisher receives it with DltReason.Deserialization,
       the offset is committed, the next record is processed
```

### 3. Implement (Execution Mode)
- Real behavior, deterministic, zero warnings.
- Deliverable: test file under `tests/<Project>/`, all green.

---

## Requirements

- **Deterministic:** no timing-based assertions, no races; same result every run.
- **Coverage:** 80% global line coverage floor; N1, N2, N3 covered; timeout/cancellation covered where applicable.
- **Reliability scenarios** (see `reliability.md`): rebalance, broker down, DB down, poison pill, duplicate delivery, crash between publish and commit, key ordering under load.

```bash
dotnet test tests/qKafka.Tests -c Release
dotnet test tests/qKafka.ReliabilityTests -c Release   # when the project exists
```

---

## Structure Pattern

```csharp
public sealed class KafkaConsumerLoopTests
{
    [Fact]
    public async Task PoisonPill_Polled_RoutedToDltAndCommitted()
    {
        // Given
        var broker = new FakeKafkaConsumer(Record.WithValue("{not json"));
        var dlt = new RecordingDltPublisher();
        var loop = TestLoop.Create(broker, dlt);

        // When
        await loop.StartAsync(CancellationToken.None);

        // Then
        await TestWait.UntilAsync(() => broker.Committed.Count == 1);
        dlt.Published.Should().ContainSingle()
            .Which.Reason.Should().Be(DltReason.Deserialization);
    }
}
```

**Naming:** `{Scenario}_{Action}_{ExpectedResult}`, e.g. `SameKey_ManyMessages_ProcessedInOrder`, `HandlerThrows_RetriesExhausted_RoutedToDlt`, `Rebalance_RevokedPartition_InFlightDiscarded`, `SagaCommit_DbFails_StateAndOutboxRolledBack`.

---

## Validation (Validation Mode)
- [ ] Deterministic, no fixed sleeps
- [ ] Real behavior (fakes only at the broker/DB boundary)
- [ ] 3N complete; coverage floor held
- [ ] Pass on `net10.0`
- [ ] Zero warnings, StyleCop compliant

## Exit Criteria
- [ ] Scenario clear and bounded
- [ ] All tests pass
- [ ] Zero compiler warnings
- [ ] No existing assertion weakened
- [ ] Ready to commit
