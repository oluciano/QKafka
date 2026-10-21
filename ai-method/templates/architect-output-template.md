# Architect Output Template

## Task: [Reference]

## Overview
- **What:** Feature description & scope
- **Why:** Rationale & alignment
- **Impact:** Systems affected & compatibility

## Classes to Create
### ClassName (public/internal sealed)
- **Location:** src/qKafka/...
- **Responsibility:** What it does
- **Dependencies:** IService (injected)

## Method Signatures
```csharp
public async Task<T> MethodNameAsync(Type1 p1, CancellationToken ct)
```
- **Purpose:** What it does
- **Notes:** Important behavior

## Execution Flow
1. Step 1 (Happy Path)
2. Inbox check (skip if already processed)
3. Persist state/outbox rows in one transaction, then commit offset
- **Edge Cases:** Duplicate delivery, DB failure, handler throws, rebalance, poison pill

## State Transitions
- **Before:** Message polled / Saga `Active`
- **After (Success):** Offset committed / Saga advanced or `Completed`
- **After (Failure):** Retry → DLT + offset committed / Saga `Failed` + compensation (LIFO)

## Architecture Compliance
- [ ] Storage is source of truth (Outbox/Inbox/Saga state persisted)
- [ ] No in-memory cache overrides storage
- [ ] Poll loop never runs business logic
- [ ] Order per partition key preserved
- [ ] Offset committed only after durable outcome
- [ ] Saga state + outbox rows in one transaction
- [ ] No Confluent types leaked in public API
- [ ] CancellationToken propagated
- [ ] DLT publisher failures do not crash the loop

## Risks & Mitigations
- **Storage Bypass:** all state via `IOutboxStore`/`IInboxStore`/`ISagaRepository`
- **Commit Order:** commit offset after durable outcome
- **Rebalance Storm:** keep polling decoupled from handlers

## Testing Strategy
- **Unit:** Isolated behavior, state transitions
- **Integration:** Workflow with real storage
- **Reliability:** Rebalance, crash recovery, duplicate delivery, concurrency

## Implementation Notes
- **New Files:** src/qKafka/..., tests/qKafka.Tests/...
- **Modified:** ChannelDispatcher.cs, IOutboxStore.cs
- **Fixed:** Core model and invariants MUST remain unchanged.
