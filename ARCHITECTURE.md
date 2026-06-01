# qKafka — Architectural Specifications and Invariants

This document outlines the core architecture, design decisions, and strict invariants for **qKafka**. Any modifications to the codebase must preserve these rules.

---

## Core Invariants (Non-Negotiable)

1. **Decoupled Polling and Execution:** The Kafka polling loop (`IConsumer.Consume`) must run in a dedicated thread and must never block on user code execution. Heartbeats must be sent continuously to prevent partition rebalances.
2. **Order-Guaranteed Key Partitioning:** Messages with the same `Partition Key` must be processed sequentially in the order they arrived. Messages with different keys or in different partitions may be processed in parallel.
3. **At-Least-Once Delivery via Outbox:** Message publishing must support the Transactional Outbox pattern. Messages must be saved in the database in the same transaction as state changes and dispatched asynchronously.
4. **Idempotency via Inbox:** To prevent duplicate processing (e.g. after consumer rebalances), the Inbox store must verify message processing status before executing consumer code.
5. **No Poison Pill Blocks:** Serialization and decoding errors must never halt the partition. Corrupted messages must be logged, routed to a Serialization Dead Letter Topic (DLT), and offsets committed immediately.
6. **Stateless Core, Stateful Sagas:** The consumer pipeline is stateless. Saga instances persist state in pluggable repositories (Postgres, MongoDB, Redis).

---

## Threading & Rebalance Protection Model

To avoid the infamous "Rebalance Storms" associated with Confluent.Kafka when user processing is slow, qKafka implements a dual-queue architecture:

```
                  ┌──────────────────────┐
                  │ Kafka Broker         │
                  └──────────┬───────────┘
                             │ Poll Messages
                             ▼
                  ┌──────────────────────┐
                  │ Dedicated Poll Loop  │ ◄─── Continues heartbeats
                  └──────────┬───────────┘      while buffer has capacity
                             │ Enqueue
                             ▼
                  ┌──────────────────────┐
                  │ In-Memory Channels   │
                  └──────────┬───────────┘
                             │ Dispatch by Key
                             ▼
        ┌────────────────────┴────────────────────┐
        ▼                                         ▼
┌──────────────┐                          ┌──────────────┐
│ Key 1 Worker │                          │ Key 2 Worker │
└──────────────┘                          └──────────────┘
```

1. **Poll Loop:** Fetches messages, monitors `Channel` capacity, and sends background heartbeats. If the channel is full, polling is paused, but heartbeats are kept alive.
2. **Channels:** Bounded in-memory queues partitioned by `Partition Key` (Hash-based) to maintain order guarantees.
3. **Workers:** Thread-pool workers that execute user consumer logic asynchronously.

---

## Saga & State Machine Model

Sagas in qKafka coordinate long-running distributed workflows. They are stateful consumers.

```
Incoming Event ──► [Correlation Resolver] ──► [Saga Repository] ──► [Saga Exec Engine]
                         │                          │                      │
                  Resolves CorrelationId      Loads Saga State       Runs C# Handler
```

### Saga Persistence
All Saga state mutations and subsequent command dispatching must be atomic:
1. Load state using `CorrelationId`.
2. Execute Saga handler.
3. Commit new state and enqueue Outbox commands in a **single database transaction**.

---

## Observability & OpenTelemetry

Every message published or consumed must propagate OpenTelemetry context:
- **Headers:** The W3C `traceparent` and `tracestate` headers must be injected during production and extracted during consumption.
- **Activities:** Consuming and producing messages must be wrapped in `System.Diagnostics.Activity` to produce spans visible in Jaeger, Zipkin, or OTel-compliant collectors.

---

## Testing Strategy
All features must be validated using the **3N Mandatory Testing Matrix**:
- **N1 (Positive):** Normal execution, happy path.
- **N2 (Negative):** Failure mode (e.g., broker disconnect, database constraint failure) handles retry/DLT correctly.
- **N3 (Invalid Input):** Missing headers, serialization errors, null bodies do not crash the engine.
