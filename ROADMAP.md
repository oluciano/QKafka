# ROADMAP — qKafka Action Plan

This document serves as the strategic guide and action plan for the development of **qKafka**. The main goal is to build a .NET 8+ alternative to MassTransit for Kafka that is **free (MIT licensed)**, **Kafka-First** (respecting the broker's native architecture), and **infinitely simpler to configure and debug**.

---

## 🎯 Project Vision
qKafka solves the complexities of Apache Kafka in .NET by combining the robustness of MassTransit's Sagas and Outbox with high performance and operational simplicity.

**Strategic Mandate:** Our goal is to cover **100% of MassTransit's critical capabilities for Kafka** (resilience, complex Sagas, Outbox/Inbox patterns, polymorphic serialization, and testability) and **go much further**, resolving MassTransit's architectural limitations, licensing issues, and inherent complexities.

### Competitive Advantages (Parity + Beyond)
1. **MIT License (100% Free):** Free from the commercial constraints of MassTransit v9.
2. **Kafka-First vs. Simulated Queue:** Does not try to simulate classic queues (like RabbitMQ). Embraces partitions, keys, and linear offsets natively.
3. **Real Heartbeat Protection (Beyond MT):** Polling thread decoupled from Workers via bounded in-memory channels, preventing *Rebalance Storms* even during extremely long-running business processes.
4. **Ultra-Simple Configuration (Beyond MT):** Source Generators eliminate massive configuration and DI registration boilerplate.
5. **Native Dashboard (Beyond MT):** Integrated visual panel for Lag monitoring, live message streaming, and real-time Saga lifecycle timelines.
6. **Safe Concurrency by Key (Beyond MT):** Allows high concurrency per partition while strictly guaranteeing sequential processing for messages sharing the same Partition Key.

---

## 🛠️ Reference Architecture

```
                        [ Database ] 
                          ▲            │
             Inbox Check  │            ▼ Outbox Commits & Saga State
                          │      ┌─────────────┐
                          │      │   qKafka    │
                          │      │ Dispatcher  │
                          │      └──────┬──────┘
                          │             │ Publish events
                          │             ▼
[ Kafka Consumer ] ──► [ Inbox ] ──► [ Saga State Machine ] ──► [ Outbox ] ──► [ Kafka Producer ]
```

---

## 📅 Step-by-Step Action Plan (Phases)

### Phase 1: Core Engine (Consumption and Publishing)
*Focus: Establish base communication and the protected consumption loop.*
- [ ] Implement the `ConsumerLoop` abstraction over `Confluent.Kafka`.
- [ ] Create physical isolation between the Polling Thread and the message processing Thread Pool.
- [ ] Implement in-memory partitioning by key (`Partition Key`) to ensure messages with the same ID run sequentially, maintaining strict order.
- [ ] Create the automatic *Poison Pill* handling infrastructure (redirect serialization failures to a Dead Letter Topic and automatically commit the offset).
- [ ] Add native support for **OpenTelemetry** header propagation (W3C Tracing).
- [ ] **MassTransit Parity:** Implement polymorphic deserialization (multiple messages on a single topic/consumer) and extensible integration with Schema Registry (Avro, Protobuf, JSON Schema).
- [ ] **MassTransit Parity:** Implement Request/Response Client (`IRequestClient<TRequest, TResponse>`) over temporary topics or partition-based correlation.

### Phase 2: Extreme Reliability (Outbox & Inbox)
*Focus: At-Least-Once delivery and Exactly-Once processing (idempotency) guarantees.*
- [ ] **Outbox Pattern:** Create database transaction interceptor (Postgres/SQL/Mongo) to save outbound messages in the same logical transaction as application state mutations.
- [ ] **Inbox Pattern:** Create automatic duplicate-checking middleware based on a unique idempotency key before invoking the consumer.
- [ ] Create the background asynchronous publisher to read from the Outbox table and send to Kafka resiliently.
- [ ] **MassTransit Parity (Claim Check / Message Data):** Add support for large payloads (>1MB) by automatically saving bytes to Object Storage (AWS S3, Azure Blob, MinIO) and publishing only metadata/references via Kafka.

### Phase 3: Saga Engine (`QKafka.Sagas`)
*Focus: Distributed transactions that are easy to program and debug.*
- [ ] Implement Saga base classes and pluggable state persistence repositories (`ISagaRepository`).
- [ ] Create the automatic message correlation router based on payload properties or headers (`CorrelationId`).
- [ ] Develop the infrastructure to execute automatic compensating actions in reverse order (LIFO) if a Saga step fails.
- [ ] Guarantee absolute atomicity: Saga state updates and Outbox message writes must occur in a single database transaction.
- [ ] **MassTransit Parity (Message Scheduling):** Create support for message scheduling (delayed publishing) integrated with Background Job providers (Quartz.NET, Hangfire).

### Phase 4: Telemetry & Dashboard (`QKafka.Dashboard`)
*Focus: Immediate operational visibility for developers.*
- [ ] Create the visual dashboard integrated into the host application (no extra infrastructure required).
- [ ] **Lag Monitor:** Real-time visualization of pending messages per partition and consumer group.
- [ ] **Saga Timeline:** View historical and live state transition timelines for any active or completed Saga instance.
- [ ] **Message Streamer:** Live formatted JSON view of payloads flowing through local consumer loops.

### Phase 5: In-Memory Test Harness (`QKafka.Testing`)
*Focus: Enable fast unit tests and high-fidelity mock environments.*
- [ ] Develop `InMemoryKafkaTestHarness` to emulate topics, partitions, producers, consumers, and Saga behavior in memory.
- [ ] **MassTransit Parity:** Allow clean and complete test assertions (e.g. `harness.AssertPublished<OrderPlacedEvent>()`, `harness.AssertConsumed<OrderPlacedEvent>()`, verification of Saga final states, and retries).

---

## 🛡️ Code Invariants
Any new feature in the project must respect the following rules (according to [00-foundation-minimal.md](file:///home/luciano/git/qKafka/ai-method/core/00-foundation-minimal.md)):
1. The persistence database is the **single source of truth**.
2. **Kafka heartbeats** must never be delayed by business logic.
3. Every feature must produce tests covering the **3N Matrix** (Positive, Negative, Invalid/Boundary Input).
4. **Release** builds require **zero warnings** (`TreatWarningsAsErrors = true`).
