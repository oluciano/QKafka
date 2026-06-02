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

### Phase 1 — Core Engine (v0.1.0)
*Focus: Establish the protected consumption loop and the standalone producer.*
- [ ] Implement `ConsumerLoop` with dedicated poll thread & rebalance protection
- [ ] Implement `ChannelDispatcher` with fixed worker pool and key-based ordering
- [ ] Build middleware pipeline: Logging → OTel → DI Scope → Inbox → Retry → Execution
- [ ] Implement retry policy with exponential backoff (`RetryMiddleware`, `RetryPolicy`)
- [ ] Implement Dead Letter Topic routing (`IDltPublisher`) for Poison Pills and exhausted retries
- [ ] Implement standalone producer (`IKafkaPublisher`) in Direct Mode and Outbox Mode
- [ ] Set up CI pipeline with GitHub Actions: `dotnet build` + `dotnet test` on every push

### Phase 2 — Reliability (v0.2.0)
*Focus: At-Least-Once delivery and Exactly-Once processing guarantees.*
- [ ] Implement Transactional Outbox pattern with database-backed `IOutboxStore`
- [ ] Implement Inbox idempotency guard with `IInboxStore`
- [ ] EF Core integration for PostgreSQL and SQL Server (`AddEntityFrameworkOutbox<TContext>`)
- [ ] Async `OutboxPublisher` background service with `SKIP LOCKED` concurrency control

### Phase 3 — Sagas (v0.3.0)
*Focus: Distributed transactions that are easy to program and debug.*
- [ ] Implement saga discovery via reflection (`IStartSaga<T>`, `IHandleSaga<T>`)
- [ ] Implement correlation resolver (`ICorrelateSaga<T>`, property convention, DLT fallback)
- [ ] Handler-based Saga (`Saga<TState>`) and Fluent DSL (`KafkaStateMachine<TState>`)
- [ ] Automatic LIFO compensation on failure (`context.RegisterCompensation`)
- [ ] Atomic saga state + outbox commit in a single database transaction
- [ ] EF Core saga repository (`ISagaRepository<TState>`) with optimistic concurrency

### Phase 4 — Dashboard (v0.4.0)
*Focus: Immediate operational visibility with zero extra infrastructure.*
- [ ] Embedded ASP.NET Core middleware (`app.UseqKafkaDashboard("/qkafka")`)
- [ ] Consumer lag explorer (per partition, per group, real-time offsets)
- [ ] Saga lifecycle tracker (searchable, interactive state transition timeline)
- [ ] Live message streamer via Server-Sent Events (SSE)

### Phase 5 — Test Harness (v1.0.0)
*Focus: Fast, deterministic tests without Docker.*
- [ ] `InMemoryKafkaTestHarness` — full in-memory Kafka emulation
- [ ] Fluent assertions: `AssertConsumed<T>`, `AssertPublished<T>` with timeout support
- [ ] In-memory outbox and saga state support
- [ ] DLT register assertions (`AssertRoutedToDlt<T>`)

---

## 🛡️ Code Invariants
Any new feature in the project must respect the following rules (according to [00-foundation-minimal.md](file:///home/luciano/git/qKafka/ai-method/core/00-foundation-minimal.md)):
1. The persistence database is the **single source of truth**.
2. **Kafka heartbeats** must never be delayed by business logic.
3. Every feature must produce tests covering the **3N Matrix** (Positive, Negative, Invalid/Boundary Input).
4. **Release** builds require **zero warnings** (`TreatWarningsAsErrors = true`).
