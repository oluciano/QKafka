# ROADMAP — qKafka Action Plan

This document serves as the strategic guide and action plan for the development of **qKafka**. The main goal is to build a .NET 10+ alternative to MassTransit for Kafka that is **free (MIT licensed)**, **Kafka-First** (respecting the broker's native architecture), and **infinitely simpler to configure and debug**.

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

## 📦 Package Strategy
Deliberately small: **one main package**, plus two optional ones that exist only to avoid forcing heavy dependencies on everyone.

| Package | Contents |
|---|---|
| `qKafka` | Consumer/producer engine, pipeline, retry, DLT, Outbox/Inbox/Saga logic + in-memory stores, test harness, OpenTelemetry (namespaces `QKafka.Outbox`, `QKafka.Sagas`, `QKafka.Testing`) |
| `qKafka.EntityFrameworkCore` | PostgreSQL / SQL Server stores (keeps EF Core out of the main package) |
| `qKafka.Dashboard` | ASP.NET Core dashboard (Phase 4; keeps the ASP.NET shared framework out of Worker Services) |

A new package is only justified by a heavy or incompatible dependency (e.g. Schema Registry/Avro), never by a feature boundary.

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

## 💡 Ideas Imported from NexJob
*Source: `~/git/NexJob` (v6.7.0 docs, CHANGELOG and open issues). NexJob has no ROADMAP of its own, so these are the proven patterns and open lessons worth porting. Each item should become a spec in `specs/` before implementation.*

### Reliability & Delivery Semantics
- [ ] **Delivery-guarantees doc**: one page stating the cost of each failure (crash, rebalance, shutdown, broker/DB outage) like NexJob's `delivery-guarantees.md`.
- [ ] **Storage-outage resilience**: commit of a failed/succeeded message retried (3x with backoff); if exhausted, leave offset uncommitted and let rebalance/redelivery decide. Outbox publisher, Saga repo and Inbox must survive DB blips without ending the host.
- [ ] **Startup retry with exponential backoff** (1s → 30s) for registration work (saga/consumer registration, outbox init) when DB is down at start; distinguish permanent config errors (never retried) from transient ones.
- [ ] **Stale-entry guard**: after a rebalance/requeue, re-check state before executing (NexJob's Redis double-run bug, #647). Applies to Inbox + Saga state.
- [ ] **Heartbeat/orphan recovery** for in-flight saga steps and outbox rows claimed by dead nodes.
- [ ] **Execution timeout per handler** (`[ExecutionTimeout]`) and **deadlines** (message expires if not processed within N).
- [ ] **Checkpoints** (`GetCheckpoint<TState>`) for long-running handlers, with a warning log when a checkpoint cannot be deserialized.

### Protection & Flow Control
- [ ] **Circuit breaker per consumer/topic**: pause consumption when a downstream dependency keeps failing (`pause()` the partitions, not the poll loop).
- [ ] **`[Throttle]` per resource**, optionally cluster-wide (Redis) to cap concurrency against a shared dependency.
- [ ] **Execution windows**: consume only inside configured time windows.
- [ ] **Runtime control**: pause/resume consumers, requeue and delete DLT items, runtime settings persisted across restarts (`IRuntimeSettingsStore`).
- [ ] **Idempotency policies** (`DuplicatePolicy`) on publish, in addition to Inbox.
- [ ] **Handler filters** (`job-filters`) as a public extension point alongside the middleware pipeline.

### Observability & Dashboard (Phase 4 inputs)
- [ ] `Ctrl+K` global search, topology map (topic → consumer → saga → outbox), streaming logs via SSE, progress bars, themes.
- [ ] **Alerts** when a message reaches DLT for good (webhook/Slack/email hook).
- [ ] Health checks + OpenTelemetry package split (`qKafka.OpenTelemetry`).
- [ ] **Dashboard security by default**: localhost-only standalone host, authorization handler required (or loud warning) in embedded mode, CSRF/Origin check on POST, security headers + CSP, encode all stored values, optional payload masking, `Cache-Control: no-store`, no third-party fonts. (NexJob issues #594, #595, #598, #629)
- [ ] **Standalone dashboard host** for Worker Services without ASP.NET Core.
- [ ] Opt-in **scenario playground** (burst load, failures) and a public live demo.

### Integrations & Packaging
- [ ] **Trigger/bridge with NexJob**: Kafka topic → NexJob job and NexJob outbox → Kafka (NexJob already references qKafka as companion). Security rule: the configured job type always wins over a message header (#630).
- [ ] Multi-provider storage with one **shared contract test suite** (InMemory, PostgreSQL, SQL Server, later Redis/MongoDB).
- [ ] Multi-service guide: queue/consumer-group isolation by default per application; web app + worker split.
- [ ] Roslyn **analyzers** for misuse (blocking calls in handlers, missing correlation, etc.).
- [ ] Target `net10.0` only (LTS). Add `net8.0` to `TargetFrameworks` only on real demand; document the runtime-drop policy.

### Quality, Testing & Docs
- [ ] **"How we test" page** with honest gaps; reliability scenarios with multiple nodes on one DB; killed-process test; upgrade test (storage from previous version opens with the new one).
- [ ] Benchmarks vs Confluent SDK / MassTransit (BenchmarkDotNet, allocation focus, reproducible, publish the caveats).
- [ ] Docs: mental model, quickstart, best practices (Kubernetes), troubleshooting with exact log messages, FAQ, common scenarios, Hangfire/MassTransit-style migration page, maturity & API-stability statement, worker sizing / connection pool rule.
- [ ] Supply-chain hygiene: OpenSSF Scorecard, CodeQL, pinned Actions, Dependabot rules, scheduled `dotnet list package --vulnerable`.
- [ ] Rule justification test: every disabled analyzer rule in `.editorconfig` carries a reason, enforced by a test.

---

## 🛡️ Code Invariants
Any new feature in the project must respect the following rules (according to [00-foundation-minimal.md](file:///home/luciano/git/qKafka/ai-method/core/00-foundation-minimal.md)):
1. The persistence database is the **single source of truth**.
2. **Kafka heartbeats** must never be delayed by business logic.
3. Every feature must produce tests covering the **3N Matrix** (Positive, Negative, Invalid/Boundary Input).
4. **Release** builds require **zero warnings** (`TreatWarningsAsErrors = true`).
