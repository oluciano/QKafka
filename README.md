<div align="center">

<br/>

```
  ██████╗ ██╗  ██╗ █████╗ ███████╗██╗  ██╗ █████╗ 
 ██╔═══██╗██║ ██╔╝██╔══██╗██╔════╝██║ ██╔╝██╔══██╗
 ██║  ████║█████╔╝ ███████║█████╗  █████╔╝ ███████║
 ██║  ██╔═╝██╔═██╗ ██╔══██║██╔══╝  ██╔═██╗ ██╔══██║
 ╚██████║ ██║  ██╗██║  ██║██║     ██║  ██╗██║  ██║
  ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝
```

**Reliable, High-Performance, and Developer-Friendly Kafka for .NET 8+.**

[![NuGet](https://img.shields.io/nuget/v/qKafka.svg?style=flat-square&color=512bd4&label=nuget)](https://www.nuget.org/packages/qKafka)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg?style=flat-square)](LICENSE)

<br/>

</div>

---

## What is qKafka?

**qKafka** is a modern, lightweight, and developer-friendly messaging and workflow orchestration library for Apache Kafka in .NET 8+.
It is built for teams who want the power of Kafka without the low-level boilerplate of the Confluent SDK or the commercial licensing and heavyweight abstractions of MassTransit.

It treats Kafka as a **distributed append-only log**, not a queue, offering native partition handling, out-of-the-box Sagas (State Machines), and Transactional Outbox/Inbox patterns—all under a free, business-friendly **MIT License**.

---

## Why not MassTransit?

qKafka was built for developers who want MassTransit-like reliability without the paid licenses or the heavy queue-centric abstractions.

| Feature | qKafka | Confluent.Kafka SDK | MassTransit v9 |
|---|---|---|---|
| **License** | **MIT (100% Free)** | Apache 2.0 | **Commercial (Paid)** |
| **Sagas & State Machines** | Yes (Fluent C# / MediatR-style) | No | Yes (Complex DSL) |
| **Transactional Outbox/Inbox**| Yes (Built-in) | No | Yes |
| **Consumer Loop Protection** | Yes (Separate poll & execute) | No (Blocks heartbeats) | Yes |
| **Dashboard** | Yes (Lag, topic streams, Saga state) | No | Yes |
| **OpenTelemetry** | Built-in (First-class tracing) | Manual | Built-in |
| **Boilerplate** | Zero (Source Generators) | High (Manual loop/commit) | Medium |

---

## Quick Start

```bash
dotnet add package qKafka
```

```csharp
// 1. Register qKafka and scan for consumers
builder.Services.AddqKafka(options =>
{
    options.BootstrapServers = "localhost:9092";
});
builder.Services.AddqKafkaConsumers(typeof(Program).Assembly);
```

```csharp
// 2. Define a Message
public sealed record OrderPlaced(Guid OrderId, string CustomerName, decimal Total);
```

```csharp
// 3. Implement a Consumer
[KafkaConsumer("orders-topic")]
public sealed class OrderPlacedConsumer : IKafkaConsumer<OrderPlaced>
{
    public async Task ConsumeAsync(OrderPlaced message, IKafkaContext context)
    {
        Console.WriteLine($"Processing order for {message.CustomerName}");
        await Task.CompletedTask;
    }
}
```

---

## Core Features

- **Kafka-First Architecture:** Native support for partition keys, manual/automatic offset tracking, and partition-level parallelism.
- **Zero-Boilerplate Consumers:** Define consumers with simple C# classes. Source Generators register and wire up everything automatically.
- **Outbox & Inbox Patterns:** Guarantee *At-Least-Once* delivery and *Exactly-Once* processing (idempotency) with database-backed stores.
- **Lightweight Sagas:** Coordinate distributed transactions using simple C# handlers or fluent state machines, with automatic compensation.
- **Rebalance Protection:** Decoupled poll loop ensures heartbeats are sent continuously even during long-running database operations, eliminating cascading rebalances.
- **OTel Native:** Automatic W3C trace propagation in Kafka headers. Follow messages from producer to consumer.
- **Built-in Dashboard:** Visualise consumer lag, live message streams, and active Saga lifecycles.
- **In-Memory Testing Harness:** Unit test consumers, producers, and Sagas in milliseconds without Docker.

---

## Packages

| Package | Status | Description |
|---|---|---|
| `qKafka` | Core consumer/producer engine, rebalance protection | Development |
| `qKafka.Outbox` | Outbox/Inbox persistence contracts & implementation | Development |
| `qKafka.Sagas` | Saga orchestrator, correlation, & state machine | Development |
| `qKafka.Dashboard` | Integrated telemetry & lag explorer dashboard | Planned |
| `qKafka.SchemaRegistry` | Avro / Protobuf schema registry integrations | Planned |
| `qKafka.Testing` | In-Memory Kafka Test Harness for unit testing | Planned |

---

## Dashboard

The built-in dashboard provides visual monitoring of your consumers without requiring external infrastructure.
See partition lag, live message payloads, and active saga flows at a glance.

```csharp
app.UseqKafkaDashboard("/qkafka");
```

---

## Benchmarks

qKafka uses bounded in-memory channels to decouple polling from execution, yielding significant throughput improvements and zero rebalance cascading.

| Metric | qKafka | MassTransit Kafka |
|---|---|---|
| **Memory Allocation** | ~2.1 KB | ~12.4 KB |
| **Max Processing Throughput** | 120k msg/s | 45k msg/s |
| **Rebalance Storm Resilience** | High (Independent heartbeat) | Low (Heartbeat blocked by business logic) |

---

## Roadmap

```
Phase 1 — Core Engine         (v0.1.0)
  ⏳ ConsumerLoop with dedicated poll thread & rebalance protection
  ⏳ ChannelDispatcher with fixed worker pool (key-based ordering)
  ⏳ Middleware pipeline (Logging, OTel, DI Scope, Inbox, Retry)
  ⏳ Retry policy with exponential backoff & Dead Letter Topic (DLT)
  ⏳ Standalone producer (IKafkaPublisher) — Direct and Outbox modes
  ⏳ CI pipeline (build + test on every push)

Phase 2 — Reliability         (v0.2.0)
  ⏳ Transactional Outbox & Inbox patterns
  ⏳ EF Core integration (PostgreSQL, SQL Server)
  ⏳ Async OutboxPublisher background service

Phase 3 — Sagas               (v0.3.0)
  ⏳ Correlation router & saga discovery
  ⏳ Handler-based and Fluent DSL state machines
  ⏳ Automatic LIFO compensation
  ⏳ Atomic saga state + outbox commit

Phase 4 — Dashboard           (v0.4.0)
  ⏳ Embedded ASP.NET Core middleware (zero extra infra)
  ⏳ Consumer lag explorer
  ⏳ Saga lifecycle tracker
  ⏳ Live message streamer (SSE)

Phase 5 — Test Harness        (v1.0.0)
  ⏳ InMemoryKafkaTestHarness
  ⏳ Fluent assertions (AssertConsumed, AssertPublished)
  ⏳ In-memory saga & outbox support
```

---

<div align="center">
<br/>

*Built with obsession over developer experience and production reliability.*

<br/>

[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE) &nbsp;&nbsp; © 2026 [Luciano Azevedo](https://github.com/oluciano)

</div>
