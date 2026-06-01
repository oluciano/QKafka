# qKafka — Reliable, High-Performance, and Developer-Friendly Kafka for .NET

**qKafka** is a modern, lightweight, and developer-friendly messaging and workflow orchestration library for Apache Kafka in .NET 8+. It is built for teams who want the power of Kafka without the low-level boilerplate of the Confluent SDK or the commercial licensing and heavyweight abstractions of MassTransit.

It treats Kafka as a **distributed append-only log**, not a queue, offering native partition handling, out-of-the-box Sagas (State Machines), and Transactional Outbox/Inbox patterns—all under a free, business-friendly **MIT License**.

---

## Why qKafka?

Traditional .NET message buses try to hide the broker behind a generic queue abstraction. This breaks down with Kafka, which relies on partitioning keys, linear offsets, and consumer groups. qKafka embraces Kafka's native architecture while shielding you from its complexity.

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

## Core Features

- **Kafka-First Architecture:** Native support for partition keys, manual/automatic offset tracking via sliding windows, and partition-level parallelism.
- **Zero-Boilerplate Consumers:** Define consumers with simple C# classes. Source Generators register and wire up everything automatically.
- **Outbox & Inbox Patterns:** Guarantee *At-Least-Once* delivery and *Exactly-Once* processing (idempotency) with database-backed stores.
- **Lightweight Sagas:** Coordinate distributed transactions using simple C# handlers or fluent state machines, with automatic compensation.
- **Rebalance Protection:** Decoupled poll loop ensures heartbeats are sent continuously even during long-running database operations, eliminating cascading rebalances.
- **OTel Native:** Automatic W3C trace propagation in Kafka headers. Follow messages from producer to consumer.
- **Built-in Dashboard:** Visualise consumer lag, live message streams, and active Saga lifecycles.
- **In-Memory Testing Harness:** Unit test consumers, producers, and Sagas in milliseconds without Docker.

---

## Quick Start

### 1. Define a Message
```csharp
public sealed record OrderPlaced(Guid OrderId, string CustomerName, decimal Total);
```

### 2. Implement a Consumer
```csharp
public sealed class OrderPlacedConsumer : IKafkaConsumer<OrderPlaced>
{
    public async Task ConsumeAsync(OrderPlaced message, IKafkaContext context)
    {
        Console.WriteLine($"Processing order for {message.CustomerName}");
        await Task.CompletedTask;
    }
}
```

### 3. Register and Run
```csharp
var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddqKafka(options =>
{
    options.BootstrapServers = "localhost:9092";
});

var app = builder.Build();
await app.RunAsync();
```

---

## Project Structure

```
qKafka/
├── src/
│   ├── qKafka/                  # Core consumer/producer engine, rebalance protection
│   ├── qKafka.Sagas/            # Saga orchestrator, correlation, & state machine
│   ├── qKafka.Outbox/           # Outbox/Inbox persistence contracts & implementation
│   ├── qKafka.SchemaRegistry/   # Avro / Protobuf schema registry integrations
│   └── qKafka.Dashboard/        # Integrated telemetry & lag explorer dashboard
├── tests/
│   ├── qKafka.Tests/            # Unit tests & In-Memory Harness
│   └── qKafka.IntegrationTests/ # Integration tests with Testcontainers
├── ai-method/                   # Strict AI execution guidelines and invariants
└── README.md
```

---

## License

This project is licensed under the MIT License. Feel free to use, modify, and distribute it in commercial applications.
