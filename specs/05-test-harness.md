# Specification 05: In-Memory Test Harness

This specification defines the design, interfaces, and assertion methods for the **qKafka** In-Memory Test Harness (Fase 5).

---

## 1. Architectural Design

Unit and integration testing of messaging architectures can be slow and brittle when relying on local Docker containers. The Test Harness mocks the entire Kafka infrastructure in memory, allowing high-performance, deterministic assertions.

```
┌────────────────────────────────────────────────────────┐
│               InMemoryKafkaTestHarness                 │
│                                                        │
│  ┌──────────────────┐            ┌──────────────────┐  │
│  │   Mock Topics    │ ◄────────► │   Subscriptions  │  │
│  │    (InMemory)    │            │     & Offsets    │  │
│  └──────────────────┘            └──────────────────┘  │
│           ▲                               ▲            │
│           │ Publish                       │ Consume    │
│           ▼                               ▼            │
│  ┌──────────────────┐            ┌──────────────────┐  │
│  │  Mock Producer   │            │   Mock Consumer  │  │
│  └──────────────────┘            └──────────────────┘  │
└────────────────────────────────────────────────────────┘
```

---

## 2. Interface Contracts

### `InMemoryKafkaTestHarness`
Exposes registers of published and consumed messages, and allows starting/stopping consumer groups.
```csharp
public sealed class InMemoryKafkaTestHarness : IDisposable
{
    public IPublishedMessageList Published { get; }
    public IConsumedMessageList Consumed { get; }
    public ISagaTestList Sagas { get; }

    public Task StartAsync(CancellationToken ct);
    public Task StopAsync(CancellationToken ct);

    public Task PublishAsync<TMessage>(TMessage message, string? key = null) where TMessage : class;
}
```

### Fluent Assertions
Assertions must support timeouts to handle asynchronous loops.
```csharp
public interface IPublishedMessageList
{
    Task AssertPublished<TMessage>(TimeSpan? timeout = null) where TMessage : class;
    Task AssertPublished<TMessage>(Func<TMessage, bool> predicate, TimeSpan? timeout = null) where TMessage : class;
}

public interface IConsumedMessageList
{
    Task AssertConsumed<TMessage>(TimeSpan? timeout = null) where TMessage : class;
    Task AssertConsumed<TMessage>(Func<TMessage, bool> predicate, TimeSpan? timeout = null) where TMessage : class;
}
```

---

## 3. Execution Flow in Tests

A typical test case uses a standard Dependency Injection setup with the harness:

```csharp
[Fact]
public async Task Should_Process_OrderPlaced_Event()
{
    // Arrange
    await using var harness = new InMemoryKafkaTestHarness();
    harness.ConfigureServices(services =>
    {
        services.AddqKafkaConsumers(typeof(OrderConsumer).Assembly);
    });
    
    await harness.StartAsync();

    // Act
    await harness.PublishAsync(new OrderPlaced(Guid.NewGuid(), "Customer"));

    // Assert
    await harness.Consumed.AssertConsumed<OrderPlaced>(msg => msg.CustomerName == "Customer");
}
```

---

## 4. Testing Matrix (3N Mandatory Coverage)

### N1 — Positive:
* Publish a message: Verify the consumer executes and `AssertConsumed` resolves successfully.
* Verify multiple consumers on the same topic receive their respective copies.

### N2 — Negative:
* Consumer throws exception: Verify that `AssertConsumed` fails or registers the exception, and the message goes to the `<topic>.dlt` register of the harness.
* Assert checking for a message that was never sent: Verify it throws an assertion exception with a clear explanation of what message types were present.

### N3 — Invalid Input:
* Asserting on a null filter predicate: Verify it throws `ArgumentNullException`.
* Corrupt serialization: Harness registers serialization error, routes to DLT list, and completes task.
