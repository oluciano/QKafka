# Specification 07: Standalone Producer (`IKafkaPublisher`)

This specification defines the standalone message publishing contract for **qKafka**,
enabling any application component to publish messages to Kafka without being a consumer.

---

## 1. Problem

`IKafkaContext.PublishAsync` is scoped to an active consumption pipeline. A developer
who wants to publish from a controller, a background service, or a domain event handler
has no injection point today. This spec introduces `IKafkaPublisher` as a first-class
injectable service.

---

## 2. Architectural Design

```
[ Controller / Service / Domain Event ]
              │
              ▼ (injected via DI)
    [ IKafkaPublisher ]
              │
              ├─► Direct Mode: IProducer<byte[], byte[]> → Kafka Broker
              └─► Outbox Mode: IOutboxStore → Database → OutboxPublisher → Kafka Broker
```

### Two Publishing Modes

**Direct Mode** (default when `qKafka.Outbox` is not registered):
Publishes directly to the Kafka broker. Fast, but not transactionally safe — if the
database write succeeds and the broker is temporarily unavailable, the message is lost.
Suitable for non-critical events or fire-and-forget scenarios.

**Outbox Mode** (active when `qKafka.Outbox` is registered):
Saves the message to the Outbox table inside the caller's active `DbTransaction`.
The `OutboxPublisher` background service dispatches it asynchronously.
Guarantees At-Least-Once delivery even if the broker is down at publish time.

---

## 3. Contracts

```csharp
/// <summary>
/// Publishes messages to Kafka topics from any application component.
/// When qKafka.Outbox is registered, publishing is routed through the
/// Transactional Outbox for At-Least-Once delivery guarantees.
/// </summary>
public interface IKafkaPublisher
{
    /// <summary>
    /// Publishes a message to the topic associated with <typeparamref name="TMessage"/>.
    /// Topic is resolved via [KafkaTopic] attribute or kebab-case convention.
    /// </summary>
    Task PublishAsync<TMessage>(
        TMessage message,
        string? key = null,
        CancellationToken ct = default)
        where TMessage : class;

    /// <summary>
    /// Publishes a message to an explicit topic, bypassing convention-based resolution.
    /// </summary>
    Task PublishToAsync<TMessage>(
        string topic,
        TMessage message,
        string? key = null,
        CancellationToken ct = default)
        where TMessage : class;
}
```

### Topic Resolution Convention

When `PublishAsync<TMessage>` is called without an explicit topic, the engine resolves
the target topic in the following order of precedence:

1. `[KafkaTopic("topic-name")]` attribute on the message class.
2. Convention: `typeof(TMessage).Name` converted to `kebab-case` + `-topic`
   (e.g. `OrderPlaced` → `order-placed-topic`).

```csharp
/// <summary>
/// Explicitly declares the Kafka topic for a message type.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class KafkaTopicAttribute : Attribute
{
    public KafkaTopicAttribute(string topic) => Topic = topic;
    public string Topic { get; }
}

// Usage:
[KafkaTopic("orders-topic")]
public sealed record OrderPlaced(Guid OrderId, string CustomerName, decimal Total);
```

### DI Registration

```csharp
// Minimal — no outbox (Direct Mode)
builder.Services.AddqKafka(options =>
{
    options.BootstrapServers = "localhost:9092";
});
// IKafkaPublisher is now available for injection anywhere
```

```csharp
// With outbox (Outbox Mode — At-Least-Once delivery)
builder.Services.AddqKafka(...)
    .AddEntityFrameworkOutbox<AppDbContext>();
```

### Usage in a Controller

```csharp
[ApiController]
[Route("api/orders")]
public sealed class OrderController : ControllerBase
{
    private readonly AppDbContext _db;
    private readonly IKafkaPublisher _publisher;

    public OrderController(AppDbContext db, IKafkaPublisher publisher)
    {
        _db = db;
        _publisher = publisher;
    }

    [HttpPost]
    public async Task<IActionResult> CreateOrder([FromBody] CreateOrderRequest request)
    {
        await using var tx = await _db.Database.BeginTransactionAsync();

        var order = new Order(Guid.NewGuid(), request.CustomerName, request.Total);
        _db.Orders.Add(order);
        await _db.SaveChangesAsync();

        // Outbox Mode: saved atomically inside the same transaction
        await _publisher.PublishAsync(
            new OrderPlaced(order.Id, order.CustomerName, order.Total),
            key: order.Id.ToString());

        await tx.CommitAsync();
        return Ok(new { order.Id });
    }
}
```

---

## 4. Testing Matrix (3N Mandatory Coverage)

### N1 — Positive:
* Call `PublishAsync` in Direct Mode: Assert message arrives at the broker with correct
  topic, key, payload, and `qkafka-message-type` header.
* Call `PublishAsync` in Outbox Mode inside a transaction: Assert message is persisted
  in `qkafka_outbox` within the same transaction and dispatched after commit.

### N2 — Negative:
* Broker unavailable in Direct Mode: Assert `PublishAsync` throws and the exception
  propagates to the caller without being silently swallowed.
* Transaction rolls back in Outbox Mode: Assert no outbox row is persisted and no
  message is ever dispatched to the broker.

### N3 — Invalid Input:
* `message` argument is null: Assert `ArgumentNullException` is thrown immediately.
* `PublishToAsync` called with null or empty topic: Assert `ArgumentException` is thrown.
* Message type has no `[KafkaTopic]` attribute and the name cannot be resolved:
  Assert a clear `InvalidOperationException` with the message type name included.
