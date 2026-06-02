# Specification 06: Retry Policy & Dead Letter Topic (DLT)

This specification defines the retry behavior, backoff strategy, and Dead Letter Topic
semantics for the **qKafka** consumer pipeline (cross-cutting concern across all phases).

---

## 1. Architectural Position

The `RetryMiddleware` sits between the `InboxMiddleware` and the final `ExecutionMiddleware`
in the built-in pipeline. It is responsible for catching exceptions thrown by user consumer
code, applying the configured backoff strategy, and routing messages to the DLT when
all retry attempts are exhausted.

```
[Inbox Middleware]
       │
       ▼
[Retry Middleware]  ◄─── catches exceptions, applies backoff, routes to DLT
       │
       ▼
[Execution Middleware]  ◄─── invokes IKafkaConsumer<T>.ConsumeAsync
```

---

## 2. Two DLT Scenarios

There are two distinct paths that lead to the DLT. They must be handled separately:

### Scenario A — Poison Pill (Deserialization Failure)
Triggered inside `KafkaConsumerLoop` **before** the message reaches the pipeline.
* The raw bytes cannot be deserialized into a known message type.
* No retry is attempted — the message is corrupt by definition.
* The raw payload is forwarded to the DLT immediately and the offset is committed.

### Scenario B — Consumer Execution Failure
Triggered inside `RetryMiddleware` when `IKafkaConsumer<T>.ConsumeAsync` throws.
* The retry policy is applied (see Section 3).
* If all retries are exhausted, the fully deserialized message is serialized back to JSON
  and forwarded to the DLT with enriched error headers.
* The offset is committed after successful DLT publish.

---

## 3. Retry Policy Configuration

### Default Values

| Setting             | Default | Description                                                     |
|---------------------|---------|-----------------------------------------------------------------|
| `MaxRetryAttempts`  | `3`     | Number of attempts before routing to DLT. `0` = no retries.    |
| `InitialDelay`      | `250ms` | Delay before the first retry.                                   |
| `BackoffMultiplier` | `2.0`   | Multiplier applied to delay on each attempt.                    |
| `MaxDelay`          | `30s`   | Cap on delay growth regardless of multiplier.                   |

### Backoff Formula

```
delay(attempt) = min(InitialDelay * BackoffMultiplier ^ attempt, MaxDelay)

attempt 1 → 250ms
attempt 2 → 500ms
attempt 3 → 1000ms
```

### Configuration via appsettings.json

Global defaults can be overridden per consumer group:

```json
{
  "qKafka": {
    "BootstrapServers": "localhost:9092",
    "Retry": {
      "MaxRetryAttempts": 3,
      "InitialDelayMs": 250,
      "BackoffMultiplier": 2.0,
      "MaxDelayMs": 30000
    },
    "Consumers": {
      "payment-service-group": {
        "Retry": {
          "MaxRetryAttempts": 5,
          "InitialDelayMs": 500
        }
      }
    }
  }
}
```

### Configuration via Attribute

Per-consumer overrides at the code level:

```csharp
[KafkaConsumer("payments-topic", GroupId = "payment-service-group",
    MaxRetryAttempts = 5, InitialDelayMs = 500)]
public sealed class PaymentConsumer : IKafkaConsumer<PaymentReceived> { ... }
```

---

## 4. DLT Message Contract

When a message is routed to the DLT (either Scenario A or B), the following headers
must be present in addition to all original message headers:

| Header                          | Value                                                              |
|---------------------------------|--------------------------------------------------------------------|
| `qkafka-dlt-source-topic`       | Original topic name                                                |
| `qkafka-dlt-source-partition`   | Original partition number                                          |
| `qkafka-dlt-source-offset`      | Original offset                                                    |
| `qkafka-dlt-reason`             | `"poison-pill"` or `"max-retries-exceeded"`                        |
| `qkafka-dlt-exception-type`     | Full exception type name (Scenario B only)                         |
| `qkafka-dlt-exception-message`  | Exception message truncated to 500 chars (Scenario B only)         |
| `qkafka-dlt-attempt-count`      | Number of attempts made before routing to DLT                      |
| `qkafka-dlt-timestamp`          | UTC ISO-8601 timestamp of the DLT routing event                    |

### DLT Topic Naming Convention

```
<original-topic>.dlt

examples:
  orders-topic    →  orders-topic.dlt
  payments-topic  →  payments-topic.dlt
```

### DLT Consumer (Reprocessing)

Developers can consume DLT topics like any other topic using the standard `[KafkaConsumer]`
attribute. The enriched headers are available via `IKafkaContext.Headers`:

```csharp
[KafkaConsumer("orders-topic.dlt", GroupId = "orders-dlt-inspector")]
public sealed class OrderDltConsumer : IKafkaConsumer<OrderPlaced>
{
    public async Task ConsumeAsync(OrderPlaced message, IKafkaContext context)
    {
        var reason = context.Headers.GetString("qkafka-dlt-reason");
        var originalOffset = context.Headers.GetString("qkafka-dlt-source-offset");
        // inspect, alert, or requeue manually
        await Task.CompletedTask;
    }
}
```

---

## 5. Contracts

```csharp
public sealed class RetryPolicy
{
    public int MaxRetryAttempts { get; init; } = 3;
    public TimeSpan InitialDelay { get; init; } = TimeSpan.FromMilliseconds(250);
    public double BackoffMultiplier { get; init; } = 2.0;
    public TimeSpan MaxDelay { get; init; } = TimeSpan.FromSeconds(30);
}

public interface IDltPublisher
{
    /// <summary>
    /// Routes a failed message to its Dead Letter Topic with enriched diagnostic headers.
    /// </summary>
    Task PublishAsync(
        ConsumeResult<byte[], byte[]> original,
        DltReason reason,
        Exception? exception,
        int attemptCount,
        CancellationToken ct);
}

public enum DltReason
{
    PoisonPill,
    MaxRetriesExceeded
}
```

---

## 6. Testing Matrix (3N Mandatory Coverage)

### N1 — Positive:
* Consumer succeeds on the first attempt: Assert no retry is triggered and Inbox is
  marked `Processed`.
* Consumer fails twice then succeeds on the third attempt: Assert correct delay between
  attempts and final `Processed` status.

### N2 — Negative:
* Consumer fails on all `MaxRetryAttempts`: Assert message is published to `<topic>.dlt`
  with all required headers, offset is committed, and Inbox is marked `Failed`.
* DLT publish itself fails: Assert the exception is logged, the offset is NOT committed,
  and the pipeline does not crash.

### N3 — Invalid Input:
* `MaxRetryAttempts` configured as a negative number: Treat as `0` (route to DLT
  immediately) and log a configuration warning.
* Exception message exceeds 500 characters: Assert it is truncated before writing to
  the DLT header.
