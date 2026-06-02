# Specification 01: Core Engine & Consumer Loop

This specification defines the architecture, contracts, and implementation details for the **qKafka** Core Engine (Fase 1).

---

## 1. Architectural Design

The Core Engine decouples Kafka polling (broker connection) from the actual processing of messages (business logic). This protects the application against cascading rebalances (Rebalance Storms).

```
                      ┌──────────────────────┐
                      │ Kafka Broker         │
                      └──────────┬───────────┘
                                 │ Poll (Consume)
                                 ▼
                      ┌──────────────────────┐
                      │ KafkaConsumerLoop    │  ◄── Dedicated Polling Thread
                      └──────────┬───────────┘
                                 │ Enqueue by Key Hash
                                 ▼
                      ┌──────────────────────┐
                      │ ChannelDispatcher    │
                      └──────────┬───────────┘
                                 │ Route to bounded channels
                                 ▼
           ┌─────────────────────┴─────────────────────┐
           ▼                                           ▼
┌──────────────────────┐                    ┌──────────────────────┐
│  PartitionWorker 1   │                    │  PartitionWorker N   │
│ (Sequential per Key) │                    │ (Sequential per Key) │
└──────────────────────┘                    └──────────────────────┘
```

### Components

1. **`KafkaConsumerLoop`:** Runs in a dedicated background task. It continuously calls `IConsumer.Consume` and monitors memory buffer capacity. If the internal channel capacity is reached, it pauses fetching messages (via `Consumer.Pause()`) but continues to invoke the heartbeat or handle broker interactions.
2. **`ChannelDispatcher`:** Distributes consumed messages to a pre-allocated pool of specialized workers (workers 1 to N, where N is `ConcurrencyLimit`). It hashes the `Partition Key` (using modulo operation: `int index = Math.Abs(key.GetHashCode()) % N`) to route messages to one of the fixed bounded channels, ensuring that messages with the **same partition key are executed in strict sequential order** by the same worker while capping resource utilization and preventing memory leaks.
3. **`PartitionWorker`:** A dedicated loop per channel that pulls messages and resolves the corresponding `IKafkaConsumer<T>` to execute the business logic.

---

## 2. Contracts & Implementation Details

### `KafkaConsumerLoop`
Handles partition assignment events, deserialization, and enqueuing.
```csharp
internal sealed class KafkaConsumerLoop : BackgroundService
{
    private readonly IConsumer<byte[], byte[]> _consumer;
    private readonly ChannelDispatcher _dispatcher;
    private readonly ILogger<KafkaConsumerLoop> _logger;
    // ...
}
```

### `ChannelDispatcher` & `PartitionWorker`
Maintains order of execution per Key via a fixed pool of bounded channels.
```csharp
internal sealed class ChannelDispatcher
{
    private readonly Channel<ConsumeResult<byte[], byte[]>>[] _channels;
    private readonly int _maxConcurrency;

    public ChannelDispatcher(int maxConcurrency)
    {
        _maxConcurrency = maxConcurrency > 0 ? maxConcurrency : Environment.ProcessorCount;
        _channels = new Channel<ConsumeResult<byte[], byte[]>>[_maxConcurrency];
        for (int i = 0; i < _maxConcurrency; i++)
        {
            _channels[i] = Channel.CreateBounded<ConsumeResult<byte[], byte[]>>(
                new BoundedChannelOptions(100)
                {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleReader = true,
                    SingleWriter = false,
                });
        }
    }

    public async Task DispatchAsync(ConsumeResult<byte[], byte[]> result, CancellationToken ct)
    {
        if (result == null)
        {
            throw new ArgumentNullException(nameof(result));
        }

        var key = result.Message.Key != null 
            ? Encoding.UTF8.GetString(result.Message.Key) 
            : Guid.NewGuid().ToString();

        var bucketIndex = Math.Abs(GetDeterministicHashCode(key) % _maxConcurrency);
        var channel = _channels[bucketIndex];
        
        await channel.Writer.WriteAsync(result, ct).ConfigureAwait(false);
    }

    private static int GetDeterministicHashCode(string str)
    {
        unchecked
        {
            int hash1 = (5381 << 16) + 5381;
            int hash2 = hash1;

            for (int i = 0; i < str.Length; i += 2)
            {
                hash1 = ((hash1 << 5) + hash1) ^ str[i];
                if (i + 1 < str.Length)
                {
                    hash2 = ((hash2 << 5) + hash2) ^ str[i + 1];
                }
            }

            return hash1 + (hash2 * 1566083941);
        }
    }
}
```

### Polymorphic Deserialization
During message consumption, a header (e.g. `qkafka-message-type`) indicates the target payload type.
```csharp
public interface IKafkaMessageDeserializer
{
    object Deserialize(byte[] data, Type targetType);
}
```
If the consumer loop reads a message type it doesn't recognize or fails to deserialize, the **Poison Pill Policy** takes action.

### Poison Pill Policy
1. Catch deserialization exceptions.
2. Log the exception and extract metadata.
3. Publish the raw payload to the Dead Letter Topic (DLT) matching the format: `<original-topic>.dlt`.
4. Commit the offset of the corrupted message immediately.
5. Do NOT crash or stop the Polling thread.

### Topic Auto-Provisioning (UX/DX for New Users)
To prevent the application from hanging or throwing connection exceptions when a topic does not yet exist:
* During `KafkaConsumerLoop` initialization, the engine uses the Confluent.Kafka `IAdminClient` to query topic metadata.
* If a topic is missing and the environment is **Development** or **Staging**, the engine automatically provisions the topic with safe defaults:
  * Partitions: 3
  * Replication Factor: 1
* In **Production**, the engine logs a high-severity warning if a topic is missing, but continues connection attempts without throwing startup blockers, allowing GitOps/External creation to take place.

### Partition Assignment Lifecycle Hooks (For Kafka Experts)
To support advanced cluster management, cache synchronization, and tracking:
* Developers can register hooks by implementing `IPartitionAssignmentListener`:
  ```csharp
  public interface IPartitionAssignmentListener
  {
      Task OnPartitionsAssignedAsync(IKafkaContext context, IEnumerable<TopicPartition> partitions);
      Task OnPartitionsRevokedAsync(IKafkaContext context, IEnumerable<TopicPartitionOffset> partitions);
  }
  ```
* These hooks are triggered by the consumer loop during Kafka rebalances. This allows expert developers to:
  * Flush or clear local in-memory caches.
  * Sync database states before the partition begins processing new messages.
### Composed Middleware Pipeline (Pipes & Filters)
To support an extensible, modular, and predictable execution flow, the message consumption pipeline in qKafka is built as a chain of middlewares (Pipes and Filters) similar to ASP.NET Core:

* **The Interface:**
  ```csharp
  public interface IKafkaConsumerMiddleware
  {
      Task InvokeAsync(IKafkaContext context, Func<Task> next);
  }
  ```

* **Built-in Pipeline Execution Order:**
  When a message is pulled by a partition worker, it runs through the following middleware layers in order:
  ```
  [Message from Channel]
           │
           ▼
     ┌───────────┐
     │  Logging  │ ◄─── Open Logging Scope (MessageId, Partition, Offset)
     └─────┬─────┘
           ▼
     ┌───────────┐
     │   OTel    │ ◄─── Extract TraceParent & start Activity Span
     └─────┬─────┘
           ▼
     ┌───────────┐
     │ DI Scope  │ ◄─── Create IServiceScope for dependency resolution
     └─────┬─────┘
           ▼
     ┌───────────┐
     │   Inbox   │ ◄─── Idempotency Guard (Lock message in database)
     └─────┬─────┘
           ▼
     ┌───────────┐
     │   Retry   │ ◄─── Catch exceptions, apply backoff, route to DLT
     └─────┬─────┘
           ▼
     ┌───────────┐
     │ Execution │ ◄─── Resolve IKafkaConsumer<T> & invoke ConsumeAsync
     └───────────┘
  ```

* **Custom Middleware Registration:**
  Developers can inject their own custom filters (e.g., for FluentValidation, authentication, or audit logs) dynamically during setup:
  ```csharp
  builder.Services.AddqKafka(options => { ... })
      .AddMiddleware<MyValidationMiddleware>();
  ```

### Configuration Simplification (Zero-Boilerplate appsettings.json)


Kafka configurations are notoriously verbose. qKafka addresses this through **Convention over Configuration**:

1. **Global Connection Inheritence:**
   Instead of configuring connection credentials and broker lists for every single consumer, they are defined globally once. Every consumer automatically inherits this configuration.
   ```json
   {
     "qKafka": {
       "BootstrapServers": "localhost:9092",
       "SaslUsername": "optional-global-username",
       "SaslPassword": "optional-global-password"
     }
   }
   ```

2. **Sensible Consumer Defaults:**
   The library configures low-level settings automatically to protect the application:
   * `AutoOffsetReset` defaults to `Earliest`.
   * `EnableAutoCommit` defaults to `false` (handled reliably by qKafka's sliding commit window).
   * Heartbeat timeouts are automatically set to safe values matching the engine's processing architecture.

3. **Attribute-Driven Declarations:**
   Routing parameters are specified directly on the consumer code, keeping configuration where it's used:
   ```csharp
   [KafkaConsumer("orders-topic", GroupId = "order-processing-service")]
   public class OrderConsumer : IKafkaConsumer<OrderPlaced>
   ```

4. **Targeted Overrides in appsettings.json:**
   If a specific consumer needs adjustments (e.g., higher concurrency or a different offset reset strategy), it can be overridden by its `GroupId` in `appsettings.json`. The user only writes what is modified, not the whole config:
   ```json
   {
     "qKafka": {
       "BootstrapServers": "localhost:9092",
       "Consumers": {
         "order-processing-service": {
           "ConcurrencyLimit": 8,
           "AutoOffsetReset": "Latest"
         }
       }
     }
    }
    ```

### Dead Letter Topic — Cross-Reference

The complete DLT behavior — naming convention (`<topic>.dlt`), required headers,
Poison Pill vs Max Retries scenarios, and how developers consume the DLT for
reprocessing — is fully specified in `specs/06-retry-dlt.md`.

This spec (01) defines only the Poison Pill entry point inside `KafkaConsumerLoop`.
The `RetryMiddleware` and `IDltPublisher` are defined in `specs/06-retry-dlt.md` and
implemented as part of Phase 1 alongside the Core Engine.

---

## 3. Observability (OpenTelemetry)

We will use a dedicated `ActivitySource` called `QKafka`:
* **Producer Spans:** Wrap publishing in an Activity, inject `traceparent` and `tracestate` into the Kafka message headers.
* **Consumer Spans:** Extract tracing context from headers, start a consumer Activity, and link it as a child of the producer span.

---

## 4. Testing Matrix (3N Mandatory Coverage)

### N1 — Positive:
* Enqueue messages with different keys and process them in parallel.
* Enqueue messages with the *same key* and assert they are processed in strict chronological order.

### N2 — Negative:
* Database or network timeout in the consumer: Verify the message retries or gets routed to DLT without stalling other partitions.
* Kafka Broker disconnects during consumption: Verify loop retries reconnection without memory leaks.

### N3 — Invalid Input:
* Message body is empty, null, or contains invalid JSON: Assert it routes to `<topic>.dlt` and commits the offset.
* Message headers are missing context: Assert standard defaults are used and processing continues.
