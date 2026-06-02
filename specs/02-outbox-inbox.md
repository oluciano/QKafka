# Specification 02: Transactional Outbox & Inbox

This specification defines the architecture, database schema, and behavior for the **qKafka** Transactional Outbox and Inbox patterns (Fase 2).

---

## 1. Architectural Design

To guarantee **At-Least-Once** delivery and **Exactly-Once** processing (idempotency), qKafka uses database-backed stores as the single source of truth.

```
[ Application Transaction ]
   │
   ├─► Save business entities (e.g. Orders)
   └─► Save outgoing messages to Outbox Table
               │ (Atomic Commit)
               ▼
   [ Outbox Database Table ] ◄──────┐ Read undispatched
               │                    │
               ▼                    │
      [ OutboxPublisher ] ──────────┘
               │
               ▼ (Publish)
         [ Kafka Broker ]
               │
               ▼ (Consume)
         [ Inbox Guard ] ◄──────────► [ Inbox Database Table ]
               │ (Idempotency check)
               ▼
       [ User Consumer ]
```

---

## 2. Database Schema

For SQL-based relational databases (PostgreSQL/SQL Server), the tables will be modeled as:

### `Outbox` Table
```sql
CREATE TABLE qkafka_outbox (
    id UUID PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    partition_key VARCHAR(255) NULL,
    payload TEXT NOT NULL,
    headers TEXT NOT NULL, -- JSON formatted key-value headers
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    dispatched_at TIMESTAMP WITH TIME ZONE NULL,
    attempts INT DEFAULT 0
);
CREATE INDEX idx_qkafka_outbox_dispatched ON qkafka_outbox (dispatched_at) WHERE dispatched_at IS NULL;
```

### `Inbox` Table
```sql
CREATE TABLE qkafka_inbox (
    message_id VARCHAR(255) NOT NULL,
    consumer_group VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL, -- 'Processing', 'Processed', 'Failed'
    received_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE NULL,
    PRIMARY KEY (message_id, consumer_group)
);
```

---

## 3. Contracts & Execution Flow

### `OutboxPublisher`
A background worker (`IHostedService`) that queries the `Outbox` table.
* **Polling Interval:** Configurable (default 100ms) or triggered via an in-memory wake-up channel.
* **Concurrency Lock:** The **database implementation of `IOutboxStore`** (not the publisher itself) is responsible for acquiring the locks. It must use strategies like `SELECT ... FOR UPDATE SKIP LOCKED` (PostgreSQL/MySQL 8.0+) or row-locking hints like `WITH (UPDLOCK, READPAST)` (SQL Server) to ensure that multiple nodes do not publish the same messages simultaneously.

```csharp
public interface IOutboxStore
{
    Task SaveAsync(IEnumerable<OutboxMessage> messages, DbTransaction transaction, CancellationToken ct);
    
    /// <summary>
    /// Fetches a batch of unpublished messages. The implementation must apply database-level
    /// locking (e.g., SKIP LOCKED) to prevent concurrent instances from fetching the same batch.
    /// </summary>
    Task<IEnumerable<OutboxMessage>> FetchUnpublishedAsync(int batchSize, CancellationToken ct);
    
    Task MarkDispatchedAsync(Guid id, CancellationToken ct);
    Task IncrementAttemptsAsync(Guid id, CancellationToken ct);
}
```

### `IInboxStore`
Contract for checking and storing idempotency logs.

```csharp
public enum InboxStatus
{
    Processing,
    Processed,
    Failed
}

public interface IInboxStore
{
    Task<InboxStatus?> GetStatusAsync(string messageId, string consumerGroup, CancellationToken ct);
    Task RegisterProcessingAsync(string messageId, string consumerGroup, CancellationToken ct);
    Task MarkProcessedAsync(string messageId, string consumerGroup, CancellationToken ct);
    Task MarkFailedAsync(string messageId, string consumerGroup, CancellationToken ct);
}
```


### `Inbox Guard`
An execution filter or middleware intercepting execution inside `PartitionWorker`.
```csharp
internal sealed class InboxFilter : IKafkaConsumerFilter
{
    private readonly IInboxStore _inboxStore;

    public async Task ExecuteAsync(ConsumeContext context, Func<Task> next)
    {
        var messageId = context.MessageId;
        var group = context.ConsumerGroup;

        var status = await _inboxStore.GetStatusAsync(messageId, group, context.CancellationToken);
        if (status == InboxStatus.Processed)
        {
            // Message duplicate - acknowledge and skip execution
            return;
        }

        if (status == InboxStatus.Processing)
        {
            // Concurrent execution detected or in-progress rebalance cleanup
            throw new InvalidOperationException($"Message {messageId} is already being processed.");
        }

        await _inboxStore.RegisterProcessingAsync(messageId, group, context.CancellationToken);

        try
        {
            await next();
            await _inboxStore.MarkProcessedAsync(messageId, group, context.CancellationToken);
        }
        catch (Exception)
        {
            await _inboxStore.MarkFailedAsync(messageId, group, context.CancellationToken);
            throw;
        }
    }
}
```

---

## 4. Testing Matrix (3N Mandatory Coverage)

### N1 — Positive:
* Save entities and messages to Outbox in a single unit of work. Assert message is published to Kafka and marked as `dispatched_at`.
* Consume a message with a unique ID: Assert it is stored in the Inbox table and subsequent duplicate messages are skipped immediately without re-running consumer logic.

### N2 — Negative:
* Database connection fails while checking the Inbox: Assert processing is rejected and the message offset is NOT committed.
* Kafka broker goes offline during Outbox publish: Assert the Outbox message state remains undispatched, increases the attempt counter, and is successfully retried when the broker reconnects.

### N3 — Invalid Input:
* Message without `MessageId` or invalid unique identifiers: Generate a deterministic ID based on Partition/Offset metadata to guarantee fallback idempotency.
* Message payload is corrupted in Outbox store: Log failure, increment attempt count, and route to Dead Letter Topic if retries exceed maximum threshold.

---

## 5. Clean Architecture & Modular Injections

To prevent the API/Startup project from directly referencing the database DLLs or EF Core, the extension methods in `qKafka.Outbox` must be design-friendly for Class Libraries:

### Optional Migrations Assembly Config
The `AddEntityFrameworkOutbox<TContext>` will optionally accept configurations to support separate Migrations assemblies:
```csharp
public static IqKafkaBuilder AddEntityFrameworkOutbox<TContext>(
    this IqKafkaBuilder builder,
    Action<DbContextOptionsBuilder>? configureOptions = null) 
    where TContext : DbContext
```

This allows the Infrastructure class library to register its database configuration, setting the Migrations assembly internally, without exposing database choices to the API project.

