# qKafka Mental Model & Message Lifecycle

To build reliable and high-performance event-driven systems with **qKafka**, it is essential to understand how messages flow through the library, where transactions occur, and how concurrency is managed.

This document guides you through the step-by-step lifecycle of both outgoing (Outbox) and incoming (Inbox/Consumer) messages.

---

## 1. The Big Picture

```
   [ Application Layer ] 
      │ 
      ├─► Mutates database state (e.g., updates Order)
      └─► Enqueues messages to Outbox (Atomic Transaction)
               │
               ▼
      [ Database: Outbox Table ] ◄───────────┐ Locks & Reads
               │                             │
               ▼                             │
      [ Outbox Publisher Background Task ] ──┘
               │
               ▼ (Publishes over Network)
         [ Kafka Broker ]
               │
               ▼ (Fetched by Polling Thread)
      [ qKafka Core: ConsumerLoop ]
               │
               ▼ (Extracts headers, deserializes)
      [ Inbox Idempotency Check ] ◄──────────► [ Database: Inbox Table ]
               │ (Locks & registers 'Processing')
               ▼
      [ Channel Dispatcher (Key Hash) ]
               │
               ▼ (Enqueues to bounded queue per key)
      [ Sequential Partition Key Worker ]
               │
               ▼ (Creates DI Scope)
      [ User Consumer: ConsumeAsync ]
               │
               ├─► Success: Updates Inbox state to 'Processed'
               └─► Failure: Updates Inbox state to 'Failed' (Retries / DLT)
```

---

## 2. Detailed Lifecycle Breakdown

### Phase A: The Publishing Path (Transactional Outbox)
This phase ensures **At-Least-Once delivery** without relying on two-phase commits.

1. **Atomic Write:** The user calls `context.PublishAsync` or maps database transactions. The system writes the business entity changes and the outbound Kafka message (payload, headers, and topic) inside the **same database transaction**.
2. **Persistence:** If the transaction succeeds, the message is stored in the `qkafka_outbox` table with `dispatched_at = NULL`. If the transaction rolls back, nothing is written.
3. **Background Dispatch:** The `OutboxPublisher` runs as a background service. It polls the database using concurrency safe queries (`SKIP LOCKED`) to fetch the oldest unpublished rows.
4. **Broker Handshake:** The publisher sends the message to the Kafka broker.
5. **Acknowledge and Mark:** Once the broker acknowledges receipt (ACK), the publisher updates the outbox table setting `dispatched_at = DateTime.UtcNow` (or deletes the row according to configuration).

---

### Phase B: The Consuming Path (Inbox & Processing Workers)
This phase ensures **Exactly-Once processing** (Idempotency) and **Rebalance Protection**.

1. **Independent Polling Loop:** The `KafkaConsumerLoop` runs on a dedicated background thread. Its sole job is to call `IConsumer.Consume()` and keep sending heartbeats to the broker. It never blocks on user code.
2. **Polymorphic Deserialization:** The loop reads the raw bytes, extracts the message type from headers, and deserializes the payload. (If deserialization fails, it commits the offset and routes to DLT as a *Poison Pill*).
3. **Inbox Verification (Idempotency Guard):**
   * Before processing, the engine queries the `qkafka_inbox` table using the `MessageId` and the `ConsumerGroup`.
   * **If `Processed`:** The message is ignored and the offset is committed (it was already processed before a crash or rebalance).
   * **If `Processing`:** The engine throws an exception (it is currently being executed elsewhere).
   * **If Not Found:** The engine registers the ID with status `Processing`.
4. **Key-Partitioned Dispatching:** The message is hashed by its `Partition Key` and enqueued into a bounded `System.Threading.Channels.Channel`.
5. **Sequential Execution:** A dedicated worker thread reads from the channel. Messages with the same key are executed sequentially in order of arrival, while messages with different keys run in parallel.
6. **Execution Scope:** The worker creates a scoped Dependency Injection container, instantiates the user's `IKafkaConsumer<T>`, and executes `ConsumeAsync`.
7. **Commit & Acknowledge:**
   * **On Success:** The worker marks the inbox status as `Processed` and registers the Kafka offset in a sliding commit window.
   * **On Failure:** The worker marks the inbox status as `Failed`. The message will be retried or routed to the DLT.

---

## 3. How qKafka Handles Failure Scenarios

### What happens if the App crashes after sending but before database update?
* **Action:** The database transaction was committed, meaning the outbox has the message.
* **Result:** When the app restarts, the `OutboxPublisher` reads the undispatched message and sends it.

### What happens if a Rebalance occurs while a consumer is running slowly?
* **Action:** In standard Kafka, this blocks the heartbeat and triggers a rebalance. In qKafka, the polling loop thread is completely decoupled and keeps sending heartbeats to the broker.
* **Result:** No rebalance is triggered. The processing worker can take its time to finish database writes safely.

### What happens if the App crashes mid-processing?
* **Action:** The database states are partially updated, but the Kafka offset was not committed. On restart, Kafka delivers the message again.
* **Result:** The `Inbox Guard` intercepts the message, sees it is not marked as `Processed`, and re-runs it safely. If it was marked as `Processed` right before the crash but the offset wasn't committed, the `Inbox Guard` skips the execution and commits the offset. No duplicate side effects occur!
