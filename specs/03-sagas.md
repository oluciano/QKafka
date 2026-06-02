# Specification 03: Saga State Machines & Compensation

This specification defines the architecture, correlation router, and atomic persistence mechanics for the **qKafka** Saga engine (Fase 3).

---

## 1. Architectural Design

Sagas manage long-running workflows and distributed transactions. They maintain state across multiple asynchronous events.

```
                  ┌──────────────────────────────────────────┐
                  │              Kafka Consumer              │
                  └────────────────────┬─────────────────────┘
                                       │
                                       ▼
                       ┌──────────────────────────────┐
                       │     Correlation Resolver     │
                       └──────────────┬───────────────┘
                                      │
                                      ▼
                       ┌──────────────────────────────┐
                       │     Saga State Repository    │
                       └──────────────┬───────────────┘
                                      │
                                      ▼
                       ┌──────────────────────────────┐
                       │      Saga Execution Loop     │
                       └──────────────┬───────────────┘
                                      │
                       ┌──────────────┴──────────────┐
                       ▼                             ▼
         [ Entity & State Mutations ]       [ Outbox Commands ]
                       │                             │
                       └──────────────┬──────────────┘
                                      ▼ (Single Database Transaction)
                         [ Persistent SQL Database ]
```

---

## 2. Invariants & Mechanics

## 2. Invariants & Mechanics

### Saga Usability & Correlation (Solving New User Friction)
Sagas in other frameworks are hard to learn because they force users into complex State Machine DSLs and manual registration of correlation keys. qKafka solves this with two simple models:

1. **Convention-Based Correlation (Zero-Configuration):**
   * If the message has a property named `CorrelationId` or `[CorrelationId]` attribute, the engine uses it automatically.
   * If the message has a property named `<SagaName>Id` (e.g., `OrderSagaId`), the engine correlates to it automatically.
2. **Explicit Correlation via C# Interface (`ICorrelateSaga<TMessage>`):**
   * Instead of a complex startup DSL, the Saga class itself implements `ICorrelateSaga<TMessage>` to extract the ID:
   ```csharp
   public interface ICorrelateSaga<in TMessage> where TMessage : class
   {
       Guid GetCorrelationId(TMessage message);
   }
   ```

Example of a clean, readable Saga:
```csharp
public sealed class OrderSaga : Saga<OrderSagaState>,
    IStartSaga<OrderPlaced>,
    IHandleSaga<PaymentReceived>,
    ICorrelateSaga<PaymentReceived>
{
    // Correlation logic - plain C#
    public Guid GetCorrelationId(PaymentReceived message) => message.OrderId;

    public async Task HandleAsync(OrderPlaced message, ISagaContext context)
    {
        State.Customer = message.CustomerName;
        State.Total = message.Total;
        
        // Enqueue next step via Outbox
        await context.PublishAsync(new ProcessPayment(CorrelationId, message.Total));
        
        // Register compensation dynamic rollback in case of future steps failing
        context.RegisterCompensation(new CancelOrder(CorrelationId));
    }

    public async Task HandleAsync(PaymentReceived message, ISagaContext context)
    {
        State.IsPaid = true;
        
        // Register compensation dynamic rollback
        context.RegisterCompensation(new RefundPayment(CorrelationId, message.Amount));
        
        await context.PublishAsync(new ShipOrder(CorrelationId));
    }
}
```

### Flow Control: No Magic DSL
Instead of configuring state transitions through an abstract builder DSL (e.g. `When(Event).TransitionTo(State)`), the developer writes standard C# control flow. This means:
* You can put standard IDE breakpoints inside handlers.
* You can write standard `if/else` or `switch` blocks.
* Unit testing is simplified since you can just instantiate the class and mock the context.

### Dynamic Compensation Registration
Users can register compensating actions dynamically during the normal execution flow via `context.RegisterCompensation(message)`. 
* The engine records these compensations in the Saga State database row.
* If a subsequent handler throws an exception or calls `context.Fail()`, the Saga engine automatically fetches all registered compensations for that Saga instance and publishes them to the Outbox in **reverse execution order** (LIFO - Last In, First Out).

### Atomic Commitment (Non-Negotiable)
The state of the Saga (including the list of registered compensations) and any outgoing commands generated during execution (written to the `Outbox` table) **must be saved in the same database transaction**.
If the transaction fails, the Saga state is rolled back and no commands or compensations are persisted.


---

## 3. Contracts & Core Classes

### Option A: Handler-Based Saga (Implicit/Classic Style)
Ideal for developers who prefer standard C# code, breakpoints, and simple testing.

```csharp
public abstract class Saga<TSagaState>
    where TSagaState : class, new()
{
    public Guid CorrelationId { get; set; }
    public TSagaState State { get; set; } = new();
}
```

### Option B: Fluent DSL State Machine (Explicit/MassTransit-Style)
Ideal for developers migrating existing complex Automatonymous state machines with minimal code modifications.

```csharp
public abstract class KafkaStateMachine<TSagaState>
    where TSagaState : class, new()
{
    public Guid CorrelationId { get; set; }
    public TSagaState State { get; set; } = new();

    protected void InstanceState(Expression<Func<TSagaState, string>> statePropertyExpression) { /* ... */ }
    
    protected void Correlate<TEvent>(Event<TEvent> @event, Func<TEvent, Guid> correlationExpression) where TEvent : class { /* ... */ }

    protected void Initially(params EventActivityBinder<TSagaState>[] binders) { /* ... */ }
    protected void During(State state, params EventActivityBinder<TSagaState>[] binders) { /* ... */ }

    protected EventActivityBinder<TSagaState> When<TEvent>(Event<TEvent> @event) where TEvent : class => new EventActivityBinder<TSagaState>(@event);
}

public sealed class State
{
    public string Name { get; }
    public State(string name) => Name = name;
}

public sealed class Event<TMessage> where TMessage : class
{
    public string Name { get; }
    public Event(string name) => Name = name;
}
```

### `ISagaRepository`
Handles load and save with concurrency control (e.g. optimistic concurrency via a `Version` property). Applies to both Handler-based and DSL-based Sagas under the hood.
```csharp
public interface ISagaRepository<TSagaState>
    where TSagaState : class, new()
{
    Task<SagaInstance<TSagaState>?> GetAsync(Guid correlationId, CancellationToken ct);
    Task SaveAsync(SagaInstance<TSagaState> instance, DbTransaction transaction, CancellationToken ct);
}
```


## 3.5 Saga Discovery & Routing

To automatically map incoming Kafka messages to Sagas and correlate them to specific Saga instances, the engine employs a two-phase discovery and resolution process.

### 1. Saga Discovery (Reflection & DI Startup scanning)
During application startup (within the `AddEntityFrameworkSagas` or `AddInMemorySagas` extensions), the registration engine scans assemblies using reflection:
* **Starting Messages:** It looks for classes implementing `IStartSaga<TMessage>` (or `Initially(When(Event<TMessage>))` in the fluent DSL). The type pair `(TMessage, TSagaType)` is registered in a routing table as a **Saga Starter**.
* **Subsequent Messages:** It looks for classes implementing `IHandleSaga<TMessage>` (or `During(State, When(Event<TMessage>))` in the fluent DSL). The type pair `(TMessage, TSagaType)` is registered as a **Saga Handler**.

On message consumption, the engine queries this routing table to identify if the incoming message is registered to trigger or be handled by any Saga.

### 2. Correlation Resolution
When an incoming message matches a registered Saga type, the engine resolves the target `CorrelationId` in the following strict order of precedence:
1. **Explicit Correlation (`ICorrelateSaga<TMessage>`):** The engine checks if the Saga class implements `ICorrelateSaga<TMessage>` (or registered via the fluent `Correlate(Event, Expression)` mapping). If so, it invokes the correlation logic (e.g., `msg => msg.OrderId`).
2. **Implicit Property Matching (`CorrelationId`):** The engine inspects the message payload using cached reflection/compiled expressions for a property named exactly `CorrelationId` (or decorated with a `[CorrelationId]` attribute).
3. **Convention-Based Property Matching (`<SagaName>Id`):** The engine looks for a property matching the pattern `<SagaName>Id` (e.g., `OrderSagaId` if the saga class is `OrderSaga`).

### 3. Handling Unresolved Correlation
If a message is routed to a Saga but the engine **cannot extract a valid CorrelationId** (e.g., all resolution strategies return `null` or `Guid.Empty`):
* The engine **must NOT throw an unhandled exception** that crashes the Partition Worker.
* It logs a high-severity warning detailing the correlation failure, message type, and offset.
* It routes the raw message payload to the Dead Letter Topic (DLT) matching the format: `<original-topic>.dlt`.
* It commits the Kafka offset immediately to allow consumption of subsequent messages to proceed.

---


## 4. Testing Matrix (3N Mandatory Coverage)

### N1 — Positive:
* Initiate a Saga with a message: Assert that a database record is created in the Saga table.
* Send subsequent correlation messages: Assert the state is updated correctly and the workflow reaches `Completed`.
* Confirm Saga completion deletes or archives the record as configured.

### N2 — Negative:
* Concurrency conflict (two events for the same Saga instance arrive simultaneously): Assert that optimistic concurrency locking throws an exception, prompting the `PartitionWorker` to retry processing the event.
* Middleware execution failure: Trigger an exception on Step 3 of a Saga. Verify that database state mutations are aborted and compensation events are dispatched.

### N3 — Invalid Input:
* Incoming event maps to a null or empty `CorrelationId`: Log validation failure, bypass the Saga workflow, and route the message to the Dead Letter Topic.
* Saga state stored in database is corrupted: Fail processing, route event to DLT, and preserve database record for manual analysis.
