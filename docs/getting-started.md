# Step-by-Step Usage Guide: Implementing qKafka

This guide demonstrates how to integrate and use **qKafka** in a real-world .NET 8+ Web API project, covering everything from initial installation to writing consumers, publishing via Outbox, orchestrating workflows with Sagas, and writing unit tests.

---

## Step 1: Install NuGet Packages

First, add the required packages to your projects.

```bash
# Add to your API / Worker Project
dotnet add package qKafka
dotnet add package qKafka.Outbox
dotnet add package qKafka.Sagas

# Add to your Test Project
dotnet add package qKafka.Testing
```

---

## Step 2: Configure DbContext (Outbox & Saga State)

To support transactional integrity, map the Outbox, Inbox, and Saga state tables inside your Entity Framework Core `DbContext`.

```csharp
using Microsoft.EntityFrameworkCore;
using QKafka.Outbox;
using QKafka.Sagas;

public sealed class AppDbContext : DbContext
{
    public AppDbContext(DbContextOptions<AppDbContext> options) : base(options) { }

    // Your Business Entities
    public DbSet<Order> Orders => Set<Order>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        // Apply qKafka schema configurations automatically
        modelBuilder.ApplyqKafkaConfigurations();
        
        // Optional: Map your custom Saga State entities if they need specific db configurations
        modelBuilder.Entity<OrderSagaState>(entity =>
        {
            entity.HasKey(s => s.CorrelationId);
            entity.Property(s => s.CustomerName).HasMaxLength(150);
        });
    }
}
```

---

## Step 3: Configure `appsettings.json`

Keep your configuration simple and clean. Define connection details globally once.

```json
{
  "ConnectionStrings": {
    "Database": "Host=localhost;Database=orders_db;Username=postgres;Password=postgres"
  },
  "qKafka": {
    "BootstrapServers": "localhost:9092",
    "Consumers": {
      "billing-service-group": {
        "ConcurrencyLimit": 4,
        "AutoOffsetReset": "Earliest"
      }
    }
  }
}
```

---

## Step 4: Wire Up Dependency Injection in `Program.cs`

Register the database, fluent qKafka pipelines, database-backed outbox/sagas, and scan for consumers in your assembly.

```csharp
using Microsoft.EntityFrameworkCore;
using QKafka;
using QKafka.Outbox;
using QKafka.Sagas;
using QKafka.Dashboard;

var builder = WebApplication.CreateBuilder(args);

// 1. Database Configuration
builder.Services.AddDbContext<AppDbContext>(options =>
    options.UseNpgsql(
        builder.Configuration.GetConnectionString("Database"),
        b => b.MigrationsAssembly(typeof(AppDbContext).Assembly.FullName)
    ));

// 2. qKafka Registration
builder.Services.AddqKafka()
    .AddEntityFrameworkOutbox<AppDbContext>()
    .AddEntityFrameworkSagas<AppDbContext>(typeof(Program).Assembly)
    .AddConsumers(typeof(Program).Assembly);

var app = builder.Build();

// 3. Web Dashboard (Optional)
app.UseqKafkaDashboard("/qkafka");

await app.RunAsync();
```

---

## Step 5: Define Message Contracts

Define messages using clean C# `record` types.

```csharp
// Started Event
public sealed record OrderPlaced(Guid OrderId, string CustomerName, decimal Total);

// Subsequent correlation Event
public sealed record PaymentReceived(Guid OrderId, decimal Amount, string TransactionId);

// Error/Compensation triggers
public sealed record CancelOrder(Guid OrderId);
public sealed record RefundPayment(Guid OrderId, decimal Amount);
```

---

## Step 6: Create a Standard Message Consumer

Implement `IKafkaConsumer<T>` and decorate it with `[KafkaConsumer]` to configure consumer groups and topic subscriptions.

```csharp
using QKafka;

[KafkaConsumer("orders-topic", GroupId = "order-audit-group")]
public sealed class OrderAuditConsumer : IKafkaConsumer<OrderPlaced>
{
    private readonly ILogger<OrderAuditConsumer> _logger;

    public OrderAuditConsumer(ILogger<OrderAuditConsumer> logger)
    {
        _logger = logger;
    }

    public async Task ConsumeAsync(OrderPlaced message, IKafkaContext context)
    {
        _logger.LogInformation("Auditing placed order {OrderId} for {Customer}", 
            message.OrderId, message.CustomerName);
            
        await Task.CompletedTask;
    }
}
```

---

## Step 7: Create a Stateful Saga with Auto-Compensation

Write a workflow that manages state across asynchronous events. Map correlation keys explicitly using the `ICorrelateSaga<T>` interface, and register dynamic compensations inline.

```csharp
using QKafka.Sagas;

public sealed class OrderSagaState
{
    public string CustomerName { get; set; } = string.Empty;
    public decimal Total { get; set; }
    public bool IsPaid { get; set; }
}

public sealed class OrderWorkflowSaga : Saga<OrderSagaState>,
    IStartSaga<OrderPlaced>,
    IHandleSaga<PaymentReceived>,
    ICorrelateSaga<PaymentReceived>
{
    // 1. Explicit correlation mapper (Zero-DSL)
    public Guid GetCorrelationId(PaymentReceived message) => message.OrderId;

    // 2. Initiate Workflow Step
    public async Task HandleAsync(OrderPlaced message, ISagaContext context)
    {
        CorrelationId = message.OrderId;
        State.CustomerName = message.CustomerName;
        State.Total = message.Total;

        // Register reverse compensation in case subsequent steps fail
        context.RegisterCompensation(new CancelOrder(CorrelationId));

        // Enqueue payment trigger message via Outbox
        await context.PublishAsync(new ProcessPayment(CorrelationId, message.Total));
    }

    // 3. Subsequent Workflow Step
    public async Task HandleAsync(PaymentReceived message, ISagaContext context)
    {
        State.IsPaid = true;

        // Register dynamic refund compensation
        context.RegisterCompensation(new RefundPayment(CorrelationId, message.Amount));

        // Enqueue next process (e.g. shipping)
        await context.PublishAsync(new PrepareShipment(CorrelationId));
    }
}
```

---

## Step 8: Publish Messages Atomically (Outbox Pattern)

Publish messages from your Web API controller inside the application's entity transaction block.

```csharp
using Microsoft.AspNetCore.Mvc;
using QKafka;

[ApiController]
[Route("api/orders")]
public sealed class OrderController : ControllerBase
{
    private readonly AppDbContext _db;
    private readonly IKafkaContext _kafka;

    public OrderController(AppDbContext db, IKafkaContext kafka)
    {
        _db = db;
        _kafka = kafka;
    }

    [HttpPost]
    public async Task<IActionResult> CreateOrder([FromBody] CreateOrderRequest request)
    {
        var orderId = Guid.NewGuid();
        var order = new Order(orderId, request.CustomerName, request.Total);

        // Open Transaction
        await using var transaction = await _db.Database.BeginTransactionAsync();
        try
        {
            // 1. Save business entity
            _db.Orders.Add(order);
            await _db.SaveChangesAsync();

            // 2. Enqueue event to database-backed Outbox
            await _kafka.PublishAsync(new OrderPlaced(orderId, order.CustomerName, order.Total));

            // Commit Transaction atomically
            await transaction.CommitAsync();
            
            return Ok(new { OrderId = orderId });
        }
        catch (Exception)
        {
            await transaction.RollbackAsync();
            throw;
        }
    }
}
```

---

## Step 9: Unit Test Everything in Memory (Fast & Reliable)

Unit test your consumers, database modifications, and sagas in milliseconds without running a Docker broker.

```csharp
using Xunit;
using QKafka.Testing;

public sealed class OrderWorkflowTests
{
    [Fact]
    public async Task Should_Start_Saga_And_Commit_Outbox_On_OrderPlaced()
    {
        // 1. Arrange: Setup In-Memory Test Harness
        await using var harness = new InMemoryKafkaTestHarness();
        harness.ConfigureServices(services =>
        {
            services.AddqKafka()
                .AddInMemoryOutbox()
                .AddInMemorySagas(typeof(OrderWorkflowSaga).Assembly)
                .AddConsumers(typeof(OrderWorkflowSaga).Assembly);
        });

        await harness.StartAsync();
        var orderId = Guid.NewGuid();

        // 2. Act: Publish the start event
        await harness.PublishAsync(new OrderPlaced(orderId, "Alice", 150.00m));

        // 3. Assert: Verify consumer consumption and outbox emission
        await harness.Consumed.AssertConsumed<OrderPlaced>(
            msg => msg.OrderId == orderId, 
            TimeSpan.FromSeconds(2));

        // Assert that the Saga started and dispatched the next command to Outbox
        await harness.Published.AssertPublished<ProcessPayment>(
            msg => msg.OrderId == orderId && msg.Amount == 150.00m,
            TimeSpan.FromSeconds(2));
    }
}
```
