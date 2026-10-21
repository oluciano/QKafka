namespace QKafka.Tests.Support;

/// <summary>A discoverable consumer with an explicit group id.</summary>
[KafkaConsumer("audit", GroupId = "audit-group")]
internal sealed class AuditConsumer : IKafkaConsumer<OrderPlaced>
{
    public Task ConsumeAsync(OrderPlaced message, IKafkaContext context) => Task.CompletedTask;
}
