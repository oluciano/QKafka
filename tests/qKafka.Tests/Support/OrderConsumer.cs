namespace QKafka.Tests.Support;

/// <summary>A discoverable consumer with the default group id.</summary>
[KafkaConsumer("orders")]
internal sealed class OrderConsumer : IKafkaConsumer<OrderPlaced>
{
    public Task ConsumeAsync(OrderPlaced message, IKafkaContext context) => Task.CompletedTask;
}
