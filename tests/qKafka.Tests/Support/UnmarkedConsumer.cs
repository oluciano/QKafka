namespace QKafka.Tests.Support;

/// <summary>Implements the consumer interface but has no [KafkaConsumer] attribute, so it must not be discovered.</summary>
internal sealed class UnmarkedConsumer : IKafkaConsumer<OrderPlaced>
{
    public Task ConsumeAsync(OrderPlaced message, IKafkaContext context) => Task.CompletedTask;
}
