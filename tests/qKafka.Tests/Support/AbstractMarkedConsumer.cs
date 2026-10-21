namespace QKafka.Tests.Support;

/// <summary>Abstract and marked: it must not be discovered because it cannot be instantiated.</summary>
[KafkaConsumer("abstract-topic")]
internal abstract class AbstractMarkedConsumer : IKafkaConsumer<OrderPlaced>
{
    public abstract Task ConsumeAsync(OrderPlaced message, IKafkaContext context);
}
