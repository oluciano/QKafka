namespace QKafka.Tests.Support;

/// <summary>A consumer that records what it received. It has no [KafkaConsumer] so assembly scans ignore it.</summary>
internal sealed class RecordingConsumer : IKafkaConsumer<OrderPlaced>
{
    public List<OrderPlaced> Received { get; } = new();

    public Exception? FailWith { get; set; }

    public Task ConsumeAsync(OrderPlaced message, IKafkaContext context)
    {
        Received.Add(message);
        return FailWith is null ? Task.CompletedTask : Task.FromException(FailWith);
    }
}
