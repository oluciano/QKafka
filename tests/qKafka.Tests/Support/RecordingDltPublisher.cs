namespace QKafka.Tests.Support;

/// <summary>An <see cref="IDltPublisher"/> that records what was routed to the Dead Letter Topic.</summary>
internal sealed class RecordingDltPublisher : IDltPublisher
{
    public List<(DltReason Reason, int Attempts, Exception? Error)> Published { get; } = new();

    public Exception? FailWith { get; set; }

    public Task PublishAsync(
        Confluent.Kafka.ConsumeResult<byte[], byte[]> original,
        DltReason reason,
        Exception? exception,
        int attemptCount,
        CancellationToken cancellationToken)
    {
        if (FailWith is not null)
        {
            return Task.FromException(FailWith);
        }

        Published.Add((reason, attemptCount, exception));
        return Task.CompletedTask;
    }
}
