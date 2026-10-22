using Confluent.Kafka;

namespace QKafka.Tests.Support;

/// <summary>An <see cref="IProducer{TKey,TValue}"/> that records produced messages instead of talking to a broker.</summary>
internal sealed class RecordingProducer : IProducer<byte[], byte[]>
{
    public List<(string Topic, Message<byte[], byte[]> Message)> Produced { get; } = new();

    /// <summary>When set, <see cref="ProduceAsync(string, Message{byte[], byte[]}, CancellationToken)"/> throws it.</summary>
    public Exception? Failure { get; set; }

    public string Name => "recording-producer";

    public Handle Handle => throw new NotSupportedException();

    public int AddBrokers(string brokers) => throw new NotSupportedException();

    public void SetSaslCredentials(string username, string password) => throw new NotSupportedException();

    public Task<DeliveryResult<byte[], byte[]>> ProduceAsync(string topic, Message<byte[], byte[]> message, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (Failure is not null)
        {
            return Task.FromException<DeliveryResult<byte[], byte[]>>(Failure);
        }

        Produced.Add((topic, message));
        return Task.FromResult(new DeliveryResult<byte[], byte[]> { Topic = topic, Message = message });
    }

    public Task<DeliveryResult<byte[], byte[]>> ProduceAsync(TopicPartition topicPartition, Message<byte[], byte[]> message, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();

    public void Produce(string topic, Message<byte[], byte[]> message, Action<DeliveryReport<byte[], byte[]>>? deliveryHandler = null) =>
        throw new NotSupportedException();

    public void Produce(TopicPartition topicPartition, Message<byte[], byte[]> message, Action<DeliveryReport<byte[], byte[]>>? deliveryHandler = null) =>
        throw new NotSupportedException();

    public int Poll(TimeSpan timeout) => 0;

    public int Flush(TimeSpan timeout) => 0;

    public void Flush(CancellationToken cancellationToken = default)
    {
    }

    public void InitTransactions(TimeSpan timeout) => throw new NotSupportedException();

    public void BeginTransaction() => throw new NotSupportedException();

    public void CommitTransaction(TimeSpan timeout) => throw new NotSupportedException();

    public void CommitTransaction() => throw new NotSupportedException();

    public void AbortTransaction(TimeSpan timeout) => throw new NotSupportedException();

    public void AbortTransaction() => throw new NotSupportedException();

    public void SendOffsetsToTransaction(IEnumerable<TopicPartitionOffset> offsets, IConsumerGroupMetadata groupMetadata, TimeSpan timeout) =>
        throw new NotSupportedException();

    public void Dispose()
    {
    }
}
