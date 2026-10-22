using Confluent.Kafka;

namespace QKafka.Tests.Support;

/// <summary>
/// An <see cref="IConsumer{TKey,TValue}"/> that plays back a script of poll results and records Pause, Resume, Commit and Close.
/// When the script runs out it cancels the supplied token, which ends the consumer loop under test.
/// </summary>
internal sealed class ScriptedConsumer : IConsumer<byte[], byte[]>
{
    private readonly Queue<Func<ConsumeResult<byte[], byte[]>?>> _steps = new();
    private readonly CancellationTokenSource _endOfScript;

    public ScriptedConsumer(CancellationTokenSource endOfScript)
    {
        _endOfScript = endOfScript;
    }

    public List<ConsumeResult<byte[], byte[]>> CommittedResults { get; } = new();

    public List<string> Calls { get; } = new();

    public int CloseCalls { get; private set; }

    public Exception? CloseFailure { get; set; }

    public string Name => "scripted-consumer";

    public Handle Handle => throw new NotSupportedException();

    public string MemberId => "member";

    public List<TopicPartition> Assignment { get; } = new() { new TopicPartition("test-topic", new Partition(0)) };

    public List<string> Subscription => new() { "test-topic" };

    public IConsumerGroupMetadata ConsumerGroupMetadata => throw new NotSupportedException();

    /// <summary>Queues a poll that returns <paramref name="result"/> (null means the poll timed out).</summary>
    public ScriptedConsumer Returns(ConsumeResult<byte[], byte[]>? result) => Then(() => result);

    /// <summary>Queues a poll that runs <paramref name="step"/> and returns its result.</summary>
    public ScriptedConsumer Then(Func<ConsumeResult<byte[], byte[]>?> step)
    {
        _steps.Enqueue(step);
        return this;
    }

    /// <summary>Queues a poll that throws <paramref name="exception"/>.</summary>
    public ScriptedConsumer Throws(Exception exception) => Then(() => throw exception);

    public ConsumeResult<byte[], byte[]>? Consume(TimeSpan timeout)
    {
        if (_steps.Count == 0)
        {
            _endOfScript.Cancel();
            return null;
        }

        return _steps.Dequeue()();
    }

    public ConsumeResult<byte[], byte[]> Consume(int millisecondsTimeout) => throw new NotSupportedException();

    public ConsumeResult<byte[], byte[]> Consume(CancellationToken cancellationToken = default) => throw new NotSupportedException();

    public void Pause(IEnumerable<TopicPartition> partitions) => Calls.Add("pause");

    public void Resume(IEnumerable<TopicPartition> partitions) => Calls.Add("resume");

    public void Commit(ConsumeResult<byte[], byte[]> result) => CommittedResults.Add(result);

    public List<TopicPartitionOffset> Commit() => throw new NotSupportedException();

    public void Commit(IEnumerable<TopicPartitionOffset> offsets) => throw new NotSupportedException();

    public void Close()
    {
        CloseCalls++;
        if (CloseFailure is not null)
        {
            throw CloseFailure;
        }
    }

    public void Dispose()
    {
    }

    public int AddBrokers(string brokers) => throw new NotSupportedException();

    public void SetSaslCredentials(string username, string password) => throw new NotSupportedException();

    public void Subscribe(IEnumerable<string> topics) => throw new NotSupportedException();

    public void Subscribe(string topic) => throw new NotSupportedException();

    public void Unsubscribe() => throw new NotSupportedException();

    public void Assign(TopicPartition partition) => throw new NotSupportedException();

    public void Assign(TopicPartitionOffset partition) => throw new NotSupportedException();

    public void Assign(IEnumerable<TopicPartitionOffset> partitions) => throw new NotSupportedException();

    public void Assign(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();

    public void IncrementalAssign(IEnumerable<TopicPartitionOffset> partitions) => throw new NotSupportedException();

    public void IncrementalAssign(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();

    public void IncrementalUnassign(IEnumerable<TopicPartition> partitions) => throw new NotSupportedException();

    public void Unassign() => throw new NotSupportedException();

    public void StoreOffset(ConsumeResult<byte[], byte[]> result) => throw new NotSupportedException();

    public void StoreOffset(TopicPartitionOffset offset) => throw new NotSupportedException();

    public void Seek(TopicPartitionOffset tpo) => throw new NotSupportedException();

    public List<TopicPartitionOffset> Committed(TimeSpan timeout) => throw new NotSupportedException();

    public List<TopicPartitionOffset> Committed(IEnumerable<TopicPartition> partitions, TimeSpan timeout) => throw new NotSupportedException();

    public Offset Position(TopicPartition partition) => throw new NotSupportedException();

    public List<TopicPartitionOffset> OffsetsForTimes(IEnumerable<TopicPartitionTimestamp> timestampsToSearch, TimeSpan timeout) =>
        throw new NotSupportedException();

    public WatermarkOffsets GetWatermarkOffsets(TopicPartition topicPartition) => throw new NotSupportedException();

    public WatermarkOffsets QueryWatermarkOffsets(TopicPartition topicPartition, TimeSpan timeout) => throw new NotSupportedException();
}
