using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using QKafka.Tests.Support;

namespace QKafka.Tests;

public sealed class KafkaConsumerLoopUnitTests
{
    private const string ValidJson = "{\"Id\":\"o-1\",\"Quantity\":1}";

    private readonly RecordingDltPublisher _dlt = new();
    private readonly CancellationTokenSource _endOfScript = new();
    private readonly ChannelDispatcher _dispatcher = new(1);

    private ScriptedConsumer Consumer() => new(_endOfScript);

    private KafkaConsumerLoop Create() => new(
        new ConsumerRegistration(typeof(OrderConsumer), typeof(OrderPlaced), "test-topic", "test-group"),
        new QKafkaOptions(),
        new ServiceCollection().BuildServiceProvider(),
        _dlt,
        new JsonKafkaMessageDeserializer(),
        NullLoggerFactory.Instance);

    private Task Poll(ScriptedConsumer consumer) => Create().PollAsync(consumer, _dispatcher, _endOfScript.Token);

    private int Queued => _dispatcher.Channels[0].Reader.Count;

    private void Drain(int keep)
    {
        var reader = _dispatcher.Channels[0].Reader;
        while (reader.Count > keep)
        {
            reader.TryRead(out _);
        }
    }

    [Fact]
    public async Task ValidMessage_Polled_IsDispatchedAndNeitherRoutedToTheDltNorCommitted()
    {
        var consumer = Consumer().Returns(TestKafka.Message("k", value: ValidJson));

        await Poll(consumer);

        Assert.Equal(1, Queued);
        Assert.Empty(_dlt.Published);
        Assert.Empty(consumer.CommittedResults);
    }

    [Fact]
    public async Task PoisonPill_Polled_GoesToTheDltIsCommittedAndNeverDispatched()
    {
        var poison = TestKafka.Message("k", value: "not json");
        var consumer = Consumer().Returns(poison);

        await Poll(consumer);

        var published = Assert.Single(_dlt.Published);
        Assert.Equal(DltReason.PoisonPill, published.Reason);
        Assert.Equal(0, published.Attempts);
        Assert.NotNull(published.Error);
        Assert.Same(poison, Assert.Single(consumer.CommittedResults));
        Assert.Equal(0, Queued);
    }

    [Fact]
    public async Task DltFailureOnAPoisonPill_Polled_IsNotCommittedAndTheLoopKeepsGoing()
    {
        _dlt.FailWith = new InvalidOperationException("dlt down");
        var consumer = Consumer()
            .Returns(TestKafka.Message("k", offset: 0, value: "not json"))
            .Returns(TestKafka.Message("k", offset: 1, value: ValidJson));

        await Poll(consumer);

        Assert.Empty(consumer.CommittedResults);
        Assert.Equal(1, Queued);
    }

    [Fact]
    public async Task ConsumeException_Polled_LoopKeepsGoing()
    {
        var consumer = Consumer()
            .Throws(new ConsumeException(new ConsumeResult<byte[], byte[]>(), new Error(ErrorCode.Local_Transport)))
            .Returns(TestKafka.Message("k", value: ValidJson));

        await Poll(consumer);

        Assert.Equal(1, Queued);
    }

    [Fact]
    public async Task UnexpectedException_Polled_LoopKeepsGoing()
    {
        var consumer = Consumer()
            .Throws(new InvalidOperationException("boom"))
            .Returns(TestKafka.Message("k", value: ValidJson));

        await Poll(consumer);

        Assert.Equal(1, Queued);
    }

    [Fact]
    public async Task PollTimeout_Polled_DispatchesNothing()
    {
        var consumer = Consumer().Returns(null).Returns(null);

        await Poll(consumer);

        Assert.Equal(0, Queued);
        Assert.Empty(_dlt.Published);
    }

    [Fact]
    public async Task Cancellation_Polled_EndsTheLoopCleanlyAndClosesTheConsumer()
    {
        var consumer = Consumer().Then(() =>
        {
            _endOfScript.Cancel();
            throw new OperationCanceledException(_endOfScript.Token);
        });

        await Poll(consumer);

        Assert.Equal(1, consumer.CloseCalls);
    }

    [Fact]
    public async Task EndOfLoop_Polled_ClosesTheConsumerExactlyOnce()
    {
        var consumer = Consumer().Returns(null);

        await Poll(consumer);

        Assert.Equal(1, consumer.CloseCalls);
    }

    [Fact]
    public async Task CloseFailure_Polled_IsLoggedNotThrown()
    {
        var consumer = Consumer().Returns(null);
        consumer.CloseFailure = new InvalidOperationException("close failed");

        await Poll(consumer);

        Assert.Equal(1, consumer.CloseCalls);
    }

    [Fact]
    public async Task FullChannel_Polled_PausesOnceAtTheHighWatermark()
    {
        var consumer = Consumer();
        for (var i = 0; i < 80; i++)
        {
            consumer.Returns(TestKafka.Message("k", i, value: ValidJson));
        }

        consumer.Returns(null).Returns(null);

        await Poll(consumer);

        Assert.Equal(new[] { "pause" }, consumer.Calls);
    }

    [Fact]
    public async Task BelowTheHighWatermark_Polled_NeverPauses()
    {
        var consumer = Consumer();
        for (var i = 0; i < 79; i++)
        {
            consumer.Returns(TestKafka.Message("k", i, value: ValidJson));
        }

        consumer.Returns(null);

        await Poll(consumer);

        Assert.Empty(consumer.Calls);
    }

    [Fact]
    public async Task DrainedChannel_Polled_ResumesAfterPausing()
    {
        var consumer = Consumer();
        for (var i = 0; i < 80; i++)
        {
            consumer.Returns(TestKafka.Message("k", i, value: ValidJson));
        }

        consumer.Returns(null).Then(() =>
        {
            Drain(0);
            return null;
        }).Returns(null);

        await Poll(consumer);

        Assert.Equal(new[] { "pause", "resume" }, consumer.Calls);
    }

    [Fact]
    public async Task PartiallyDrainedChannel_Polled_StaysPausedUntilItFallsToTheLowWatermark()
    {
        var consumer = Consumer();
        for (var i = 0; i < 80; i++)
        {
            consumer.Returns(TestKafka.Message("k", i, value: ValidJson));
        }

        consumer.Returns(null);
        foreach (var level in new[] { 50, 21, 20 })
        {
            consumer.Then(() =>
            {
                Drain(level);
                consumer.Calls.Add($"drained-to-{level}");
                return null;
            }).Returns(null);
        }

        await Poll(consumer);

        Assert.Equal(new[] { "pause", "drained-to-50", "drained-to-21", "drained-to-20", "resume" }, consumer.Calls);
    }

    [Fact]
    public void NullDependencies_Constructed_ThrowArgumentNull()
    {
        var registration = new ConsumerRegistration(typeof(OrderConsumer), typeof(OrderPlaced), "t", "g");
        var options = new QKafkaOptions();
        var services = new ServiceCollection().BuildServiceProvider();
        var deserializer = new JsonKafkaMessageDeserializer();

        Assert.Throws<ArgumentNullException>(() => new KafkaConsumerLoop(null!, options, services, _dlt, deserializer, NullLoggerFactory.Instance));
        Assert.Throws<ArgumentNullException>(() => new KafkaConsumerLoop(registration, null!, services, _dlt, deserializer, NullLoggerFactory.Instance));
        Assert.Throws<ArgumentNullException>(() => new KafkaConsumerLoop(registration, options, null!, _dlt, deserializer, NullLoggerFactory.Instance));
        Assert.Throws<ArgumentNullException>(() => new KafkaConsumerLoop(registration, options, services, null!, deserializer, NullLoggerFactory.Instance));
        Assert.Throws<ArgumentNullException>(() => new KafkaConsumerLoop(registration, options, services, _dlt, null!, NullLoggerFactory.Instance));
        Assert.Throws<ArgumentNullException>(() => new KafkaConsumerLoop(registration, options, services, _dlt, deserializer, null!));
    }
}
