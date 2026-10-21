using System.Text;
using Confluent.Kafka;
using QKafka.Tests.Support;

namespace QKafka.Tests;

public sealed class ChannelDispatcherUnitTests
{
    [Fact]
    public async Task SameKey_ManyMessages_LandInOneChannelInOrder()
    {
        var dispatcher = new ChannelDispatcher(4);

        for (var offset = 0; offset < 50; offset++)
        {
            await dispatcher.DispatchAsync(TestKafka.Message("order-1", offset), CancellationToken.None);
        }

        var filled = dispatcher.Channels.Where(c => c.Reader.Count > 0).ToList();
        var channel = Assert.Single(filled);
        Assert.Equal(50, channel.Reader.Count);
        for (var expected = 0; expected < 50; expected++)
        {
            Assert.True(channel.Reader.TryRead(out var item));
            Assert.Equal(expected, item.Offset.Value);
        }
    }

    [Fact]
    public async Task ManyDifferentKeys_Dispatched_SpreadAcrossChannels()
    {
        var dispatcher = new ChannelDispatcher(4);

        for (var i = 0; i < 200; i++)
        {
            await dispatcher.DispatchAsync(TestKafka.Message($"key-{i}", i), CancellationToken.None);
        }

        Assert.True(dispatcher.Channels.Count(c => c.Reader.Count > 0) > 1);
        Assert.Equal(200, dispatcher.Channels.Sum(c => c.Reader.Count));
    }

    [Fact]
    public async Task SameKey_TwoDispatcherInstances_PickTheSameBucket()
    {
        var first = new ChannelDispatcher(8);
        var second = new ChannelDispatcher(8);

        await first.DispatchAsync(TestKafka.Message("customer-42"), CancellationToken.None);
        await second.DispatchAsync(TestKafka.Message("customer-42"), CancellationToken.None);

        Assert.Equal(
            IndexOfFilledChannel(first),
            IndexOfFilledChannel(second));
    }

    [Fact]
    public void ConcurrencyLimit_Configured_CreatesThatManyChannels()
    {
        var dispatcher = new ChannelDispatcher(6);

        Assert.Equal(6, dispatcher.Channels.Count);
    }

    [Fact]
    public async Task FullChannel_NextDispatch_WaitsUntilTheReaderDrains()
    {
        var dispatcher = new ChannelDispatcher(1);
        for (var i = 0; i < 100; i++)
        {
            await dispatcher.DispatchAsync(TestKafka.Message("k", i), CancellationToken.None);
        }

        var blocked = dispatcher.DispatchAsync(TestKafka.Message("k", 100), CancellationToken.None);

        var winner = await Task.WhenAny(blocked, Task.Delay(100));
        Assert.NotSame(blocked, winner);

        Assert.True(dispatcher.Channels[0].Reader.TryRead(out _));
        await blocked.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(100, dispatcher.Channels[0].Reader.Count);
    }

    [Fact]
    public async Task FullChannel_CancelledWhileWaiting_ThrowsOperationCanceled()
    {
        var dispatcher = new ChannelDispatcher(1);
        for (var i = 0; i < 100; i++)
        {
            await dispatcher.DispatchAsync(TestKafka.Message("k", i), CancellationToken.None);
        }

        using var cts = new CancellationTokenSource();
        var blocked = dispatcher.DispatchAsync(TestKafka.Message("k", 100), cts.Token);
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => blocked);
    }

    [Fact]
    public async Task NullResult_Dispatched_ThrowsArgumentNull()
    {
        var dispatcher = new ChannelDispatcher(2);

        await Assert.ThrowsAsync<ArgumentNullException>(() => dispatcher.DispatchAsync(null!, CancellationToken.None));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void NonPositiveConcurrency_Created_FallsBackToProcessorCount(int limit)
    {
        var dispatcher = new ChannelDispatcher(limit);

        Assert.Equal(Environment.ProcessorCount, dispatcher.Channels.Count);
    }

    [Fact]
    public async Task NullKey_Dispatched_StillDeliversTheMessage()
    {
        var dispatcher = new ChannelDispatcher(4);

        await dispatcher.DispatchAsync(TestKafka.Message(null), CancellationToken.None);

        Assert.Equal(1, dispatcher.Channels.Sum(c => c.Reader.Count));
    }

    [Fact]
    public async Task EmptyKey_DispatchedTwice_GoesToTheSameChannel()
    {
        var dispatcher = new ChannelDispatcher(8);
        var empty = new ConsumeResult<byte[], byte[]>
        {
            Message = new Message<byte[], byte[]> { Key = Encoding.UTF8.GetBytes(string.Empty), Value = Array.Empty<byte>() },
        };

        await dispatcher.DispatchAsync(empty, CancellationToken.None);
        await dispatcher.DispatchAsync(empty, CancellationToken.None);

        Assert.Equal(2, dispatcher.Channels.Max(c => c.Reader.Count));
    }

    private static int IndexOfFilledChannel(ChannelDispatcher dispatcher)
    {
        for (var i = 0; i < dispatcher.Channels.Count; i++)
        {
            if (dispatcher.Channels[i].Reader.Count > 0)
            {
                return i;
            }
        }

        return -1;
    }
}
