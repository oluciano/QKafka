using System.Text;
using System.Threading.Channels;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using QKafka.Tests.Support;

namespace QKafka.Tests;

public sealed class PartitionWorkerUnitTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

    private readonly Channel<ConsumeResult<byte[], byte[]>> _channel = Channel.CreateUnbounded<ConsumeResult<byte[], byte[]>>();
    private readonly IServiceProvider _services = new ServiceCollection().BuildServiceProvider();

    private PartitionWorker Create(Func<IKafkaContext, Task> pipeline) => new(_channel.Reader, pipeline, _services);

    [Fact]
    public async Task Message_Written_RunsThePipelineWithAContextBuiltFromIt()
    {
        var seen = new TaskCompletionSource<IKafkaContext>();
        using var sut = Create(context =>
        {
            seen.TrySetResult(context);
            return Task.CompletedTask;
        });
        var message = TestKafka.Message("order-1", offset: 42, partition: 3, value: "body");
        message.Message.Headers = new Headers { { "traceparent", Encoding.UTF8.GetBytes("00-abc") } };

        await _channel.Writer.WriteAsync(message);
        var context = await seen.Task.WaitAsync(Timeout);

        Assert.Equal(3, context.Partition);
        Assert.Equal(42, context.Offset);
        Assert.Equal("order-1", context.Key);
        Assert.Equal("body", Encoding.UTF8.GetString(context.Payload));
        Assert.Equal("00-abc", Encoding.UTF8.GetString(context.Headers["traceparent"]));
        Assert.Same(_services, context.RequestServices);
        await sut.StopAsync();
    }

    [Fact]
    public async Task MessageWithoutKeyOrHeaders_Written_GetsAnEmptyKeyAndNoHeaders()
    {
        var seen = new TaskCompletionSource<IKafkaContext>();
        using var sut = Create(context =>
        {
            seen.TrySetResult(context);
            return Task.CompletedTask;
        });

        await _channel.Writer.WriteAsync(TestKafka.Message(null));
        var context = await seen.Task.WaitAsync(Timeout);

        Assert.Equal(string.Empty, context.Key);
        Assert.Empty(context.Headers);
        await sut.StopAsync();
    }

    [Fact]
    public async Task MessageWithNullValue_Written_GetsAnEmptyPayload()
    {
        var seen = new TaskCompletionSource<IKafkaContext>();
        using var sut = Create(context =>
        {
            seen.TrySetResult(context);
            return Task.CompletedTask;
        });
        var tombstone = TestKafka.Message("k");
        tombstone.Message.Value = null!;

        await _channel.Writer.WriteAsync(tombstone);

        Assert.Empty((await seen.Task.WaitAsync(Timeout)).Payload);
        await sut.StopAsync();
    }

    [Fact]
    public async Task Messages_Written_AreExecutedOneAtATimeInOrder()
    {
        var order = new List<long>();
        var running = 0;
        var overlapped = false;
        var done = new TaskCompletionSource();
        using var sut = Create(async context =>
        {
            if (Interlocked.Increment(ref running) > 1)
            {
                overlapped = true;
            }

            await Task.Delay(20);
            order.Add(context.Offset);
            Interlocked.Decrement(ref running);
            if (order.Count == 3)
            {
                done.TrySetResult();
            }
        });

        for (var offset = 0; offset < 3; offset++)
        {
            await _channel.Writer.WriteAsync(TestKafka.Message("k", offset));
        }

        await done.Task.WaitAsync(Timeout);

        Assert.Equal(new long[] { 0, 1, 2 }, order);
        Assert.False(overlapped);
        await sut.StopAsync();
    }

    [Fact]
    public async Task PipelineThrows_Written_WorkerSurvivesAndProcessesTheNextMessage()
    {
        var second = new TaskCompletionSource();
        using var sut = Create(context =>
        {
            if (context.Offset == 0)
            {
                throw new InvalidOperationException("boom");
            }

            second.TrySetResult();
            return Task.CompletedTask;
        });

        await _channel.Writer.WriteAsync(TestKafka.Message("k", 0));
        await _channel.Writer.WriteAsync(TestKafka.Message("k", 1));

        await second.Task.WaitAsync(Timeout);

        Assert.True(second.Task.IsCompletedSuccessfully);
        await sut.StopAsync();
    }

    [Fact]
    public async Task IdleWorker_Stopped_Completes()
    {
        using var sut = Create(_ => Task.CompletedTask);

        Assert.Null(await Record.ExceptionAsync(() => sut.StopAsync().WaitAsync(Timeout)));
    }

    [Fact]
    public async Task Worker_Stopped_NoLongerProcessesMessages()
    {
        var calls = 0;
        using var sut = Create(_ =>
        {
            Interlocked.Increment(ref calls);
            return Task.CompletedTask;
        });

        await sut.StopAsync();
        await _channel.Writer.WriteAsync(TestKafka.Message("k"));
        await Task.Delay(100);

        Assert.Equal(0, calls);
    }

    [Fact]
    public void Worker_DisposedTwice_DoesNotThrow()
    {
        var sut = Create(_ => Task.CompletedTask);

        sut.Dispose();

        Assert.Null(Record.Exception(sut.Dispose));
    }

    [Fact]
    public void NullDependencies_Constructed_ThrowArgumentNull()
    {
        Assert.Throws<ArgumentNullException>(() => new PartitionWorker(null!, _ => Task.CompletedTask, _services));
        Assert.Throws<ArgumentNullException>(() => new PartitionWorker(_channel.Reader, null!, _services));
        Assert.Throws<ArgumentNullException>(() => new PartitionWorker(_channel.Reader, _ => Task.CompletedTask, null!));
    }
}
