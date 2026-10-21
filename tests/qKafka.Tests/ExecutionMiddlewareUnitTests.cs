using System.Text;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using QKafka.Tests.Support;

namespace QKafka.Tests;

public sealed class ExecutionMiddlewareUnitTests
{
    private static KafkaContext ContextWith(string json) =>
        TestKafka.Context(TestKafka.Message("k", value: json));

    private static (ExecutionMiddleware<RecordingConsumer, OrderPlaced> Sut, RecordingConsumer Consumer) Create(IKafkaMessageDeserializer? deserializer = null)
    {
        var consumer = new RecordingConsumer();
        var provider = new ServiceCollection().AddSingleton(consumer).BuildServiceProvider();
        return (new ExecutionMiddleware<RecordingConsumer, OrderPlaced>(provider, deserializer ?? new JsonKafkaMessageDeserializer()), consumer);
    }

    [Fact]
    public async Task ValidMessage_Invoke_DeliversTheDeserializedMessageThenCallsNext()
    {
        var (sut, consumer) = Create();
        var nextRan = false;

        await sut.InvokeAsync(ContextWith("{\"Id\":\"o-1\",\"Quantity\":2}"), () =>
        {
            nextRan = true;
            return Task.CompletedTask;
        });

        var received = Assert.Single(consumer.Received);
        Assert.Equal("o-1", received.Id);
        Assert.Equal(2, received.Quantity);
        Assert.True(nextRan);
    }

    [Fact]
    public async Task ConsumerThrows_Invoke_PropagatesAndSkipsNext()
    {
        var (sut, consumer) = Create();
        consumer.FailWith = new InvalidOperationException("handler failed");
        var nextRan = false;

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            sut.InvokeAsync(ContextWith("{\"Id\":\"o-1\"}"), () =>
            {
                nextRan = true;
                return Task.CompletedTask;
            }));

        Assert.False(nextRan);
    }

    [Fact]
    public async Task PoisonPill_Invoke_ThrowsJsonExceptionWithoutCallingTheConsumer()
    {
        var (sut, consumer) = Create();

        await Assert.ThrowsAnyAsync<JsonException>(() => sut.InvokeAsync(ContextWith("{{{ not json"), () => Task.CompletedTask));

        Assert.Empty(consumer.Received);
    }

    [Fact]
    public async Task ConsumerNotRegistered_Invoke_ThrowsInvalidOperation()
    {
        var empty = new ServiceCollection().BuildServiceProvider();
        var sut = new ExecutionMiddleware<RecordingConsumer, OrderPlaced>(empty, new JsonKafkaMessageDeserializer());

        var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            sut.InvokeAsync(ContextWith("{\"Id\":\"o-1\"}"), () => Task.CompletedTask));

        Assert.Contains(nameof(RecordingConsumer), error.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task NullContext_Invoke_ThrowsArgumentNull()
    {
        var (sut, _) = Create();

        await Assert.ThrowsAsync<ArgumentNullException>(() => sut.InvokeAsync(null!, () => Task.CompletedTask));
    }

    [Fact]
    public async Task NullNext_Invoke_ThrowsArgumentNull()
    {
        var (sut, _) = Create();

        await Assert.ThrowsAsync<ArgumentNullException>(() => sut.InvokeAsync(ContextWith("{}"), null!));
    }

    [Fact]
    public void NullDependencies_Constructed_ThrowArgumentNull()
    {
        var provider = new ServiceCollection().BuildServiceProvider();

        Assert.Throws<ArgumentNullException>(() => new ExecutionMiddleware<RecordingConsumer, OrderPlaced>(null!, new JsonKafkaMessageDeserializer()));
        Assert.Throws<ArgumentNullException>(() => new ExecutionMiddleware<RecordingConsumer, OrderPlaced>(provider, null!));
    }
}
