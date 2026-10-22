using System.Text;
using Confluent.Kafka;
using QKafka.Tests.Support;

namespace QKafka.Tests;

public sealed class KafkaPublisherUnitTests
{
    private readonly RecordingProducer _producer = new();

    private KafkaPublisher Create() => new(_producer, new JsonKafkaMessageSerializer());

    [Fact]
    public async Task MessageWithTopicAttribute_Published_GoesToThatTopicWithKeyPayloadAndTypeHeader()
    {
        await Create().PublishAsync(new OrderShipped { Id = "o-1" }, "k-1");

        var (topic, message) = Assert.Single(_producer.Produced);
        Assert.Equal("orders-shipped", topic);
        Assert.Equal("k-1", Encoding.UTF8.GetString(message.Key));
        Assert.Equal("{\"Id\":\"o-1\"}", Encoding.UTF8.GetString(message.Value));
        Assert.Equal(typeof(OrderShipped).FullName, Encoding.UTF8.GetString(message.Headers.GetLastBytes(KafkaPublisher.MessageTypeHeader)));
    }

    [Fact]
    public async Task NoKey_Published_ProducesAKeylessMessage()
    {
        await Create().PublishAsync(new OrderShipped());

        Assert.Null(Assert.Single(_producer.Produced).Message.Key);
    }

    [Fact]
    public async Task ExplicitTopic_PublishedTo_OverridesTheAttributeAndWorksWithoutOne()
    {
        var sut = Create();

        await sut.PublishToAsync("custom", new OrderShipped());
        await sut.PublishToAsync("other", new OrderPlaced { Id = "o-2" });

        Assert.Equal(new[] { "custom", "other" }, _producer.Produced.Select(p => p.Topic));
    }

    [Fact]
    public async Task BrokerFailure_Published_PropagatesToTheCaller()
    {
        _producer.Failure = new InvalidOperationException("broker down");

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => Create().PublishAsync(new OrderShipped()));

        Assert.Equal("broker down", thrown.Message);
        Assert.Empty(_producer.Produced);
    }

    [Fact]
    public async Task CancelledToken_Published_Cancels()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Create().PublishAsync(new OrderShipped(), ct: cts.Token));
    }

    [Fact]
    public async Task NullMessage_Published_ThrowsArgumentNullBeforeTouchingTheBroker()
    {
        var sut = Create();

        await Assert.ThrowsAsync<ArgumentNullException>(() => sut.PublishAsync<OrderShipped>(null!));
        await Assert.ThrowsAsync<ArgumentNullException>(() => sut.PublishToAsync<OrderShipped>("t", null!));
        Assert.Empty(_producer.Produced);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("  ")]
    public async Task BlankTopic_PublishedTo_ThrowsArgumentException(string? topic)
    {
        await Assert.ThrowsAnyAsync<ArgumentException>(() => Create().PublishToAsync(topic!, new OrderShipped()));
    }

    [Fact]
    public async Task MessageWithoutTopicAttribute_Published_ThrowsNamingTheType()
    {
        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => Create().PublishAsync(new OrderPlaced()));

        Assert.Contains(typeof(OrderPlaced).FullName!, thrown.Message, StringComparison.Ordinal);
        Assert.Empty(_producer.Produced);
    }

    [Fact]
    public void NullDependencies_Constructed_ThrowArgumentNull()
    {
        Assert.Throws<ArgumentNullException>(() => new KafkaPublisher(null!, new JsonKafkaMessageSerializer()));
        Assert.Throws<ArgumentNullException>(() => new KafkaPublisher(_producer, null!));
    }
}
