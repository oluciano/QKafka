using System.Text;
using Confluent.Kafka;
using QKafka.Tests.Support;

namespace QKafka.Tests;

public sealed class DltPublisherUnitTests
{
    private readonly RecordingProducer _producer = new();
    private readonly ListLogger<DltPublisher> _logger = new();

    private DltPublisher Create() => new(_producer, _logger);

    private static string Header(Message<byte[], byte[]> message, string name) =>
        Encoding.UTF8.GetString(message.Headers.GetLastBytes(name));

    [Fact]
    public async Task FailedMessage_Routed_GoesToTheDltTopicWithKeyAndPayloadIntact()
    {
        var original = TestKafka.Message("k-1", offset: 7, partition: 2, value: "body");

        await Create().PublishAsync(original, DltReason.MaxRetriesExceeded, null, 4, default);

        var (topic, message) = Assert.Single(_producer.Produced);
        Assert.Equal("test-topic.dlt", topic);
        Assert.Equal("k-1", Encoding.UTF8.GetString(message.Key));
        Assert.Equal("body", Encoding.UTF8.GetString(message.Value));
    }

    [Fact]
    public async Task FailedMessage_Routed_CarriesTheDiagnosticHeaders()
    {
        var original = TestKafka.Message("k", offset: 7, partition: 2);

        await Create().PublishAsync(original, DltReason.MaxRetriesExceeded, null, 4, default);

        var message = Assert.Single(_producer.Produced).Message;
        Assert.Equal("test-topic", Header(message, "qkafka-dlt-source-topic"));
        Assert.Equal("2", Header(message, "qkafka-dlt-source-partition"));
        Assert.Equal("7", Header(message, "qkafka-dlt-source-offset"));
        Assert.Equal("max-retries-exceeded", Header(message, "qkafka-dlt-reason"));
        Assert.Equal("4", Header(message, "qkafka-dlt-attempt-count"));
        Assert.True(DateTime.TryParse(Header(message, "qkafka-dlt-timestamp"), System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind, out _));
    }

    [Fact]
    public async Task PoisonPill_Routed_UsesItsOwnReasonLabel()
    {
        await Create().PublishAsync(TestKafka.Message("k"), DltReason.PoisonPill, null, 0, default);

        Assert.Equal("poison-pill", Header(Assert.Single(_producer.Produced).Message, "qkafka-dlt-reason"));
    }

    [Fact]
    public async Task OriginalHeaders_Routed_AreKeptAlongsideTheDltOnes()
    {
        var original = TestKafka.Message("k");
        original.Message.Headers = new Headers { { "traceparent", Encoding.UTF8.GetBytes("00-abc") } };

        await Create().PublishAsync(original, DltReason.PoisonPill, null, 0, default);

        Assert.Equal("00-abc", Header(Assert.Single(_producer.Produced).Message, "traceparent"));
    }

    [Fact]
    public async Task Exception_Routed_RecordsItsTypeAndMessage()
    {
        await Create().PublishAsync(TestKafka.Message("k"), DltReason.MaxRetriesExceeded, new InvalidOperationException("boom"), 4, default);

        var message = Assert.Single(_producer.Produced).Message;
        Assert.Equal(typeof(InvalidOperationException).FullName, Header(message, "qkafka-dlt-exception-type"));
        Assert.Equal("boom", Header(message, "qkafka-dlt-exception-message"));
    }

    [Fact]
    public async Task LongExceptionMessage_Routed_IsTruncatedTo500Characters()
    {
        await Create().PublishAsync(TestKafka.Message("k"), DltReason.MaxRetriesExceeded, new InvalidOperationException(new string('x', 900)), 4, default);

        Assert.Equal(500, Header(Assert.Single(_producer.Produced).Message, "qkafka-dlt-exception-message").Length);
    }

    [Fact]
    public async Task NoException_Routed_OmitsTheExceptionHeaders()
    {
        await Create().PublishAsync(TestKafka.Message("k"), DltReason.PoisonPill, null, 0, default);

        var headers = Assert.Single(_producer.Produced).Message.Headers;
        Assert.False(headers.TryGetLastBytes("qkafka-dlt-exception-type", out _));
    }

    [Fact]
    public async Task ProducerFailure_Routed_IsLoggedAndRethrownSoTheOffsetIsNotCommitted()
    {
        var failure = new ProduceException<byte[], byte[]>(new Error(ErrorCode.Local_MsgTimedOut), new DeliveryResult<byte[], byte[]>());
        _producer.Failure = failure;

        var thrown = await Assert.ThrowsAsync<ProduceException<byte[], byte[]>>(
            () => Create().PublishAsync(TestKafka.Message("k"), DltReason.MaxRetriesExceeded, null, 4, default));

        Assert.Same(failure, thrown);
        Assert.Contains(_logger.Entries, e => e.Level == Microsoft.Extensions.Logging.LogLevel.Error);
    }

    [Fact]
    public async Task NullOriginal_Routed_ThrowsArgumentNull()
    {
        await Assert.ThrowsAsync<ArgumentNullException>(() => Create().PublishAsync(null!, DltReason.PoisonPill, null, 0, default));
    }

    [Fact]
    public void NullDependencies_Constructed_ThrowArgumentNull()
    {
        Assert.Throws<ArgumentNullException>(() => new DltPublisher(null!, _logger));
        Assert.Throws<ArgumentNullException>(() => new DltPublisher(_producer, null!));
    }
}
