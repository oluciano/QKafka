using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace QKafka;

/// <summary>
/// Default implementation of <see cref="IDltPublisher"/> using the shared Kafka producer.
/// </summary>
internal sealed class DltPublisher : IDltPublisher
{
    private readonly IProducer<byte[], byte[]> _producer;
    private readonly ILogger<DltPublisher> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="DltPublisher"/> class.
    /// </summary>
    /// <param name="producer">The shared Kafka producer.</param>
    /// <param name="logger">The logger instance.</param>
    public DltPublisher(IProducer<byte[], byte[]> producer, ILogger<DltPublisher> logger)
    {
        _producer = producer ?? throw new ArgumentNullException(nameof(producer));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <inheritdoc/>
    public async Task PublishAsync(
        ConsumeResult<byte[], byte[]> original,
        DltReason reason,
        Exception? exception,
        int attemptCount,
        CancellationToken cancellationToken)
    {
        if (original == null)
        {
            throw new ArgumentNullException(nameof(original));
        }

        var dltTopic = $"{original.Topic}.dlt";

        var message = new Message<byte[], byte[]>
        {
            Key = original.Message.Key,
            Value = original.Message.Value,
            Headers = new Headers(),
        };

        if (original.Message.Headers != null)
        {
            foreach (var h in original.Message.Headers)
            {
                message.Headers.Add(h.Key, h.GetValueBytes());
            }
        }

        AddHeader(message.Headers, "qkafka-dlt-source-topic", original.Topic);
        AddHeader(message.Headers, "qkafka-dlt-source-partition", original.Partition.Value.ToString(System.Globalization.CultureInfo.InvariantCulture));
        AddHeader(message.Headers, "qkafka-dlt-source-offset", original.Offset.Value.ToString(System.Globalization.CultureInfo.InvariantCulture));
        AddHeader(message.Headers, "qkafka-dlt-reason", reason == DltReason.PoisonPill ? "poison-pill" : "max-retries-exceeded");
        AddHeader(message.Headers, "qkafka-dlt-attempt-count", attemptCount.ToString(System.Globalization.CultureInfo.InvariantCulture));
        AddHeader(message.Headers, "qkafka-dlt-timestamp", DateTime.UtcNow.ToString("o", System.Globalization.CultureInfo.InvariantCulture));

        if (exception != null)
        {
            AddHeader(message.Headers, "qkafka-dlt-exception-type", exception.GetType().FullName ?? exception.GetType().Name);
            var msg = exception.Message;
            if (msg.Length > 500)
            {
                msg = msg.Substring(0, 500);
            }

            AddHeader(message.Headers, "qkafka-dlt-exception-message", msg);
        }

        _logger.LogWarning(
            "Routing failed message from topic '{SourceTopic}' partition {Partition} offset {Offset} to DLT '{DltTopic}'. Reason: {Reason}.",
            original.Topic,
            original.Partition.Value,
            original.Offset.Value,
            dltTopic,
            reason);

        try
        {
            await _producer.ProduceAsync(dltTopic, message, cancellationToken).ConfigureAwait(false);
        }
#pragma warning disable S2139 // log with context here, rethrow so the caller decides (retry, DLT, no offset commit)
        catch (ProduceException<byte[], byte[]> ex)
        {
            _logger.LogError(ex, "Failed to publish message to Dead Letter Topic '{DltTopic}'", dltTopic);
            throw;
        }
#pragma warning restore S2139
    }

    private static void AddHeader(Headers headers, string key, string value)
    {
        headers.Add(key, Encoding.UTF8.GetBytes(value));
    }
}
