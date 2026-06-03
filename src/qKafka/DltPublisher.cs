using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;

namespace QKafka;

/// <summary>
/// Default implementation of <see cref="IDltPublisher"/> using a Kafka producer.
/// </summary>
public sealed class DltPublisher : IDltPublisher, IDisposable
{
    private readonly IProducer<byte[], byte[]>? _producer;
    private readonly ILogger<DltPublisher> _logger;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="DltPublisher"/> class.
    /// </summary>
    /// <param name="options">The global configuration options.</param>
    /// <param name="logger">The logger instance.</param>
    public DltPublisher(QKafkaOptions options, ILogger<DltPublisher> logger)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        _logger = logger ?? throw new ArgumentNullException(nameof(logger));

        if (!string.IsNullOrEmpty(options.BootstrapServers))
        {
            var config = new ProducerConfig
            {
                BootstrapServers = options.BootstrapServers,
                SaslUsername = options.SaslUsername,
                SaslPassword = options.SaslPassword,
            };

            if (options.SaslMechanism != null)
            {
                config.SaslMechanism = Enum.Parse<SaslMechanism>(options.SaslMechanism, true);
            }

            if (options.SecurityProtocol != null)
            {
                config.SecurityProtocol = Enum.Parse<SecurityProtocol>(options.SecurityProtocol, true);
            }

            _producer = new ProducerBuilder<byte[], byte[]>(config).Build();
        }
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

        if (_producer == null)
        {
            _logger.LogWarning("DLT Publisher is not configured (BootstrapServers is empty). Message not routed.");
            return;
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
        catch (ProduceException<byte[], byte[]> ex)
        {
            _logger.LogError(ex, "Failed to publish message to Dead Letter Topic '{DltTopic}'", dltTopic);
            throw;
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _producer?.Dispose();
        _disposed = true;
    }

    private static void AddHeader(Headers headers, string key, string value)
    {
        headers.Add(key, Encoding.UTF8.GetBytes(value));
    }
}
