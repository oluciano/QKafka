using System;
using System.Collections.Concurrent;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace QKafka;

/// <summary>
/// Default <see cref="IKafkaPublisher"/>: serializes the message and produces it through the shared Kafka producer.
/// </summary>
internal sealed class KafkaPublisher : IKafkaPublisher
{
    /// <summary>The header that carries the CLR type name of the published message.</summary>
    internal const string MessageTypeHeader = "qkafka-message-type";

    private static readonly ConcurrentDictionary<Type, string?> TopicByType = new();

    private readonly IProducer<byte[], byte[]> _producer;
    private readonly IKafkaMessageSerializer _serializer;

    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaPublisher"/> class.
    /// </summary>
    /// <param name="producer">The shared Kafka producer.</param>
    /// <param name="serializer">The message serializer.</param>
    public KafkaPublisher(IProducer<byte[], byte[]> producer, IKafkaMessageSerializer serializer)
    {
        _producer = producer ?? throw new ArgumentNullException(nameof(producer));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    }

    /// <inheritdoc/>
    public Task PublishAsync<TMessage>(TMessage message, string? key = null, CancellationToken ct = default)
        where TMessage : class
    {
        if (message == null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        var topic = TopicByType.GetOrAdd(typeof(TMessage), static t => t.GetCustomAttribute<KafkaTopicAttribute>()?.Topic)
            ?? throw new InvalidOperationException(
                $"Message type '{typeof(TMessage).FullName}' has no [KafkaTopic] attribute. Add one, or call PublishToAsync with an explicit topic.");

        return ProduceAsync(topic, message, key, ct);
    }

    /// <inheritdoc/>
    public Task PublishToAsync<TMessage>(string topic, TMessage message, string? key = null, CancellationToken ct = default)
        where TMessage : class
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(topic);

        if (message == null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        return ProduceAsync(topic, message, key, ct);
    }

    private async Task ProduceAsync<TMessage>(string topic, TMessage message, string? key, CancellationToken ct)
        where TMessage : class
    {
        var kafkaMessage = new Message<byte[], byte[]>
        {
            Key = key is null ? null! : Encoding.UTF8.GetBytes(key),
            Value = _serializer.Serialize(message, typeof(TMessage)),
            Headers = new Headers
            {
                { MessageTypeHeader, Encoding.UTF8.GetBytes(typeof(TMessage).FullName ?? typeof(TMessage).Name) },
            },
        };

        await _producer.ProduceAsync(topic, kafkaMessage, ct).ConfigureAwait(false);
    }
}
