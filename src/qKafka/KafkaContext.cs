using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace QKafka;

/// <summary>
/// Execution context for a message being consumed.
/// </summary>
public sealed class KafkaContext : IKafkaContext
{
    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaContext"/> class.
    /// </summary>
    /// <param name="consumeResult">The raw Confluent Kafka consume result, if available.</param>
    /// <param name="partition">The partition identifier.</param>
    /// <param name="offset">The offset of the message.</param>
    /// <param name="key">The message partition key.</param>
    /// <param name="headers">The headers of the message.</param>
    /// <param name="payload">The raw message payload bytes.</param>
    /// <param name="requestServices">The service provider.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public KafkaContext(
        ConsumeResult<byte[], byte[]>? consumeResult,
        int partition,
        long offset,
        string key,
        IReadOnlyDictionary<string, byte[]> headers,
        byte[] payload,
        IServiceProvider requestServices,
        CancellationToken cancellationToken)
    {
        ConsumeResult = consumeResult;
        Partition = partition;
        Offset = offset;
        Key = key ?? throw new ArgumentNullException(nameof(key));
        Headers = headers ?? throw new ArgumentNullException(nameof(headers));
        Payload = payload ?? throw new ArgumentNullException(nameof(payload));
        RequestServices = requestServices ?? throw new ArgumentNullException(nameof(requestServices));
        CancellationToken = cancellationToken;
    }

    /// <inheritdoc/>
    public CancellationToken CancellationToken { get; }

    /// <inheritdoc/>
    public int Partition { get; }

    /// <inheritdoc/>
    public long Offset { get; }

    /// <inheritdoc/>
    public string Key { get; }

    /// <inheritdoc/>
    public IReadOnlyDictionary<string, byte[]> Headers { get; }

    /// <inheritdoc/>
    public byte[] Payload { get; }

    /// <summary>
    /// Gets the original Kafka consume result, if available.
    /// </summary>
    public ConsumeResult<byte[], byte[]>? ConsumeResult { get; }

    /// <inheritdoc/>
    public IServiceProvider RequestServices { get; set; }

    /// <inheritdoc/>
    public async Task PublishAsync<TMessage>(TMessage message, string? key = null)
        where TMessage : class
    {
        if (message == null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        // Prototype implementation for writing to outbox/producer:
        // Will be connected to IProducer / IOutboxStore in future phases.
        await Task.CompletedTask.ConfigureAwait(false);
    }
}
