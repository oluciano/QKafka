using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace QKafka;

/// <summary>
/// Execution context for a message being consumed.
/// </summary>
public sealed class KafkaContext : IKafkaContext
{
    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaContext"/> class.
    /// </summary>
    /// <param name="partition">The partition identifier.</param>
    /// <param name="offset">The offset of the message.</param>
    /// <param name="key">The message partition key.</param>
    /// <param name="headers">The headers of the message.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    public KafkaContext(
        int partition,
        long offset,
        string key,
        IReadOnlyDictionary<string, byte[]> headers,
        CancellationToken cancellationToken)
    {
        Partition = partition;
        Offset = offset;
        Key = key ?? throw new ArgumentNullException(nameof(key));
        Headers = headers ?? throw new ArgumentNullException(nameof(headers));
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
