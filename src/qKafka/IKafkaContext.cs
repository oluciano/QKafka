using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace QKafka;

/// <summary>
/// Provides execution context for the current message consumption.
/// </summary>
public interface IKafkaContext
{
    /// <summary>
    /// Gets the cancellation token for the current execution.
    /// </summary>
    CancellationToken CancellationToken { get; }

    /// <summary>
    /// Gets the partition number from which the message was consumed.
    /// </summary>
    int Partition { get; }

    /// <summary>
    /// Gets the offset of the consumed message.
    /// </summary>
    long Offset { get; }

    /// <summary>
    /// Gets the key of the message.
    /// </summary>
    string Key { get; }

    /// <summary>
    /// Gets the headers associated with the message.
    /// </summary>
    IReadOnlyDictionary<string, byte[]> Headers { get; }

    /// <summary>
    /// Publishes a message to a topic.
    /// </summary>
    /// <typeparam name="TMessage">The type of the message.</typeparam>
    /// <param name="message">The message payload.</param>
    /// <param name="key">Optional partition key.</param>
    /// <returns>A task representing the asynchronous publish operation.</returns>
    Task PublishAsync<TMessage>(TMessage message, string? key = null)
        where TMessage : class;
}
