using System.Threading;
using System.Threading.Tasks;

namespace QKafka;

/// <summary>
/// Publishes messages to Kafka from any application component (controller, background service, consumer).
/// Each call completes only after the broker acknowledged the message and throws if it did not.
/// </summary>
public interface IKafkaPublisher
{
    /// <summary>
    /// Publishes a message to the topic declared on <typeparamref name="TMessage"/> with <see cref="KafkaTopicAttribute"/>.
    /// </summary>
    /// <typeparam name="TMessage">The message type.</typeparam>
    /// <param name="message">The message to publish.</param>
    /// <param name="key">Optional partition key; messages with the same key keep their order.</param>
    /// <param name="ct">A token to cancel the operation.</param>
    /// <returns>A task that completes when the broker acknowledged the message.</returns>
    /// <exception cref="System.InvalidOperationException"><typeparamref name="TMessage"/> has no <see cref="KafkaTopicAttribute"/>.</exception>
    Task PublishAsync<TMessage>(TMessage message, string? key = null, CancellationToken ct = default)
        where TMessage : class;

    /// <summary>
    /// Publishes a message to an explicit topic, ignoring any <see cref="KafkaTopicAttribute"/>.
    /// </summary>
    /// <typeparam name="TMessage">The message type.</typeparam>
    /// <param name="topic">The destination topic.</param>
    /// <param name="message">The message to publish.</param>
    /// <param name="key">Optional partition key; messages with the same key keep their order.</param>
    /// <param name="ct">A token to cancel the operation.</param>
    /// <returns>A task that completes when the broker acknowledged the message.</returns>
    Task PublishToAsync<TMessage>(string topic, TMessage message, string? key = null, CancellationToken ct = default)
        where TMessage : class;
}
