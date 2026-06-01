using System.Threading.Tasks;

namespace QKafka;

/// <summary>
/// Defines a consumer for a specific message type.
/// </summary>
/// <typeparam name="TMessage">The type of message to consume.</typeparam>
public interface IKafkaConsumer<in TMessage>
    where TMessage : class
{
    /// <summary>
    /// Consumes the message asynchronously.
    /// </summary>
    /// <param name="message">The incoming message payload.</param>
    /// <param name="context">The execution context for the consumer.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task ConsumeAsync(TMessage message, IKafkaContext context);
}
