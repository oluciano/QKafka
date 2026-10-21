using System.Threading.Tasks;

namespace QKafka.Sagas;

/// <summary>
/// Defines a handler that initiates a new Saga instance.
/// </summary>
/// <typeparam name="TMessage">The type of the initiating message.</typeparam>
public interface IStartSaga<in TMessage>
    where TMessage : class
{
    /// <summary>
    /// Handles the initiating message to start a Saga.
    /// </summary>
    /// <param name="message">The initiating message.</param>
    /// <param name="context">The Saga execution context.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task HandleAsync(TMessage message, ISagaContext context);
}
