using System.Threading.Tasks;

namespace QKafka.Sagas;

/// <summary>
/// Defines a handler that processes subsequent messages for an active Saga.
/// </summary>
/// <typeparam name="TMessage">The type of message to handle.</typeparam>
public interface IHandleSaga<in TMessage>
    where TMessage : class
{
    /// <summary>
    /// Handles a subsequent message in the Saga workflow.
    /// </summary>
    /// <param name="message">The message payload.</param>
    /// <param name="context">The Saga execution context.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task HandleAsync(TMessage message, ISagaContext context);
}
