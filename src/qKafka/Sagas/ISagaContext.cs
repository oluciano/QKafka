using QKafka;

namespace QKafka.Sagas;

/// <summary>
/// Execution context for Saga handlers.
/// </summary>
public interface ISagaContext : IKafkaContext
{
    /// <summary>
    /// Flags the Saga instance as completed, marking it for deletion or archival.
    /// </summary>
    void Complete();

    /// <summary>
    /// Registers a compensating command to be executed in reverse order if the Saga fails.
    /// </summary>
    /// <typeparam name="TMessage">The type of the compensating message.</typeparam>
    /// <param name="message">The compensating message payload.</param>
    void RegisterCompensation<TMessage>(TMessage message)
        where TMessage : class;
}
