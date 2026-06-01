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
}
