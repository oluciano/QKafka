using System;

namespace QKafka.Sagas;

/// <summary>
/// Represents the base class for all Saga orchestrators.
/// </summary>
/// <typeparam name="TSagaState">The type representing the state of the Saga.</typeparam>
public abstract class Saga<TSagaState>
    where TSagaState : class, new()
{
    /// <summary>
    /// Gets or sets the unique correlation identifier for this Saga instance.
    /// </summary>
    public Guid CorrelationId { get; set; }

    /// <summary>
    /// Gets or sets the state of the Saga instance.
    /// </summary>
    public TSagaState State { get; set; } = new();
}
