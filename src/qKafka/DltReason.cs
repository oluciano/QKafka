namespace QKafka;

/// <summary>
/// Reasons for a message to be sent to the Dead Letter Topic.
/// </summary>
public enum DltReason
{
    /// <summary>
    /// The message could not be deserialized.
    /// </summary>
    PoisonPill,

    /// <summary>
    /// The message processing failed and exceeded max retry attempts.
    /// </summary>
    MaxRetriesExceeded,
}
