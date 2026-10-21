using System;

namespace QKafka;

/// <summary>
/// Represents a registered consumer with its configuration metadata.
/// </summary>
internal sealed class ConsumerRegistration
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ConsumerRegistration"/> class.
    /// </summary>
    /// <param name="consumerType">The type of the consumer implementation.</param>
    /// <param name="messageType">The type of the message being consumed.</param>
    /// <param name="topic">The Kafka topic name.</param>
    /// <param name="groupId">The Kafka consumer Group ID.</param>
    public ConsumerRegistration(Type consumerType, Type messageType, string topic, string groupId)
    {
        ConsumerType = consumerType ?? throw new ArgumentNullException(nameof(consumerType));
        MessageType = messageType ?? throw new ArgumentNullException(nameof(messageType));
        Topic = topic ?? throw new ArgumentNullException(nameof(topic));
        GroupId = groupId ?? throw new ArgumentNullException(nameof(groupId));
    }

    /// <summary>
    /// Gets the C# type of the consumer implementation.
    /// </summary>
    public Type ConsumerType { get; }

    /// <summary>
    /// Gets the C# type of the message being consumed.
    /// </summary>
    public Type MessageType { get; }

    /// <summary>
    /// Gets the Kafka topic name.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// Gets the Kafka consumer Group ID.
    /// </summary>
    public string GroupId { get; }
}
