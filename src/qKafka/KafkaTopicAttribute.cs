using System;

namespace QKafka;

/// <summary>
/// Declares the Kafka topic a message type is published to by <see cref="IKafkaPublisher.PublishAsync{TMessage}"/>.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class KafkaTopicAttribute : Attribute
{
    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaTopicAttribute"/> class.
    /// </summary>
    /// <param name="topic">The topic name.</param>
    public KafkaTopicAttribute(string topic)
    {
        Topic = topic ?? throw new ArgumentNullException(nameof(topic));
    }

    /// <summary>
    /// Gets the topic name.
    /// </summary>
    public string Topic { get; }
}
