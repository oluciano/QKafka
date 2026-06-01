using System;

namespace QKafka;

/// <summary>
/// Configures routing and group details for a consumer.
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false, Inherited = false)]
public sealed class KafkaConsumerAttribute : Attribute
{
    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaConsumerAttribute"/> class.
    /// </summary>
    /// <param name="topic">The topic to consume from.</param>
    public KafkaConsumerAttribute(string topic)
    {
        Topic = topic ?? throw new ArgumentNullException(nameof(topic));
    }

    /// <summary>
    /// Gets the Kafka topic to consume.
    /// </summary>
    public string Topic { get; }

    /// <summary>
    /// Gets or sets the Consumer Group ID. If null, a convention-based Group ID will be generated.
    /// </summary>
    public string? GroupId { get; set; }
}
