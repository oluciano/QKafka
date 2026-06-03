using System;

namespace QKafka;

/// <summary>
/// Defines a deserializer for Kafka message payloads.
/// </summary>
public interface IKafkaMessageDeserializer
{
    /// <summary>
    /// Deserializes raw binary data into a message object of the target type.
    /// </summary>
    /// <param name="data">The raw binary payload.</param>
    /// <param name="targetType">The expected target message type.</param>
    /// <returns>The deserialized message object.</returns>
    object Deserialize(byte[] data, Type targetType);
}
