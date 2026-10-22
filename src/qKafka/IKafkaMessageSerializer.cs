using System;

namespace QKafka;

/// <summary>
/// Converts a message object into the bytes written to a Kafka topic.
/// Replace it together with <see cref="IKafkaMessageDeserializer"/> to use another wire format.
/// </summary>
public interface IKafkaMessageSerializer
{
    /// <summary>
    /// Serializes a message.
    /// </summary>
    /// <param name="message">The message to serialize.</param>
    /// <param name="messageType">The declared type of the message.</param>
    /// <returns>The serialized payload.</returns>
    byte[] Serialize(object message, Type messageType);
}
