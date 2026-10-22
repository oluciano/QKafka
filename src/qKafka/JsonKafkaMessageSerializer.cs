using System;
using System.Text.Json;

namespace QKafka;

/// <summary>
/// Default <see cref="IKafkaMessageSerializer"/>: UTF-8 JSON, the counterpart of <see cref="JsonKafkaMessageDeserializer"/>.
/// </summary>
internal sealed class JsonKafkaMessageSerializer : IKafkaMessageSerializer
{
    /// <inheritdoc/>
    public byte[] Serialize(object message, Type messageType)
    {
        if (message == null)
        {
            throw new ArgumentNullException(nameof(message));
        }

        if (messageType == null)
        {
            throw new ArgumentNullException(nameof(messageType));
        }

        return JsonSerializer.SerializeToUtf8Bytes(message, messageType);
    }
}
