using System;
using System.Text;
using System.Text.Json;

namespace QKafka;

/// <summary>
/// A default implementation of <see cref="IKafkaMessageDeserializer"/> using System.Text.Json.
/// </summary>
public sealed class JsonKafkaMessageDeserializer : IKafkaMessageDeserializer
{
    /// <inheritdoc/>
    public object Deserialize(byte[] data, Type targetType)
    {
        if (data == null)
        {
            throw new ArgumentNullException(nameof(data));
        }

        if (targetType == null)
        {
            throw new ArgumentNullException(nameof(targetType));
        }

        var json = Encoding.UTF8.GetString(data);
        var result = JsonSerializer.Deserialize(json, targetType);
        if (result == null)
        {
            throw new JsonException($"Deserialized object was null for type {targetType.FullName}");
        }

        return result;
    }
}
