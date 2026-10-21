using System.Text;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;

namespace QKafka.Tests.Support;

/// <summary>Builders for Kafka messages and execution contexts used across unit tests.</summary>
internal static class TestKafka
{
    public static ConsumeResult<byte[], byte[]> Message(string? key, long offset = 0, int partition = 0, string value = "payload") =>
        new()
        {
            Topic = "test-topic",
            Partition = new Partition(partition),
            Offset = new Offset(offset),
            Message = new Message<byte[], byte[]>
            {
                Key = key is null ? null! : Encoding.UTF8.GetBytes(key),
                Value = Encoding.UTF8.GetBytes(value),
            },
        };

    public static KafkaContext Context(ConsumeResult<byte[], byte[]>? result, CancellationToken cancellationToken = default) =>
        new(
            result,
            result?.Partition.Value ?? 0,
            result?.Offset.Value ?? 0,
            result?.Message.Key is { } key ? Encoding.UTF8.GetString(key) : string.Empty,
            new Dictionary<string, byte[]>(StringComparer.Ordinal),
            result?.Message.Value ?? Array.Empty<byte>(),
            new ServiceCollection().BuildServiceProvider(),
            cancellationToken);
}
