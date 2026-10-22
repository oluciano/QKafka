using Microsoft.Extensions.DependencyInjection;

namespace QKafka.Tests.Support;

/// <summary>An <see cref="IKafkaContext"/> that is not a <see cref="KafkaContext"/>, to exercise the non-Kafka branches.</summary>
internal sealed class StubKafkaContext : IKafkaContext
{
    public CancellationToken CancellationToken => CancellationToken.None;

    public int Partition => 0;

    public long Offset => 0;

    public string Key => "stub";

    public IReadOnlyDictionary<string, byte[]> Headers { get; } = new Dictionary<string, byte[]>(StringComparer.Ordinal);

    public byte[] Payload => Array.Empty<byte>();

    public IServiceProvider RequestServices { get; } = new ServiceCollection().BuildServiceProvider();
}
