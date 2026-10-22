namespace QKafka.Tests.Support;

/// <summary>A sample message type that declares its topic.</summary>
[KafkaTopic("orders-shipped")]
internal sealed class OrderShipped
{
    public string Id { get; set; } = string.Empty;
}
