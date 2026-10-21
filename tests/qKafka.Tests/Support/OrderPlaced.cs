namespace QKafka.Tests.Support;

/// <summary>A sample message type.</summary>
internal sealed class OrderPlaced
{
    public string Id { get; set; } = string.Empty;

    public int Quantity { get; set; }
}
