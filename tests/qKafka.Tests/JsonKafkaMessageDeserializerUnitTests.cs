using System.Text;
using System.Text.Json;
using QKafka.Tests.Support;

namespace QKafka.Tests;

public sealed class JsonKafkaMessageDeserializerUnitTests
{
    private readonly JsonKafkaMessageDeserializer _sut = new();

    [Fact]
    public void ValidJson_Deserialized_ReturnsTheTypedMessage()
    {
        var data = Encoding.UTF8.GetBytes("{\"Id\":\"o-1\",\"Quantity\":3}");

        var result = Assert.IsType<OrderPlaced>(_sut.Deserialize(data, typeof(OrderPlaced)));

        Assert.Equal("o-1", result.Id);
        Assert.Equal(3, result.Quantity);
    }

    [Fact]
    public void UnknownExtraFields_Deserialized_AreIgnored()
    {
        var data = Encoding.UTF8.GetBytes("{\"Id\":\"o-2\",\"Quantity\":1,\"Surprise\":true}");

        var result = Assert.IsType<OrderPlaced>(_sut.Deserialize(data, typeof(OrderPlaced)));

        Assert.Equal("o-2", result.Id);
    }

    [Fact]
    public void NullData_Deserialized_ThrowsArgumentNull()
    {
        Assert.Throws<ArgumentNullException>(() => _sut.Deserialize(null!, typeof(OrderPlaced)));
    }

    [Fact]
    public void NullType_Deserialized_ThrowsArgumentNull()
    {
        Assert.Throws<ArgumentNullException>(() => _sut.Deserialize(new byte[] { 1 }, null!));
    }

    [Theory]
    [InlineData("not json at all")]
    [InlineData("{\"Id\":")]
    [InlineData("{\"Id\":\"o-1\",\"Quantity\":\"many\"}")]
    public void CorruptPayload_Deserialized_ThrowsJsonException(string payload)
    {
        Assert.ThrowsAny<JsonException>(() => _sut.Deserialize(Encoding.UTF8.GetBytes(payload), typeof(OrderPlaced)));
    }

    [Fact]
    public void EmptyPayload_Deserialized_ThrowsJsonException()
    {
        Assert.ThrowsAny<JsonException>(() => _sut.Deserialize(Array.Empty<byte>(), typeof(OrderPlaced)));
    }

    [Fact]
    public void JsonNullLiteral_Deserialized_ThrowsJsonException()
    {
        Assert.Throws<JsonException>(() => _sut.Deserialize(Encoding.UTF8.GetBytes("null"), typeof(OrderPlaced)));
    }
}
