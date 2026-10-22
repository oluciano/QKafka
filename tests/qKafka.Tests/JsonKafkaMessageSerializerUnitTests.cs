using System.Text;
using QKafka.Tests.Support;

namespace QKafka.Tests;

public sealed class JsonKafkaMessageSerializerUnitTests
{
    private readonly JsonKafkaMessageSerializer _sut = new();

    [Fact]
    public void Message_Serialized_ProducesUtf8Json()
    {
        var bytes = _sut.Serialize(new OrderPlaced { Id = "o-1", Quantity = 3 }, typeof(OrderPlaced));

        Assert.Equal("{\"Id\":\"o-1\",\"Quantity\":3}", Encoding.UTF8.GetString(bytes));
    }

    [Fact]
    public void Message_SerializedThenDeserialized_RoundTrips()
    {
        var bytes = _sut.Serialize(new OrderPlaced { Id = "o-2", Quantity = 9 }, typeof(OrderPlaced));

        var back = Assert.IsType<OrderPlaced>(new JsonKafkaMessageDeserializer().Deserialize(bytes, typeof(OrderPlaced)));

        Assert.Equal("o-2", back.Id);
        Assert.Equal(9, back.Quantity);
    }

    [Fact]
    public void NullMessage_Serialized_ThrowsArgumentNull()
    {
        Assert.Throws<ArgumentNullException>(() => _sut.Serialize(null!, typeof(OrderPlaced)));
    }

    [Fact]
    public void NullType_Serialized_ThrowsArgumentNull()
    {
        Assert.Throws<ArgumentNullException>(() => _sut.Serialize(new OrderPlaced(), null!));
    }
}
