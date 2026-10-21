using QKafka.Tests.Support;

namespace QKafka.Tests;

public sealed class ConsumerRegistrationUnitTests
{
    [Fact]
    public void Arguments_Provided_AreExposed()
    {
        var sut = new ConsumerRegistration(typeof(OrderConsumer), typeof(OrderPlaced), "orders", "orders-group");

        Assert.Equal(typeof(OrderConsumer), sut.ConsumerType);
        Assert.Equal(typeof(OrderPlaced), sut.MessageType);
        Assert.Equal("orders", sut.Topic);
        Assert.Equal("orders-group", sut.GroupId);
    }

    [Fact]
    public void NullArguments_Constructed_EachThrowArgumentNull()
    {
        var consumer = typeof(OrderConsumer);
        var message = typeof(OrderPlaced);

        Assert.Throws<ArgumentNullException>(() => new ConsumerRegistration(null!, message, "t", "g"));
        Assert.Throws<ArgumentNullException>(() => new ConsumerRegistration(consumer, null!, "t", "g"));
        Assert.Throws<ArgumentNullException>(() => new ConsumerRegistration(consumer, message, null!, "g"));
        Assert.Throws<ArgumentNullException>(() => new ConsumerRegistration(consumer, message, "t", null!));
    }
}
