using System.Reflection;

namespace QKafka.Tests;

public sealed class KafkaTopicAttributeUnitTests
{
    [Fact]
    public void Topic_Provided_IsExposed()
    {
        Assert.Equal("orders", new KafkaTopicAttribute("orders").Topic);
    }

    [Fact]
    public void NullTopic_Constructed_ThrowsArgumentNull()
    {
        Assert.Throws<ArgumentNullException>(() => new KafkaTopicAttribute(null!));
    }

    [Fact]
    public void Usage_IsOnePerClassAndNotInherited()
    {
        var usage = typeof(KafkaTopicAttribute).GetCustomAttribute<AttributeUsageAttribute>()!;

        Assert.Equal(AttributeTargets.Class, usage.ValidOn);
        Assert.False(usage.AllowMultiple);
        Assert.False(usage.Inherited);
    }
}
