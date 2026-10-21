using System.Reflection;

namespace QKafka.Tests;

public sealed class KafkaConsumerAttributeUnitTests
{
    [Fact]
    public void Topic_Provided_IsExposedAndGroupIdDefaultsToNull()
    {
        var sut = new KafkaConsumerAttribute("orders");

        Assert.Equal("orders", sut.Topic);
        Assert.Null(sut.GroupId);
    }

    [Fact]
    public void GroupId_Assigned_IsReadBack()
    {
        var sut = new KafkaConsumerAttribute("orders") { GroupId = "billing" };

        Assert.Equal("billing", sut.GroupId);
    }

    [Fact]
    public void NullTopic_Constructed_ThrowsArgumentNull()
    {
        Assert.Throws<ArgumentNullException>(() => new KafkaConsumerAttribute(null!));
    }

    [Fact]
    public void AttributeUsage_Inspected_AllowsOnlyOneNonInheritedOnClasses()
    {
        var usage = typeof(KafkaConsumerAttribute).GetCustomAttribute<AttributeUsageAttribute>();

        Assert.NotNull(usage);
        Assert.Equal(AttributeTargets.Class, usage.ValidOn);
        Assert.False(usage.AllowMultiple);
        Assert.False(usage.Inherited);
    }
}
