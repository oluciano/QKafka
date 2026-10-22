using System.Text;
using Microsoft.Extensions.DependencyInjection;
using QKafka.Tests.Support;

namespace QKafka.Tests;

public sealed class KafkaContextUnitTests
{
    [Fact]
    public void Constructed_FromAMessage_ExposesItsValues()
    {
        var result = TestKafka.Message("order-1", offset: 42, partition: 3, value: "body");
        var headers = new Dictionary<string, byte[]>(StringComparer.Ordinal) { ["h"] = new byte[] { 7 } };
        var services = new ServiceCollection().BuildServiceProvider();
        using var cts = new CancellationTokenSource();

        var sut = new KafkaContext(result, 3, 42, "order-1", headers, Encoding.UTF8.GetBytes("body"), services, cts.Token);

        Assert.Equal(3, sut.Partition);
        Assert.Equal(42, sut.Offset);
        Assert.Equal("order-1", sut.Key);
        Assert.Same(headers, sut.Headers);
        Assert.Equal("body", Encoding.UTF8.GetString(sut.Payload));
        Assert.Same(services, sut.RequestServices);
        Assert.Same(result, sut.ConsumeResult);
        Assert.Equal(cts.Token, sut.CancellationToken);
    }

    [Fact]
    public void NoConsumeResult_Constructed_IsAllowed()
    {
        var sut = TestKafka.Context(null);

        Assert.Null(sut.ConsumeResult);
    }

    [Fact]
    public void RequestServices_Assigned_CanBeReplaced()
    {
        var sut = TestKafka.Context(TestKafka.Message("k"));
        var replacement = new ServiceCollection().BuildServiceProvider();

        sut.RequestServices = replacement;

        Assert.Same(replacement, sut.RequestServices);
    }

    [Fact]
    public void NullArguments_Constructed_ThrowArgumentNull()
    {
        var headers = new Dictionary<string, byte[]>(StringComparer.Ordinal);
        var services = new ServiceCollection().BuildServiceProvider();
        var payload = Array.Empty<byte>();

        Assert.Throws<ArgumentNullException>(() => new KafkaContext(null, 0, 0, null!, headers, payload, services, default));
        Assert.Throws<ArgumentNullException>(() => new KafkaContext(null, 0, 0, "k", null!, payload, services, default));
        Assert.Throws<ArgumentNullException>(() => new KafkaContext(null, 0, 0, "k", headers, null!, services, default));
        Assert.Throws<ArgumentNullException>(() => new KafkaContext(null, 0, 0, "k", headers, payload, null!, default));
    }
}
