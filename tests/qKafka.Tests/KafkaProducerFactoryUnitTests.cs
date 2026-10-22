using Confluent.Kafka;

namespace QKafka.Tests;

public sealed class KafkaProducerFactoryUnitTests
{
    [Fact]
    public void Options_Configured_ProduceAcksAllWithIdempotence()
    {
        var config = KafkaProducerFactory.BuildConfig(new QKafkaOptions { BootstrapServers = "broker:9092" });

        Assert.Equal("broker:9092", config.BootstrapServers);
        Assert.Equal(Acks.All, config.Acks);
        Assert.True(config.EnableIdempotence);
    }

    [Fact]
    public void SaslOptions_Configured_AreCopiedAndParsedIgnoringCase()
    {
        var options = new QKafkaOptions
        {
            BootstrapServers = "broker:9092",
            SaslUsername = "user",
            SaslPassword = "secret",
            SaslMechanism = "scramsha256",
            SecurityProtocol = "saslssl",
        };

        var config = KafkaProducerFactory.BuildConfig(options);

        Assert.Equal("user", config.SaslUsername);
        Assert.Equal("secret", config.SaslPassword);
        Assert.Equal(SaslMechanism.ScramSha256, config.SaslMechanism);
        Assert.Equal(SecurityProtocol.SaslSsl, config.SecurityProtocol);
    }

    [Fact]
    public void NoSaslOptions_Configured_LeavesThemUnset()
    {
        var config = KafkaProducerFactory.BuildConfig(new QKafkaOptions { BootstrapServers = "broker:9092" });

        Assert.Null(config.SaslMechanism);
        Assert.Null(config.SecurityProtocol);
    }

    [Fact]
    public void ValidOptions_Created_BuildsAProducerWithoutContactingTheBroker()
    {
        using var producer = KafkaProducerFactory.Create(new QKafkaOptions { BootstrapServers = "localhost:1" });

        Assert.NotNull(producer);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void BlankBootstrapServers_Configured_ThrowsInvalidOperation(string servers)
    {
        Assert.Throws<InvalidOperationException>(() => KafkaProducerFactory.BuildConfig(new QKafkaOptions { BootstrapServers = servers }));
    }

    [Fact]
    public void NullOptions_Configured_ThrowsArgumentNull()
    {
        Assert.Throws<ArgumentNullException>(() => KafkaProducerFactory.BuildConfig(null!));
    }

    [Fact]
    public void UnknownSaslMechanism_Configured_ThrowsArgumentException()
    {
        var options = new QKafkaOptions { BootstrapServers = "broker:9092", SaslMechanism = "nope" };

        Assert.Throws<ArgumentException>(() => KafkaProducerFactory.BuildConfig(options));
    }
}
