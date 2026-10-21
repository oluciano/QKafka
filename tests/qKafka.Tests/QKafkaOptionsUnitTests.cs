namespace QKafka.Tests;

public sealed class QKafkaOptionsUnitTests
{
    [Fact]
    public void NewInstance_Created_HasSafeDefaults()
    {
        var sut = new QKafkaOptions();

        Assert.Equal(string.Empty, sut.BootstrapServers);
        Assert.Null(sut.SaslUsername);
        Assert.Null(sut.SaslPassword);
        Assert.Null(sut.SaslMechanism);
        Assert.Null(sut.SecurityProtocol);
        Assert.NotNull(sut.Retry);
        Assert.Empty(sut.Consumers);
    }

    [Fact]
    public void Values_Assigned_AreReadBack()
    {
        var sut = new QKafkaOptions
        {
            BootstrapServers = "broker:9092",
            SaslUsername = "user",
            SaslPassword = "secret",
            SaslMechanism = "Plain",
            SecurityProtocol = "SaslSsl",
        };

        Assert.Equal("broker:9092", sut.BootstrapServers);
        Assert.Equal("user", sut.SaslUsername);
        Assert.Equal("secret", sut.SaslPassword);
        Assert.Equal("Plain", sut.SaslMechanism);
        Assert.Equal("SaslSsl", sut.SecurityProtocol);
    }

    [Fact]
    public void Consumers_ConfiguredPerName_AreKeptSeparately()
    {
        var sut = new QKafkaOptions();

        sut.Consumers["orders"] = new ConsumerOptions { ConcurrencyLimit = 1 };
        sut.Consumers["audit"] = new ConsumerOptions { ConcurrencyLimit = 5 };

        Assert.Equal(1, sut.Consumers["orders"].ConcurrencyLimit);
        Assert.Equal(5, sut.Consumers["audit"].ConcurrencyLimit);
    }

    [Fact]
    public void ConsumerNames_DifferingOnlyByCase_AreDistinctKeys()
    {
        var sut = new QKafkaOptions();

        sut.Consumers["Orders"] = new ConsumerOptions();

        Assert.False(sut.Consumers.ContainsKey("orders"));
    }

    [Fact]
    public void TwoInstances_Created_DoNotShareConsumersOrRetry()
    {
        var first = new QKafkaOptions();
        var second = new QKafkaOptions();

        first.Consumers["x"] = new ConsumerOptions();
        first.Retry.MaxRetryAttempts = 99;

        Assert.Empty(second.Consumers);
        Assert.Equal(3, second.Retry.MaxRetryAttempts);
    }
}
