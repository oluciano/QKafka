namespace QKafka.Tests;

public sealed class ConsumerOptionsUnitTests
{
    [Fact]
    public void NewInstance_Created_HasTheDocumentedDefaults()
    {
        var sut = new ConsumerOptions();

        Assert.Equal(3, sut.ConcurrencyLimit);
        Assert.Null(sut.AutoOffsetReset);
        Assert.NotNull(sut.Retry);
        Assert.Equal(3, sut.Retry.MaxRetryAttempts);
    }

    [Fact]
    public void Values_Assigned_AreReadBack()
    {
        var retry = new RetryOptions { MaxRetryAttempts = 1 };

        var sut = new ConsumerOptions { ConcurrencyLimit = 8, AutoOffsetReset = "earliest", Retry = retry };

        Assert.Equal(8, sut.ConcurrencyLimit);
        Assert.Equal("earliest", sut.AutoOffsetReset);
        Assert.Same(retry, sut.Retry);
    }

    [Fact]
    public void TwoInstances_Created_DoNotShareTheDefaultRetryOptions()
    {
        var first = new ConsumerOptions();
        var second = new ConsumerOptions();

        first.Retry.MaxRetryAttempts = 99;

        Assert.NotSame(first.Retry, second.Retry);
        Assert.Equal(3, second.Retry.MaxRetryAttempts);
    }
}
