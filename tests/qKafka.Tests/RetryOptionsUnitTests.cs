namespace QKafka.Tests;

public sealed class RetryOptionsUnitTests
{
    [Fact]
    public void NewInstance_Created_HasTheDocumentedDefaults()
    {
        var sut = new RetryOptions();

        Assert.Equal(3, sut.MaxRetryAttempts);
        Assert.Equal(250, sut.InitialDelayMs);
        Assert.Equal(2.0, sut.BackoffMultiplier);
        Assert.Equal(30000, sut.MaxDelayMs);
    }

    [Fact]
    public void Values_Assigned_AreReadBack()
    {
        var sut = new RetryOptions { MaxRetryAttempts = 7, InitialDelayMs = 10, BackoffMultiplier = 1.5, MaxDelayMs = 99 };

        Assert.Equal(7, sut.MaxRetryAttempts);
        Assert.Equal(10, sut.InitialDelayMs);
        Assert.Equal(1.5, sut.BackoffMultiplier);
        Assert.Equal(99, sut.MaxDelayMs);
    }

    [Fact]
    public void TwoInstances_Modified_DoNotShareState()
    {
        var first = new RetryOptions { MaxRetryAttempts = 9 };
        var second = new RetryOptions();

        Assert.Equal(3, second.MaxRetryAttempts);
        Assert.Equal(9, first.MaxRetryAttempts);
    }
}
