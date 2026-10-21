using Microsoft.Extensions.Logging;
using QKafka.Tests.Support;

namespace QKafka.Tests;

public sealed class RetryMiddlewareUnitTests
{
    private static RetryOptions Fast(int max = 3) => new()
    {
        MaxRetryAttempts = max,
        InitialDelayMs = 0,
        MaxDelayMs = 0,
    };

    [Fact]
    public async Task NextSucceeds_OnFirstAttempt_NoRetryAndNoDlt()
    {
        var dlt = new RecordingDltPublisher();
        var sut = new RetryMiddleware(Fast(), dlt, new ListLogger<RetryMiddleware>());
        var calls = 0;

        await sut.InvokeAsync(TestKafka.Context(TestKafka.Message("k")), () =>
        {
            calls++;
            return Task.CompletedTask;
        });

        Assert.Equal(1, calls);
        Assert.Empty(dlt.Published);
    }

    [Fact]
    public async Task FailsTwiceThenSucceeds_Retried_CompletesWithoutDlt()
    {
        var dlt = new RecordingDltPublisher();
        var sut = new RetryMiddleware(Fast(), dlt, new ListLogger<RetryMiddleware>());
        var calls = 0;

        await sut.InvokeAsync(TestKafka.Context(TestKafka.Message("k")), () =>
        {
            calls++;
            return calls < 3 ? throw new InvalidOperationException("boom") : Task.CompletedTask;
        });

        Assert.Equal(3, calls);
        Assert.Empty(dlt.Published);
    }

    [Fact]
    public async Task Backoff_GrowsByMultiplier_AndIsCappedAtMaxDelay()
    {
        var logger = new ListLogger<RetryMiddleware>();
        var options = new RetryOptions { MaxRetryAttempts = 4, InitialDelayMs = 1, BackoffMultiplier = 2, MaxDelayMs = 4 };
        var sut = new RetryMiddleware(options, new RecordingDltPublisher(), logger);

        await sut.InvokeAsync(TestKafka.Context(TestKafka.Message("k")), () => throw new InvalidOperationException("boom"));

        Assert.Equal(new[] { 1.0, 2.0, 4.0, 4.0 }, logger.LoggedDelays());
    }

    [Fact]
    public async Task AlwaysFails_RetriesExhausted_RoutesToDltOnceAndSwallows()
    {
        var dlt = new RecordingDltPublisher();
        var sut = new RetryMiddleware(Fast(2), dlt, new ListLogger<RetryMiddleware>());
        var calls = 0;
        var failure = new InvalidOperationException("boom");

        await sut.InvokeAsync(TestKafka.Context(TestKafka.Message("k")), () =>
        {
            calls++;
            throw failure;
        });

        Assert.Equal(3, calls);
        var published = Assert.Single(dlt.Published);
        Assert.Equal(DltReason.MaxRetriesExceeded, published.Reason);
        Assert.Equal(3, published.Attempts);
        Assert.Same(failure, published.Error);
    }

    [Fact]
    public async Task DltPublishFails_RetriesExhausted_PropagatesSoTheOffsetIsNotCommitted()
    {
        var dlt = new RecordingDltPublisher { FailWith = new TimeoutException("broker down") };
        var sut = new RetryMiddleware(Fast(1), dlt, new ListLogger<RetryMiddleware>());

        await Assert.ThrowsAsync<TimeoutException>(() =>
            sut.InvokeAsync(TestKafka.Context(TestKafka.Message("k")), () => throw new InvalidOperationException("boom")));
    }

    [Fact]
    public async Task NoConsumeResult_RetriesExhausted_RethrowsOriginalAndSkipsDlt()
    {
        var dlt = new RecordingDltPublisher();
        var sut = new RetryMiddleware(Fast(1), dlt, new ListLogger<RetryMiddleware>());

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            sut.InvokeAsync(TestKafka.Context(null), () => throw new InvalidOperationException("boom")));

        Assert.Empty(dlt.Published);
    }

    [Fact]
    public async Task CancelledDuringBackoff_Retrying_ThrowsOperationCanceled()
    {
        using var cts = new CancellationTokenSource();
        var options = new RetryOptions { MaxRetryAttempts = 3, InitialDelayMs = 60_000, MaxDelayMs = 60_000 };
        var sut = new RetryMiddleware(options, new RecordingDltPublisher(), new ListLogger<RetryMiddleware>());

        var running = sut.InvokeAsync(TestKafka.Context(TestKafka.Message("k"), cts.Token), () => throw new InvalidOperationException("boom"));
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running);
    }

    [Fact]
    public async Task NullContext_Invoked_ThrowsArgumentNull()
    {
        var sut = new RetryMiddleware(Fast(), new RecordingDltPublisher(), new ListLogger<RetryMiddleware>());

        await Assert.ThrowsAsync<ArgumentNullException>(() => sut.InvokeAsync(null!, () => Task.CompletedTask));
    }

    [Fact]
    public async Task NullNext_Invoked_ThrowsArgumentNull()
    {
        var sut = new RetryMiddleware(Fast(), new RecordingDltPublisher(), new ListLogger<RetryMiddleware>());

        await Assert.ThrowsAsync<ArgumentNullException>(() => sut.InvokeAsync(TestKafka.Context(TestKafka.Message("k")), null!));
    }

    [Fact]
    public void NullDependencies_Constructed_ThrowArgumentNull()
    {
        var logger = new ListLogger<RetryMiddleware>();
        var dlt = new RecordingDltPublisher();

        Assert.Throws<ArgumentNullException>(() => new RetryMiddleware(null!, dlt, logger));
        Assert.Throws<ArgumentNullException>(() => new RetryMiddleware(Fast(), null!, logger));
        Assert.Throws<ArgumentNullException>(() => new RetryMiddleware(Fast(), dlt, null!));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-3)]
    public async Task ZeroOrNegativeMaxRetries_Failing_RunsOnceThenRoutesToDlt(int max)
    {
        var dlt = new RecordingDltPublisher();
        var sut = new RetryMiddleware(Fast(max), dlt, new ListLogger<RetryMiddleware>());
        var calls = 0;

        await sut.InvokeAsync(TestKafka.Context(TestKafka.Message("k")), () =>
        {
            calls++;
            throw new InvalidOperationException("boom");
        });

        Assert.Equal(1, calls);
        Assert.Equal(1, Assert.Single(dlt.Published).Attempts);
    }

    [Fact]
    public async Task NonPositiveMultiplier_Backoff_FallsBackToConstantDelay()
    {
        var logger = new ListLogger<RetryMiddleware>();
        var options = new RetryOptions { MaxRetryAttempts = 3, InitialDelayMs = 2, BackoffMultiplier = 0, MaxDelayMs = 100 };
        var sut = new RetryMiddleware(options, new RecordingDltPublisher(), logger);

        await sut.InvokeAsync(TestKafka.Context(TestKafka.Message("k")), () => throw new InvalidOperationException("boom"));

        Assert.Equal(new[] { 2.0, 2.0, 2.0 }, logger.LoggedDelays());
    }
}
