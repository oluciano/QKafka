using System.Text;
using Microsoft.Extensions.Logging;
using QKafka.Tests.Support;

namespace QKafka.Tests;

public sealed class LoggingMiddlewareUnitTests
{
    [Fact]
    public async Task MessageWithId_Invoke_OpensAScopeCarryingIdPartitionOffsetAndKey()
    {
        var logger = new ListLogger<LoggingMiddleware>();
        var headers = new Dictionary<string, byte[]>(StringComparer.Ordinal) { ["qkafka-message-id"] = Encoding.UTF8.GetBytes("msg-9") };
        var context = TestKafka.Context(TestKafka.Message("order-7", offset: 12, partition: 2), headers: headers);

        await new LoggingMiddleware(logger).InvokeAsync(context, () => Task.CompletedTask);

        var scope = Assert.IsType<Dictionary<string, object>>(Assert.Single(logger.Scopes));
        Assert.Equal("msg-9", scope["MessageId"]);
        Assert.Equal(2, scope["Partition"]);
        Assert.Equal(12L, scope["Offset"]);
        Assert.Equal("order-7", scope["Key"]);
    }

    [Fact]
    public async Task MessageWithoutId_Invoke_UsesUnknown()
    {
        var logger = new ListLogger<LoggingMiddleware>();

        await new LoggingMiddleware(logger).InvokeAsync(TestKafka.Context(TestKafka.Message("k")), () => Task.CompletedTask);

        var scope = Assert.IsType<Dictionary<string, object>>(Assert.Single(logger.Scopes));
        Assert.Equal("unknown", scope["MessageId"]);
    }

    [Fact]
    public async Task NextSucceeds_Invoke_LogsNoError()
    {
        var logger = new ListLogger<LoggingMiddleware>();

        await new LoggingMiddleware(logger).InvokeAsync(TestKafka.Context(TestKafka.Message("k")), () => Task.CompletedTask);

        Assert.DoesNotContain(logger.Entries, e => e.Level == LogLevel.Error);
    }

    [Fact]
    public async Task NextThrows_Invoke_LogsAnErrorAndRethrowsTheSameException()
    {
        var logger = new ListLogger<LoggingMiddleware>();
        var failure = new InvalidOperationException("boom");

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            new LoggingMiddleware(logger).InvokeAsync(TestKafka.Context(TestKafka.Message("k")), () => throw failure));

        Assert.Same(failure, thrown);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error);
    }

    [Fact]
    public async Task NullContext_Invoke_ThrowsArgumentNull()
    {
        var sut = new LoggingMiddleware(new ListLogger<LoggingMiddleware>());

        await Assert.ThrowsAsync<ArgumentNullException>(() => sut.InvokeAsync(null!, () => Task.CompletedTask));
    }

    [Fact]
    public async Task NullNext_Invoke_ThrowsArgumentNull()
    {
        var sut = new LoggingMiddleware(new ListLogger<LoggingMiddleware>());

        await Assert.ThrowsAsync<ArgumentNullException>(() => sut.InvokeAsync(new StubKafkaContext(), null!));
    }

    [Fact]
    public void NullLogger_Constructed_ThrowsArgumentNull()
    {
        Assert.Throws<ArgumentNullException>(() => new LoggingMiddleware(null!));
    }
}
