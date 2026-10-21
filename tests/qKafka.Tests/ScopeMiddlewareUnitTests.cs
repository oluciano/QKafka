using Microsoft.Extensions.DependencyInjection;
using QKafka.Tests.Support;

namespace QKafka.Tests;

public sealed class ScopeMiddlewareUnitTests
{
    [Fact]
    public async Task Invoke_RunningNext_ExposesAScopedProviderThenRestoresTheOriginal()
    {
        var root = new ServiceCollection().AddScoped<object>(_ => new object()).BuildServiceProvider();
        var original = new ServiceCollection().BuildServiceProvider();
        var context = TestKafka.Context(TestKafka.Message("k"), services: original);
        var sut = new ScopeMiddleware(root);
        IServiceProvider? seen = null;

        await sut.InvokeAsync(context, () =>
        {
            seen = context.RequestServices;
            return Task.CompletedTask;
        });

        Assert.NotNull(seen);
        Assert.NotSame(original, seen);
        Assert.Same(original, context.RequestServices);
    }

    [Fact]
    public async Task Invoke_EachMessage_GetsItsOwnScopeAndTheScopeIsDisposed()
    {
        var root = new ServiceCollection().AddScoped<DisposableProbe>().BuildServiceProvider();
        var sut = new ScopeMiddleware(root);
        var probes = new List<DisposableProbe>();

        for (var i = 0; i < 2; i++)
        {
            var context = TestKafka.Context(TestKafka.Message("k", i));
            await sut.InvokeAsync(context, () =>
            {
                probes.Add(context.RequestServices.GetRequiredService<DisposableProbe>());
                return Task.CompletedTask;
            });
        }

        Assert.NotSame(probes[0], probes[1]);
        Assert.All(probes, p => Assert.True(p.Disposed));
    }

    [Fact]
    public async Task NextThrows_Invoke_RestoresTheOriginalProviderAndPropagates()
    {
        var original = new ServiceCollection().BuildServiceProvider();
        var context = TestKafka.Context(TestKafka.Message("k"), services: original);
        var sut = new ScopeMiddleware(new ServiceCollection().BuildServiceProvider());

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            sut.InvokeAsync(context, () => throw new InvalidOperationException("boom")));

        Assert.Same(original, context.RequestServices);
    }

    [Fact]
    public async Task NonKafkaContext_Invoke_StillRunsNext()
    {
        var sut = new ScopeMiddleware(new ServiceCollection().BuildServiceProvider());
        var ran = false;

        await sut.InvokeAsync(new StubKafkaContext(), () =>
        {
            ran = true;
            return Task.CompletedTask;
        });

        Assert.True(ran);
    }

    [Fact]
    public async Task NullContext_Invoke_ThrowsArgumentNull()
    {
        var sut = new ScopeMiddleware(new ServiceCollection().BuildServiceProvider());

        await Assert.ThrowsAsync<ArgumentNullException>(() => sut.InvokeAsync(null!, () => Task.CompletedTask));
    }

    [Fact]
    public async Task NullNext_Invoke_ThrowsArgumentNull()
    {
        var sut = new ScopeMiddleware(new ServiceCollection().BuildServiceProvider());

        await Assert.ThrowsAsync<ArgumentNullException>(() => sut.InvokeAsync(new StubKafkaContext(), null!));
    }

    [Fact]
    public void NullServiceProvider_Constructed_ThrowsArgumentNull()
    {
        Assert.Throws<ArgumentNullException>(() => new ScopeMiddleware(null!));
    }
}
