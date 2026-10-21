using QKafka.Tests.Support;

namespace QKafka.Tests;

public sealed class KafkaPipelineBuilderUnitTests
{
    private static readonly KafkaContext Context = TestKafka.Context(TestKafka.Message("k"));

    [Fact]
    public async Task NoMiddleware_Built_RunsOnlyTheHandler()
    {
        var ran = false;

        var pipeline = new KafkaPipelineBuilder().Build(_ =>
        {
            ran = true;
            return Task.CompletedTask;
        });
        await pipeline(Context);

        Assert.True(ran);
    }

    [Fact]
    public async Task Middlewares_Registered_RunOuterToInnerInRegistrationOrderAndUnwindInReverse()
    {
        var trace = new List<string>();
        var builder = new KafkaPipelineBuilder()
            .Use(async (_, next) =>
            {
                trace.Add("A-in");
                await next();
                trace.Add("A-out");
            })
            .Use(async (_, next) =>
            {
                trace.Add("B-in");
                await next();
                trace.Add("B-out");
            });

        await builder.Build(_ =>
        {
            trace.Add("handler");
            return Task.CompletedTask;
        })(Context);

        Assert.Equal(new[] { "A-in", "B-in", "handler", "B-out", "A-out" }, trace);
    }

    [Fact]
    public async Task MiddlewareThatDoesNotCallNext_Built_ShortCircuitsTheRest()
    {
        var handlerRan = false;
        var builder = new KafkaPipelineBuilder().Use((_, _) => Task.CompletedTask);

        await builder.Build(_ =>
        {
            handlerRan = true;
            return Task.CompletedTask;
        })(Context);

        Assert.False(handlerRan);
    }

    [Fact]
    public async Task MiddlewareInstance_Registered_IsInvokedLikeADelegate()
    {
        var invoked = false;
        var middleware = new DelegateMiddleware((_, next) =>
        {
            invoked = true;
            return next();
        });

        await new KafkaPipelineBuilder().UseMiddleware(middleware).Build(_ => Task.CompletedTask)(Context);

        Assert.True(invoked);
    }

    [Fact]
    public async Task HandlerThrows_Built_PropagatesThroughTheMiddlewares()
    {
        var builder = new KafkaPipelineBuilder().Use((ctx, next) => next());

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            builder.Build(_ => throw new InvalidOperationException("boom"))(Context));
    }

    [Fact]
    public void Use_Called_ReturnsTheSameBuilderForChaining()
    {
        var builder = new KafkaPipelineBuilder();

        Assert.Same(builder, builder.Use((_, next) => next()));
        Assert.Same(builder, builder.UseMiddleware(new DelegateMiddleware((_, next) => next())));
    }

    [Fact]
    public void NullArguments_Used_ThrowArgumentNull()
    {
        var builder = new KafkaPipelineBuilder();

        Assert.Throws<ArgumentNullException>(() => builder.Use(null!));
        Assert.Throws<ArgumentNullException>(() => builder.UseMiddleware(null!));
        Assert.Throws<ArgumentNullException>(() => builder.Build(null!));
    }
}
