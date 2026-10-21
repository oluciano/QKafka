namespace QKafka.Tests.Support;

/// <summary>An <see cref="IKafkaConsumerMiddleware"/> backed by a delegate.</summary>
internal sealed class DelegateMiddleware : IKafkaConsumerMiddleware
{
    private readonly Func<IKafkaContext, Func<Task>, Task> _invoke;

    public DelegateMiddleware(Func<IKafkaContext, Func<Task>, Task> invoke) => _invoke = invoke;

    public Task InvokeAsync(IKafkaContext context, Func<Task> next) => _invoke(context, next);
}
