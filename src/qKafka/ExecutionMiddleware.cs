using System;
using System.Threading.Tasks;

namespace QKafka;

/// <summary>
/// A pipeline middleware that deserializes the message and executes the consumer.
/// </summary>
/// <typeparam name="TConsumer">The type of the consumer class.</typeparam>
/// <typeparam name="TMessage">The type of the message.</typeparam>
internal sealed class ExecutionMiddleware<TConsumer, TMessage> : IKafkaConsumerMiddleware
    where TConsumer : class, IKafkaConsumer<TMessage>
    where TMessage : class
{
    private readonly IServiceProvider _serviceProvider;
    private readonly IKafkaMessageDeserializer _deserializer;

    /// <summary>
    /// Initializes a new instance of the <see cref="ExecutionMiddleware{TConsumer, TMessage}"/> class.
    /// </summary>
    /// <param name="serviceProvider">The service provider.</param>
    /// <param name="deserializer">The message deserializer.</param>
    public ExecutionMiddleware(IServiceProvider serviceProvider, IKafkaMessageDeserializer deserializer)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _deserializer = deserializer ?? throw new ArgumentNullException(nameof(deserializer));
    }

    /// <inheritdoc/>
    public async Task InvokeAsync(IKafkaContext context, Func<Task> next)
    {
        if (context == null)
        {
            throw new ArgumentNullException(nameof(context));
        }

        if (next == null)
        {
            throw new ArgumentNullException(nameof(next));
        }

        var consumer = _serviceProvider.GetService(typeof(TConsumer)) as IKafkaConsumer<TMessage>;
        if (consumer == null)
        {
            throw new InvalidOperationException($"Could not resolve consumer of type {typeof(TConsumer).FullName} from the service provider.");
        }

        var message = (TMessage)_deserializer.Deserialize(context.Payload, typeof(TMessage));

        await consumer.ConsumeAsync(message, context).ConfigureAwait(false);

        await next().ConfigureAwait(false);
    }
}
