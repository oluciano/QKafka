using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace QKafka;

/// <summary>
/// Builds a pipeline of middlewares for message consumption.
/// </summary>
public sealed class KafkaPipelineBuilder
{
    private readonly List<Func<KafkaConsumerDelegate, KafkaConsumerDelegate>> _components = new();

    /// <summary>
    /// Adds a middleware component to the pipeline.
    /// </summary>
    /// <param name="middleware">The middleware logic function.</param>
    /// <returns>The pipeline builder instance for chaining.</returns>
    public KafkaPipelineBuilder Use(Func<IKafkaContext, Func<Task>, Task> middleware)
    {
        if (middleware == null)
        {
            throw new ArgumentNullException(nameof(middleware));
        }

        _components.Add(next => context => middleware(context, () => next(context)));
        return this;
    }

    /// <summary>
    /// Adds an <see cref="IKafkaConsumerMiddleware"/> component to the pipeline.
    /// </summary>
    /// <param name="middleware">The middleware instance.</param>
    /// <returns>The pipeline builder instance for chaining.</returns>
    public KafkaPipelineBuilder UseMiddleware(IKafkaConsumerMiddleware middleware)
    {
        if (middleware == null)
        {
            throw new ArgumentNullException(nameof(middleware));
        }

        return Use(middleware.InvokeAsync);
    }

    /// <summary>
    /// Builds the final execution delegate from the registered components.
    /// </summary>
    /// <param name="handler">The leaf execution handler.</param>
    /// <returns>The composed execution delegate.</returns>
    public KafkaConsumerDelegate Build(KafkaConsumerDelegate handler)
    {
        if (handler == null)
        {
            throw new ArgumentNullException(nameof(handler));
        }

        var current = handler;
        for (int i = _components.Count - 1; i >= 0; i--)
        {
            current = _components[i](current);
        }

        return current;
    }
}
