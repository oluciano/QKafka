using System;
using System.Threading.Tasks;

namespace QKafka;

/// <summary>
/// Defines a middleware filter in the qKafka consumer execution pipeline.
/// </summary>
public interface IKafkaConsumerMiddleware
{
    /// <summary>
    /// Invokes the middleware logic.
    /// </summary>
    /// <param name="context">The execution context.</param>
    /// <param name="next">The next middleware pipeline delegate to invoke.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task InvokeAsync(IKafkaContext context, Func<Task> next);
}
