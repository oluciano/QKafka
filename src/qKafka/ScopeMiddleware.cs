using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace QKafka;

/// <summary>
/// A middleware that creates a Dependency Injection scope for each message execution.
/// </summary>
public sealed class ScopeMiddleware : IKafkaConsumerMiddleware
{
    private readonly IServiceProvider _serviceProvider;

    /// <summary>
    /// Initializes a new instance of the <see cref="ScopeMiddleware"/> class.
    /// </summary>
    /// <param name="serviceProvider">The root service provider.</param>
    public ScopeMiddleware(IServiceProvider serviceProvider)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
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

        using (var scope = _serviceProvider.CreateScope())
        {
            var originalServiceProvider = context.RequestServices;
            try
            {
                if (context is KafkaContext kafkaContext)
                {
                    kafkaContext.RequestServices = scope.ServiceProvider;
                }

                await next().ConfigureAwait(false);
            }
            finally
            {
                if (context is KafkaContext kafkaContext)
                {
                    kafkaContext.RequestServices = originalServiceProvider;
                }
            }
        }
    }
}
