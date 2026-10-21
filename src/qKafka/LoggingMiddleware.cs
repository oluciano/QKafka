using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace QKafka;

/// <summary>
/// A middleware that opens a logging scope for the current message.
/// </summary>
public sealed class LoggingMiddleware : IKafkaConsumerMiddleware
{
    private readonly ILogger<LoggingMiddleware> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="LoggingMiddleware"/> class.
    /// </summary>
    /// <param name="logger">The logger instance.</param>
    public LoggingMiddleware(ILogger<LoggingMiddleware> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
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

        string messageId = "unknown";
        if (context.Headers.TryGetValue("qkafka-message-id", out var idBytes))
        {
            messageId = System.Text.Encoding.UTF8.GetString(idBytes);
        }

        var scopeState = new Dictionary<string, object>(StringComparer.Ordinal)
        {
            ["MessageId"] = messageId,
            ["Partition"] = context.Partition,
            ["Offset"] = context.Offset,
            ["Key"] = context.Key,
        };

        using (_logger.BeginScope(scopeState))
        {
            try
            {
                await next().ConfigureAwait(false);
            }
            #pragma warning disable S2139 // log with context here, rethrow so the caller decides (retry, DLT, no offset commit)
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing message. Partition: {Partition}, Offset: {Offset}", context.Partition, context.Offset);
                throw;
            }
            #pragma warning restore S2139
        }
    }
}
