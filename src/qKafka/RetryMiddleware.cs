using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace QKafka;

/// <summary>
/// A pipeline middleware that handles retries with exponential backoff and routes to Dead Letter Topic on failure.
/// </summary>
public sealed class RetryMiddleware : IKafkaConsumerMiddleware
{
    private readonly RetryOptions _options;
    private readonly IDltPublisher _dltPublisher;
    private readonly ILogger<RetryMiddleware> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="RetryMiddleware"/> class.
    /// </summary>
    /// <param name="options">The retry configuration options.</param>
    /// <param name="dltPublisher">The dead letter topic publisher.</param>
    /// <param name="logger">The logger instance.</param>
    public RetryMiddleware(RetryOptions options, IDltPublisher dltPublisher, ILogger<RetryMiddleware> logger)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _dltPublisher = dltPublisher ?? throw new ArgumentNullException(nameof(dltPublisher));
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

        int attempts = 0;
        int maxAttempts = _options.MaxRetryAttempts >= 0 ? _options.MaxRetryAttempts : 0;
        double backoffMultiplier = _options.BackoffMultiplier > 0 ? _options.BackoffMultiplier : 1.0;
        int initialDelayMs = _options.InitialDelayMs >= 0 ? _options.InitialDelayMs : 0;
        int maxDelayMs = _options.MaxDelayMs >= 0 ? _options.MaxDelayMs : 0;

        while (true)
        {
            try
            {
                attempts++;
                await next().ConfigureAwait(false);
                return;
            }
            catch (Exception ex)
            {
                if (attempts > maxAttempts)
                {
                    _logger.LogError(
                        ex,
                        "Message execution failed after {Attempts} attempts. Routing to Dead Letter Topic.",
                        attempts);

                    if (context is KafkaContext kafkaContext && kafkaContext.ConsumeResult != null)
                    {
                        try
                        {
                            await _dltPublisher.PublishAsync(
                                kafkaContext.ConsumeResult,
                                DltReason.MaxRetriesExceeded,
                                ex,
                                attempts,
                                context.CancellationToken).ConfigureAwait(false);

                            // Swallowing the exception so the offset is committed (since it's now in the DLT)
                            return;
                        }
                        #pragma warning disable S2139 // log with context here, rethrow so the caller decides (retry, DLT, no offset commit)
                        catch (Exception dltEx)
                        {
                            _logger.LogError(dltEx, "Failed to publish message to Dead Letter Topic. Offset will not be committed.");
                            throw;
                        }
                        #pragma warning restore S2139
                    }

                    throw;
                }

                var delayMs = Math.Min(initialDelayMs * Math.Pow(backoffMultiplier, attempts - 1), maxDelayMs);
                _logger.LogWarning(
                    ex,
                    "Message consumption attempt {Attempts} failed. Retrying in {Delay}ms.",
                    attempts,
                    delayMs);

                await Task.Delay(TimeSpan.FromMilliseconds(delayMs), context.CancellationToken).ConfigureAwait(false);
            }
        }
    }
}
