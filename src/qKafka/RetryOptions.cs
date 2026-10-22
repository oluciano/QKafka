using System;

namespace QKafka;

/// <summary>
/// Retry configuration options.
/// </summary>
public sealed class RetryOptions
{
    /// <summary>
    /// Gets or sets how many times a failed message is retried after its first execution.
    /// The handler runs at most <c>1 + MaxRetryAttempts</c> times before the message is routed to the
    /// Dead Letter Topic (the default of 3 means 4 executions). <c>0</c> disables retries; a negative value is treated as <c>0</c>.
    /// </summary>
    public int MaxRetryAttempts { get; set; } = 3;

    /// <summary>
    /// Gets or sets the initial retry delay in milliseconds.
    /// </summary>
    public int InitialDelayMs { get; set; } = 250;

    /// <summary>
    /// Gets or sets the backoff multiplier.
    /// </summary>
    public double BackoffMultiplier { get; set; } = 2.0;

    /// <summary>
    /// Gets or sets the maximum delay in milliseconds.
    /// </summary>
    public int MaxDelayMs { get; set; } = 30000;
}
