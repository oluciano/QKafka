using System;

namespace QKafka;

/// <summary>
/// Retry configuration options.
/// </summary>
public sealed class RetryOptions
{
    /// <summary>
    /// Gets or sets the maximum retry attempts.
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
