namespace QKafka;

/// <summary>
/// Configuration overrides for a specific consumer group.
/// </summary>
public sealed class ConsumerOptions
{
    /// <summary>
    /// Gets or sets the maximum concurrency (number of partition workers).
    /// </summary>
    public int ConcurrencyLimit { get; set; } = 3;

    /// <summary>
    /// Gets or sets the auto offset reset strategy.
    /// </summary>
    public string? AutoOffsetReset { get; set; }

    /// <summary>
    /// Gets or sets the retry configuration options for this consumer.
    /// </summary>
    public RetryOptions Retry { get; set; } = new();
}
