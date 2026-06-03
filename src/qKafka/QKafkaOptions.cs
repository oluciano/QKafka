using System;
using System.Collections.Generic;

namespace QKafka;

/// <summary>
/// Global configuration options for qKafka.
/// </summary>
public sealed class QKafkaOptions
{
    /// <summary>
    /// Gets or sets the bootstrap servers.
    /// </summary>
    public string BootstrapServers { get; set; } = string.Empty;

    /// <summary>
    /// Gets or sets the SASL username.
    /// </summary>
    public string? SaslUsername { get; set; }

    /// <summary>
    /// Gets or sets the SASL password.
    /// </summary>
    public string? SaslPassword { get; set; }

    /// <summary>
    /// Gets or sets the SASL mechanism.
    /// </summary>
    public string? SaslMechanism { get; set; }

    /// <summary>
    /// Gets or sets the security protocol.
    /// </summary>
    public string? SecurityProtocol { get; set; }

    /// <summary>
    /// Gets or sets the global retry options.
    /// </summary>
    public RetryOptions Retry { get; set; } = new();

    /// <summary>
    /// Gets the dictionary of consumer overrides by Group ID.
    /// </summary>
    public Dictionary<string, ConsumerOptions> Consumers { get; } = new(StringComparer.Ordinal);
}
