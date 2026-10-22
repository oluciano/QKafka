using System;
using Confluent.Kafka;

namespace QKafka;

/// <summary>
/// Builds the single Kafka producer shared by the publisher and the Dead Letter Topic publisher.
/// </summary>
internal static class KafkaProducerFactory
{
    /// <summary>
    /// Creates a producer from the global options.
    /// </summary>
    /// <param name="options">The global configuration options.</param>
    /// <returns>A producer configured with <c>acks=all</c> and idempotence enabled.</returns>
    public static IProducer<byte[], byte[]> Create(QKafkaOptions options) =>
        new ProducerBuilder<byte[], byte[]>(BuildConfig(options)).Build();

    /// <summary>
    /// Translates the global options into the producer configuration.
    /// </summary>
    /// <param name="options">The global configuration options.</param>
    /// <returns>The producer configuration.</returns>
    public static ProducerConfig BuildConfig(QKafkaOptions options)
    {
        if (options == null)
        {
            throw new ArgumentNullException(nameof(options));
        }

        if (string.IsNullOrWhiteSpace(options.BootstrapServers))
        {
            throw new InvalidOperationException("QKafkaOptions.BootstrapServers must be set before messages can be published.");
        }

        var config = new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            SaslUsername = options.SaslUsername,
            SaslPassword = options.SaslPassword,
            Acks = Acks.All,
            EnableIdempotence = true,
        };

        if (options.SaslMechanism != null)
        {
            config.SaslMechanism = Enum.Parse<SaslMechanism>(options.SaslMechanism, true);
        }

        if (options.SecurityProtocol != null)
        {
            config.SecurityProtocol = Enum.Parse<SecurityProtocol>(options.SecurityProtocol, true);
        }

        return config;
    }
}
