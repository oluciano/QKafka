using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace QKafka;

/// <summary>
/// Tells every registered <see cref="IPartitionAssignmentListener"/> about partition assignment and revocation,
/// isolating a failing listener from the others and from the Kafka client thread.
/// </summary>
internal sealed class PartitionAssignmentNotifier
{
    private readonly IServiceProvider _serviceProvider;
    private readonly ILogger _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="PartitionAssignmentNotifier"/> class.
    /// </summary>
    /// <param name="serviceProvider">The root service provider; a scope is created per notification.</param>
    /// <param name="logger">The logger instance.</param>
    public PartitionAssignmentNotifier(IServiceProvider serviceProvider, ILogger logger)
    {
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    /// <summary>
    /// Notifies the listeners that partitions were assigned.
    /// </summary>
    /// <param name="partitions">The assigned partitions.</param>
    /// <returns>A task that completes when every listener was notified.</returns>
    public Task NotifyAssignedAsync(IEnumerable<TopicPartition> partitions) =>
        NotifyAsync((listener, context) => listener.OnPartitionsAssignedAsync(context, partitions), "Error in partition assignment listener.");

    /// <summary>
    /// Notifies the listeners that partitions were revoked.
    /// </summary>
    /// <param name="partitions">The revoked partitions.</param>
    /// <returns>A task that completes when every listener was notified.</returns>
    public Task NotifyRevokedAsync(IEnumerable<TopicPartitionOffset> partitions) =>
        NotifyAsync((listener, context) => listener.OnPartitionsRevokedAsync(context, partitions), "Error in partition revocation listener.");

    private async Task NotifyAsync(Func<IPartitionAssignmentListener, IKafkaContext, Task> notify, string failureMessage)
    {
        using var scope = _serviceProvider.CreateScope();
        var context = new KafkaContext(
            null,
            -1,
            -1,
            string.Empty,
            new Dictionary<string, byte[]>(StringComparer.Ordinal),
            Array.Empty<byte>(),
            scope.ServiceProvider,
            CancellationToken.None);

        foreach (var listener in scope.ServiceProvider.GetServices<IPartitionAssignmentListener>())
        {
            try
            {
                await notify(listener, context).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "{Message}", failureMessage);
            }
        }
    }
}
