using System.Collections.Generic;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace QKafka;

/// <summary>
/// Defines hooks to react to partition assignments and rebalances.
/// </summary>
public interface IPartitionAssignmentListener
{
    /// <summary>
    /// Triggered when partitions are assigned to this consumer group instance.
    /// </summary>
    /// <param name="context">The execution context.</param>
    /// <param name="partitions">The assigned partitions.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task OnPartitionsAssignedAsync(IKafkaContext context, IEnumerable<TopicPartition> partitions);

    /// <summary>
    /// Triggered when partitions are revoked from this consumer group instance.
    /// </summary>
    /// <param name="context">The execution context.</param>
    /// <param name="partitions">The revoked partition offsets.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task OnPartitionsRevokedAsync(IKafkaContext context, IEnumerable<TopicPartitionOffset> partitions);
}
