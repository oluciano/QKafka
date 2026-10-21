using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace QKafka;

/// <summary>
/// Defines a publisher that routes failed messages to a Dead Letter Topic.
/// </summary>
internal interface IDltPublisher
{
    /// <summary>
    /// Routes a failed message to its Dead Letter Topic with enriched diagnostic headers.
    /// </summary>
    /// <param name="original">The original consume result that failed.</param>
    /// <param name="reason">The reason for sending the message to DLT.</param>
    /// <param name="exception">The exception that caused the failure, if any.</param>
    /// <param name="attemptCount">The number of processing attempts made before routing to DLT.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous operation.</returns>
    Task PublishAsync(
        ConsumeResult<byte[], byte[]> original,
        DltReason reason,
        Exception? exception,
        int attemptCount,
        CancellationToken cancellationToken);
}
