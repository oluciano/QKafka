using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace QKafka;

/// <summary>
/// A worker thread that pulls messages from a dedicated partition channel and executes them sequentially.
/// </summary>
internal sealed class PartitionWorker : IDisposable
{
    private readonly ChannelReader<ConsumeResult<byte[], byte[]>> _reader;
    private readonly Func<IKafkaContext, Task> _pipelineExecutor;
    private readonly Task _runningTask;
    private readonly CancellationTokenSource _cts;
    private bool _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="PartitionWorker"/> class.
    /// </summary>
    /// <param name="reader">The channel reader.</param>
    /// <param name="pipelineExecutor">The middleware/consumer execution pipeline.</param>
    public PartitionWorker(
        ChannelReader<ConsumeResult<byte[], byte[]>> reader,
        Func<IKafkaContext, Task> pipelineExecutor)
    {
        _reader = reader ?? throw new ArgumentNullException(nameof(reader));
        _pipelineExecutor = pipelineExecutor ?? throw new ArgumentNullException(nameof(pipelineExecutor));
        _cts = new CancellationTokenSource();
        _runningTask = Task.Run(() => RunLoopAsync(_cts.Token));
    }

    /// <summary>
    /// Stops the worker asynchronously.
    /// </summary>
    /// <returns>A task representing the asynchronous operation.</returns>
    public async Task StopAsync()
    {
        await _cts.CancelAsync().ConfigureAwait(false);
        try
        {
            await _runningTask.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // Expected during shutdown
        }
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _cts.Cancel();
        _cts.Dispose();

        _disposed = true;
    }

    private async Task RunLoopAsync(CancellationToken cancellationToken)
    {
        while (await _reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            while (_reader.TryRead(out var result))
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Extract headers
                var headers = new Dictionary<string, byte[]>(StringComparer.Ordinal);
                if (result.Message.Headers != null)
                {
                    foreach (var header in result.Message.Headers)
                    {
                        headers[header.Key] = header.GetValueBytes();
                    }
                }

                // Build execution context
                var key = result.Message.Key != null
                    ? System.Text.Encoding.UTF8.GetString(result.Message.Key)
                    : string.Empty;

                var context = new KafkaContext(
                    result.Partition.Value,
                    result.Offset.Value,
                    key,
                    headers,
                    cancellationToken);

                try
                {
                    // Execute pipeline (Logging -> OTel -> Scope -> Retry -> Consumer)
                    await _pipelineExecutor(context).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    // Exception is handled by RetryMiddleware, but swallowed here as a fallback
                    // to prevent crashing the channel loop.
                }
            }
        }
    }
}
