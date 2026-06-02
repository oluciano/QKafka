using System;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace QKafka;

/// <summary>
/// Dispatches consumed messages to bounded partition channels based on Partition Key hashing.
/// </summary>
public sealed class ChannelDispatcher
{
    private readonly Channel<ConsumeResult<byte[], byte[]>>[] _channels;
    private readonly int _maxConcurrency;

    /// <summary>
    /// Initializes a new instance of the <see cref="ChannelDispatcher"/> class.
    /// </summary>
    /// <param name="maxConcurrency">The maximum number of partition worker channels.</param>
    public ChannelDispatcher(int maxConcurrency)
    {
        _maxConcurrency = maxConcurrency > 0 ? maxConcurrency : Environment.ProcessorCount;
        _channels = new Channel<ConsumeResult<byte[], byte[]>>[_maxConcurrency];
        for (int i = 0; i < _maxConcurrency; i++)
        {
            _channels[i] = Channel.CreateBounded<ConsumeResult<byte[], byte[]>>(
                new BoundedChannelOptions(100)
                {
                    FullMode = BoundedChannelFullMode.Wait,
                    SingleReader = true,
                    SingleWriter = false,
                });
        }
    }

    /// <summary>
    /// Dispatches the consume result to a bounded channel.
    /// </summary>
    /// <param name="result">The message consume result from Kafka broker.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task representing the asynchronous dispatch operation.</returns>
    public async Task DispatchAsync(ConsumeResult<byte[], byte[]> result, CancellationToken cancellationToken)
    {
        if (result == null)
        {
            throw new ArgumentNullException(nameof(result));
        }

        // 1. Resolve key or generate a fallback key
        string keyString = result.Message.Key != null
            ? Encoding.UTF8.GetString(result.Message.Key)
            : Guid.NewGuid().ToString();

        // 2. Select channel bucket via key hash modulo max concurrency
        var bucketIndex = Math.Abs(GetDeterministicHashCode(keyString) % _maxConcurrency);
        var channel = _channels[bucketIndex];

        // 3. Write to the channel (blocking if full to enforce backpressure)
        await channel.Writer.WriteAsync(result, cancellationToken).ConfigureAwait(false);
    }

    private static int GetDeterministicHashCode(string str)
    {
        unchecked
        {
            int hash1 = (5381 << 16) + 5381;
            int hash2 = hash1;

            for (int i = 0; i < str.Length; i += 2)
            {
                hash1 = ((hash1 << 5) + hash1) ^ str[i];
                if (i + 1 < str.Length)
                {
                    hash2 = ((hash2 << 5) + hash2) ^ str[i + 1];
                }
            }

            return hash1 + (hash2 * 1566083941);
        }
    }
}
