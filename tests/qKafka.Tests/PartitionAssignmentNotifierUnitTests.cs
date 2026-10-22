using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using QKafka.Tests.Support;

namespace QKafka.Tests;

public sealed class PartitionAssignmentNotifierUnitTests
{
    private readonly ListLogger<PartitionAssignmentNotifier> _logger = new();

    private sealed class RecordingListener : IPartitionAssignmentListener
    {
        public List<TopicPartition> Assigned { get; } = new();

        public List<TopicPartitionOffset> Revoked { get; } = new();

        public List<IServiceProvider> Scopes { get; } = new();

        public Task OnPartitionsAssignedAsync(IKafkaContext context, IEnumerable<TopicPartition> partitions)
        {
            Scopes.Add(context.RequestServices);
            Assigned.AddRange(partitions);
            return Task.CompletedTask;
        }

        public Task OnPartitionsRevokedAsync(IKafkaContext context, IEnumerable<TopicPartitionOffset> partitions)
        {
            Revoked.AddRange(partitions);
            return Task.CompletedTask;
        }
    }

    private sealed class ThrowingListener : IPartitionAssignmentListener
    {
        public Task OnPartitionsAssignedAsync(IKafkaContext context, IEnumerable<TopicPartition> partitions) => throw new InvalidOperationException("listener down");

        public Task OnPartitionsRevokedAsync(IKafkaContext context, IEnumerable<TopicPartitionOffset> partitions) => throw new InvalidOperationException("listener down");
    }

    private PartitionAssignmentNotifier Create(params IPartitionAssignmentListener[] listeners)
    {
        var services = new ServiceCollection();
        foreach (var listener in listeners)
        {
            services.AddSingleton(listener);
        }

        return new PartitionAssignmentNotifier(services.BuildServiceProvider(), _logger);
    }

    [Fact]
    public async Task Assignment_Notified_ReachesEveryListenerWithThePartitions()
    {
        var first = new RecordingListener();
        var second = new RecordingListener();
        var partition = new TopicPartition("t", new Partition(2));

        await Create(first, second).NotifyAssignedAsync(new[] { partition });

        Assert.Equal(new[] { partition }, first.Assigned);
        Assert.Equal(new[] { partition }, second.Assigned);
    }

    [Fact]
    public async Task Revocation_Notified_ReachesTheListenerWithTheOffsets()
    {
        var listener = new RecordingListener();
        var revoked = new TopicPartitionOffset("t", new Partition(1), new Offset(40));

        await Create(listener).NotifyRevokedAsync(new[] { revoked });

        Assert.Equal(new[] { revoked }, listener.Revoked);
    }

    [Fact]
    public async Task Listeners_Notified_GetAScopedProviderThatIsNotTheRoot()
    {
        var listener = new RecordingListener();
        var root = new ServiceCollection().AddSingleton<IPartitionAssignmentListener>(listener).BuildServiceProvider();

        await new PartitionAssignmentNotifier(root, _logger).NotifyAssignedAsync(Array.Empty<TopicPartition>());

        Assert.NotSame(root, Assert.Single(listener.Scopes));
    }

    [Fact]
    public async Task FailingListener_Notified_IsLoggedAndTheOthersStillRun()
    {
        var healthy = new RecordingListener();
        var partition = new TopicPartition("t", new Partition(0));

        await Create(new ThrowingListener(), healthy).NotifyAssignedAsync(new[] { partition });

        Assert.Equal(new[] { partition }, healthy.Assigned);
        Assert.Contains(_logger.Entries, e => e.Level == Microsoft.Extensions.Logging.LogLevel.Error);
    }

    [Fact]
    public async Task FailingListener_RevocationNotified_IsLoggedAndTheOthersStillRun()
    {
        var healthy = new RecordingListener();
        var revoked = new TopicPartitionOffset("t", new Partition(0), new Offset(1));

        await Create(new ThrowingListener(), healthy).NotifyRevokedAsync(new[] { revoked });

        Assert.Equal(new[] { revoked }, healthy.Revoked);
        Assert.Contains(_logger.Entries, e => e.Level == Microsoft.Extensions.Logging.LogLevel.Error);
    }

    [Fact]
    public async Task NoListeners_Notified_CompletesQuietly()
    {
        await Create().NotifyAssignedAsync(new[] { new TopicPartition("t", new Partition(0)) });

        Assert.Empty(_logger.Entries);
    }

    [Fact]
    public void NullDependencies_Constructed_ThrowArgumentNull()
    {
        Assert.Throws<ArgumentNullException>(() => new PartitionAssignmentNotifier(null!, _logger));
        Assert.Throws<ArgumentNullException>(() => new PartitionAssignmentNotifier(new ServiceCollection().BuildServiceProvider(), null!));
    }
}
