using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace QKafka;

/// <summary>
/// A background service that manages the polling loop and rebalance protection for a specific consumer group.
/// </summary>
internal sealed class KafkaConsumerLoop : BackgroundService
{
    private readonly ConsumerRegistration _registration;
    private readonly QKafkaOptions _options;
    private readonly IServiceProvider _serviceProvider;
    private readonly IDltPublisher _dltPublisher;
    private readonly IKafkaMessageDeserializer _deserializer;
    private readonly IHostEnvironment? _environment;
    private readonly ILoggerFactory _loggerFactory;
    private readonly ILogger<KafkaConsumerLoop> _logger;

    /// <summary>
    /// Initializes a new instance of the <see cref="KafkaConsumerLoop"/> class.
    /// </summary>
    public KafkaConsumerLoop(
        ConsumerRegistration registration,
        QKafkaOptions options,
        IServiceProvider serviceProvider,
        IDltPublisher dltPublisher,
        IKafkaMessageDeserializer deserializer,
        ILoggerFactory loggerFactory,
        IHostEnvironment? environment = null)
    {
        _registration = registration ?? throw new ArgumentNullException(nameof(registration));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
        _dltPublisher = dltPublisher ?? throw new ArgumentNullException(nameof(dltPublisher));
        _deserializer = deserializer ?? throw new ArgumentNullException(nameof(deserializer));
        _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        _logger = loggerFactory.CreateLogger<KafkaConsumerLoop>();
        _environment = environment;
    }

    /// <inheritdoc/>
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        // 1. Topic auto-provisioning check
        await AutoProvisionTopicAsync(stoppingToken).ConfigureAwait(false);

        // 2. Resolve options
        _options.Consumers.TryGetValue(_registration.GroupId, out var consumerOptions);
        consumerOptions ??= new ConsumerOptions();

        int concurrencyLimit = consumerOptions.ConcurrencyLimit > 0 ? consumerOptions.ConcurrencyLimit : 3;

        // 3. Create dispatcher and workers
        var dispatcher = new ChannelDispatcher(concurrencyLimit);
        var retryOptions = consumerOptions.Retry;

        var builder = new KafkaPipelineBuilder();
        builder.UseMiddleware(new LoggingMiddleware(_loggerFactory.CreateLogger<LoggingMiddleware>()));
        builder.UseMiddleware(new ScopeMiddleware(_serviceProvider));
        builder.UseMiddleware(new RetryMiddleware(retryOptions, _dltPublisher, _loggerFactory.CreateLogger<RetryMiddleware>()));

        var execMiddleware = (IKafkaConsumerMiddleware)ActivatorUtilities.CreateInstance(
            _serviceProvider,
            typeof(ExecutionMiddleware<,>).MakeGenericType(_registration.ConsumerType, _registration.MessageType));
        builder.UseMiddleware(execMiddleware);

        var pipelineExecutor = builder.Build(_ => Task.CompletedTask);

        var workers = new PartitionWorker[concurrencyLimit];
        for (int i = 0; i < concurrencyLimit; i++)
        {
            workers[i] = new PartitionWorker(dispatcher.Channels[i].Reader, new Func<IKafkaContext, Task>(pipelineExecutor), _serviceProvider);
        }

        // 4. Build and subscribe consumer
        using var consumer = BuildConsumer(consumerOptions);
        consumer.Subscribe(_registration.Topic);

        _logger.LogInformation(
            "Subscribed to topic '{Topic}' using consumer group '{GroupId}' with concurrency {ConcurrencyLimit}.",
            _registration.Topic,
            _registration.GroupId,
            concurrencyLimit);

        bool isPaused = false;

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                bool shouldPause = false;
                bool shouldResume = true;

                foreach (var channel in dispatcher.Channels)
                {
                    var count = channel.Reader.Count;
                    if (count >= 80)
                    {
                        shouldPause = true;
                    }

                    if (count > 20)
                    {
                        shouldResume = false;
                    }
                }

                if (shouldPause && !isPaused)
                {
                    consumer.Pause(consumer.Assignment);
                    isPaused = true;
                    _logger.LogWarning("Watermark reached. Pausing message fetching from topic {Topic}.", _registration.Topic);
                }
                else if (shouldResume && isPaused)
                {
                    consumer.Resume(consumer.Assignment);
                    isPaused = false;
                    _logger.LogInformation("Watermark cleared. Resuming message fetching from topic {Topic}.", _registration.Topic);
                }

                try
                {
                    var result = consumer.Consume(TimeSpan.FromMilliseconds(100));
                    if (result != null)
                    {
                        // Check if deserialization is a Poison Pill
                        try
                        {
                            _deserializer.Deserialize(result.Message.Value, _registration.MessageType);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(
                                ex,
                                "Poison pill detected in topic '{Topic}' partition {Partition} offset {Offset}. Routing to DLT.",
                                result.Topic,
                                result.Partition.Value,
                                result.Offset.Value);

                            await _dltPublisher.PublishAsync(result, DltReason.PoisonPill, ex, 0, stoppingToken).ConfigureAwait(false);
                            consumer.Commit(result);
                            continue;
                        }

                        await dispatcher.DispatchAsync(result, stoppingToken).ConfigureAwait(false);
                    }
                }
                catch (ConsumeException ex)
                {
                    _logger.LogError(ex, "Kafka consumption error. Loop continuing.");
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Unexpected error in consumer loop. Loop continuing.");
                }
            }
        }
        finally
        {
            try
            {
                consumer.Close();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error closing Kafka consumer.");
            }

            for (int i = 0; i < concurrencyLimit; i++)
            {
                try
                {
                    await workers[i].StopAsync().ConfigureAwait(false);
                    workers[i].Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error stopping partition worker {Index}.", i);
                }
            }
        }
    }

    private async Task AutoProvisionTopicAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (string.IsNullOrEmpty(_options.BootstrapServers))
        {
            return;
        }

        try
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = _options.BootstrapServers,
                SaslUsername = _options.SaslUsername,
                SaslPassword = _options.SaslPassword,
            };

            if (_options.SaslMechanism != null)
            {
                config.SaslMechanism = Enum.Parse<SaslMechanism>(_options.SaslMechanism, true);
            }

            if (_options.SecurityProtocol != null)
            {
                config.SecurityProtocol = Enum.Parse<SecurityProtocol>(_options.SecurityProtocol, true);
            }

            using var adminClient = new AdminClientBuilder(config).Build();

            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
            var topicExists = metadata.Topics.Exists(t => string.Equals(t.Topic, _registration.Topic, StringComparison.Ordinal));

            if (!topicExists)
            {
                if (IsDevelopmentOrStaging())
                {
                    var envName = _environment?.EnvironmentName ?? "Development";
                    _logger.LogInformation("Topic '{Topic}' not found. Auto-provisioning in '{Env}' environment...", _registration.Topic, envName);

                    await adminClient.CreateTopicsAsync(new[]
                    {
                        new TopicSpecification
                        {
                            Name = _registration.Topic,
                            NumPartitions = 3,
                            ReplicationFactor = 1,
                        },
                    }).ConfigureAwait(false);

                    _logger.LogInformation("Topic '{Topic}' successfully auto-provisioned.", _registration.Topic);
                }
                else
                {
                    _logger.LogCritical("Topic '{Topic}' is missing in production environment! Please configure it externally.", _registration.Topic);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error while checking/provisioning topic '{Topic}'. Continuing consumer startup.", _registration.Topic);
        }
    }

    private bool IsDevelopmentOrStaging()
    {
        if (_environment != null)
        {
            return _environment.IsDevelopment() || _environment.IsStaging() ||
                   string.Equals(_environment.EnvironmentName, "Development", StringComparison.OrdinalIgnoreCase) ||
                   string.Equals(_environment.EnvironmentName, "Staging", StringComparison.OrdinalIgnoreCase);
        }

        var env = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT") ?? Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
        return string.Equals(env, "Development", StringComparison.OrdinalIgnoreCase) ||
               string.Equals(env, "Staging", StringComparison.OrdinalIgnoreCase);
    }

    private IConsumer<byte[], byte[]> BuildConsumer(ConsumerOptions consumerOptions)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = _registration.GroupId,
            AutoOffsetReset = string.IsNullOrEmpty(consumerOptions.AutoOffsetReset)
                ? AutoOffsetReset.Earliest
                : Enum.Parse<AutoOffsetReset>(consumerOptions.AutoOffsetReset, true),
            EnableAutoCommit = false,
        };

        if (_options.SaslUsername != null)
        {
            config.SaslUsername = _options.SaslUsername;
            config.SaslPassword = _options.SaslPassword;
        }

        if (_options.SaslMechanism != null)
        {
            config.SaslMechanism = Enum.Parse<SaslMechanism>(_options.SaslMechanism, true);
        }

        if (_options.SecurityProtocol != null)
        {
            config.SecurityProtocol = Enum.Parse<SecurityProtocol>(_options.SecurityProtocol, true);
        }

        var consumerBuilder = new ConsumerBuilder<byte[], byte[]>(config);

        consumerBuilder.SetPartitionsAssignedHandler((c, partitions) =>
        {
            _logger.LogInformation("Partitions assigned: {Partitions}", string.Join(", ", partitions));

            _ = Task.Run(async () =>
            {
                using (var scope = _serviceProvider.CreateScope())
                {
                    var listeners = scope.ServiceProvider.GetServices<IPartitionAssignmentListener>();
                    var dummyContext = new KafkaContext(
                        null,
                        -1,
                        -1,
                        string.Empty,
                        new Dictionary<string, byte[]>(StringComparer.Ordinal),
                        Array.Empty<byte>(),
                        scope.ServiceProvider,
                        CancellationToken.None);

                    foreach (var listener in listeners)
                    {
                        try
                        {
                            await listener.OnPartitionsAssignedAsync(dummyContext, partitions).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error in partition assignment listener.");
                        }
                    }
                }
            });
        });

        consumerBuilder.SetPartitionsRevokedHandler((c, partitions) =>
        {
            _logger.LogInformation("Partitions revoked: {Partitions}", string.Join(", ", partitions));

            _ = Task.Run(async () =>
            {
                using (var scope = _serviceProvider.CreateScope())
                {
                    var listeners = scope.ServiceProvider.GetServices<IPartitionAssignmentListener>();
                    var dummyContext = new KafkaContext(
                        null,
                        -1,
                        -1,
                        string.Empty,
                        new Dictionary<string, byte[]>(StringComparer.Ordinal),
                        Array.Empty<byte>(),
                        scope.ServiceProvider,
                        CancellationToken.None);

                    foreach (var listener in listeners)
                    {
                        try
                        {
                            await listener.OnPartitionsRevokedAsync(dummyContext, partitions).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Error in partition revocation listener.");
                        }
                    }
                }
            });
        });

        return consumerBuilder.Build();
    }
}
