using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using QKafka.Tests.Support;

namespace QKafka.Tests;

public sealed class ServiceCollectionExtensionsUnitTests
{
    private static readonly System.Reflection.Assembly TestAssembly = typeof(OrderConsumer).Assembly;

    [Fact]
    public void AddqKafka_Configured_RegistersTheOptionsWithTheConfiguredValues()
    {
        var services = new ServiceCollection();

        services.AddqKafka(o => o.BootstrapServers = "broker:9092");

        var options = services.BuildServiceProvider().GetRequiredService<QKafkaOptions>();
        Assert.Equal("broker:9092", options.BootstrapServers);
    }

    [Fact]
    public void AddqKafka_Called_RegistersTheDefaultDeserializerAndDltPublisher()
    {
        var services = new ServiceCollection();

        services.AddqKafka(_ => { });

        Assert.Contains(services, d => d.ServiceType == typeof(IKafkaMessageDeserializer) && d.ImplementationType == typeof(JsonKafkaMessageDeserializer));
        Assert.Contains(services, d => d.ServiceType == typeof(IDltPublisher));
    }

    [Fact]
    public void AddqKafka_Called_ResolvesThePublisherAndSharesOneProducerWithTheDltPublisher()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddqKafka(o => o.BootstrapServers = "localhost:1");
        using var provider = services.BuildServiceProvider();

        Assert.IsType<KafkaPublisher>(provider.GetRequiredService<IKafkaPublisher>());
        Assert.IsType<JsonKafkaMessageSerializer>(provider.GetRequiredService<IKafkaMessageSerializer>());
        Assert.NotNull(provider.GetRequiredService<IDltPublisher>());
        Assert.Same(
            provider.GetRequiredService<Confluent.Kafka.IProducer<byte[], byte[]>>(),
            provider.GetRequiredService<Confluent.Kafka.IProducer<byte[], byte[]>>());
    }

    [Fact]
    public void BlankBootstrapServers_PublisherResolved_FailsFastInsteadOfDroppingMessages()
    {
        var services = new ServiceCollection();
        services.AddqKafka(_ => { });
        using var provider = services.BuildServiceProvider();

        Assert.Throws<InvalidOperationException>(() => provider.GetRequiredService<IKafkaPublisher>());
    }

    [Fact]
    public void CustomSerializerRegisteredFirst_AddqKafka_DoesNotOverrideIt()
    {
        var services = new ServiceCollection();
        var custom = new JsonKafkaMessageSerializer();
        services.AddSingleton<IKafkaMessageSerializer>(custom);

        services.AddqKafka(_ => { });

        Assert.Same(custom, services.BuildServiceProvider().GetRequiredService<IKafkaMessageSerializer>());
    }

    [Fact]
    public void CustomDeserializerRegisteredFirst_AddqKafka_DoesNotOverrideIt()
    {
        var services = new ServiceCollection();
        var custom = new JsonKafkaMessageDeserializer();
        services.AddSingleton<IKafkaMessageDeserializer>(custom);

        services.AddqKafka(_ => { });

        Assert.Same(custom, services.BuildServiceProvider().GetRequiredService<IKafkaMessageDeserializer>());
    }

    [Fact]
    public void NullArguments_AddqKafka_ThrowArgumentNull()
    {
        Assert.Throws<ArgumentNullException>(() => ServiceCollectionExtensions.AddqKafka(null!, _ => { }));
        Assert.Throws<ArgumentNullException>(() => new ServiceCollection().AddqKafka(null!));
    }

    [Fact]
    public void AssemblyWithMarkedConsumers_AddqKafkaConsumers_RegistersEachOneAndAHostedLoopPerConsumer()
    {
        var services = new ServiceCollection();

        services.AddqKafkaConsumers(TestAssembly);

        Assert.Contains(services, d => d.ServiceType == typeof(OrderConsumer) && d.Lifetime == ServiceLifetime.Transient);
        Assert.Contains(services, d => d.ServiceType == typeof(AuditConsumer) && d.Lifetime == ServiceLifetime.Transient);
        Assert.Equal(2, services.Count(d => d.ServiceType == typeof(IHostedService)));
    }

    [Fact]
    public void UnmarkedAndAbstractConsumers_AddqKafkaConsumers_AreIgnored()
    {
        var services = new ServiceCollection();

        services.AddqKafkaConsumers(TestAssembly);

        Assert.DoesNotContain(services, d => d.ServiceType == typeof(UnmarkedConsumer));
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(AbstractMarkedConsumer));
    }

    [Fact]
    public void NullServices_AddqKafkaConsumers_ThrowsArgumentNull()
    {
        Assert.Throws<ArgumentNullException>(() => ServiceCollectionExtensions.AddqKafkaConsumers(null!, TestAssembly));
    }

    [Fact]
    public void NoAssemblies_AddqKafkaConsumers_ThrowsArgumentNull()
    {
        Assert.Throws<ArgumentNullException>(() => new ServiceCollection().AddqKafkaConsumers());
        Assert.Throws<ArgumentNullException>(() => new ServiceCollection().AddqKafkaConsumers(null!));
    }
}
