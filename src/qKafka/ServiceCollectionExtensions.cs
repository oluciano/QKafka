using System;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace QKafka;

/// <summary>
/// Dependency Injection extensions for registering qKafka services and consumers.
/// </summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the core qKafka services and global options.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="configure">The configuration delegate.</param>
    /// <returns>The same service collection, to allow call chaining.</returns>
    public static IServiceCollection AddqKafka(this IServiceCollection services, Action<QKafkaOptions> configure)
    {
        if (services == null)
        {
            throw new ArgumentNullException(nameof(services));
        }

        if (configure == null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        var options = new QKafkaOptions();
        configure(options);
        services.AddSingleton(options);

        services.TryAddSingleton<IKafkaMessageDeserializer, JsonKafkaMessageDeserializer>();
        services.TryAddSingleton<IDltPublisher, DltPublisher>();

        return services;
    }

    /// <summary>
    /// Scans the specified assemblies for classes implementing <see cref="IKafkaConsumer{TMessage}"/> with a <see cref="KafkaConsumerAttribute"/> and registers them as background consumer loops.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="assemblies">The assemblies to scan.</param>
    /// <returns>The same service collection, to allow call chaining.</returns>
    public static IServiceCollection AddqKafkaConsumers(this IServiceCollection services, params Assembly[] assemblies)
    {
        if (services == null)
        {
            throw new ArgumentNullException(nameof(services));
        }

        if (assemblies == null || assemblies.Length == 0)
        {
            throw new ArgumentNullException(nameof(assemblies));
        }

        var registrations = new List<ConsumerRegistration>();

        foreach (var assembly in assemblies)
        {
            foreach (var type in assembly.GetTypes())
            {
                if (type.IsClass && !type.IsAbstract)
                {
                    var consumerInterface = FindKafkaConsumerInterface(type);
                    if (consumerInterface != null)
                    {
                        var attribute = type.GetCustomAttribute<KafkaConsumerAttribute>();
                        if (attribute != null)
                        {
                            var messageType = consumerInterface.GetGenericArguments()[0];
                            var groupId = attribute.GroupId ?? $"{type.Name}-group";
                            var reg = new ConsumerRegistration(type, messageType, attribute.Topic, groupId);
                            registrations.Add(reg);
                        }
                    }
                }
            }
        }

        foreach (var reg in registrations)
        {
            services.AddTransient(reg.ConsumerType);

            services.AddSingleton<IHostedService>(sp =>
            {
                var options = sp.GetRequiredService<QKafkaOptions>();
                var dltPublisher = sp.GetRequiredService<IDltPublisher>();
                var deserializer = sp.GetRequiredService<IKafkaMessageDeserializer>();
                var loggerFactory = sp.GetRequiredService<ILoggerFactory>();
                var env = sp.GetService<IHostEnvironment>();

                return new KafkaConsumerLoop(reg, options, sp, dltPublisher, deserializer, loggerFactory, env);
            });
        }

        return services;
    }

    private static Type? FindKafkaConsumerInterface(Type type)
    {
        foreach (var iface in type.GetInterfaces())
        {
            if (iface.IsGenericType && iface.GetGenericTypeDefinition() == typeof(IKafkaConsumer<>))
            {
                return iface;
            }
        }

        return null;
    }
}
