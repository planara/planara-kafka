using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Planara.Kafka.Interfaces;
using Planara.Kafka.Options;
using Planara.Kafka.Services;

namespace Planara.Kafka.Extensions;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Регистрация Kafka-producer с поддержкой сериализации сообщений.
    /// </summary>
    public static IServiceCollection AddKafkaProducer<TMessage>(
        this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<KafkaOptions>(configuration.GetSection("Kafka"));
        services.AddSingleton<IKafkaProducer<TMessage>, KafkaProducer<TMessage>>();

        return services;
    }
    
    /// <summary>
    /// Регистрирует Kafka-consumer с поддержкой десериализации сообщений.
    /// </summary>
    public static IServiceCollection AddKafkaConsumer<TMessage>(
        this IServiceCollection services, IConfiguration configuration)
        where TMessage : class
    {
        services.Configure<KafkaOptions>(configuration.GetSection("Kafka"));
        services.AddSingleton<IKafkaConsumer<TMessage>, KafkaConsumer<TMessage>>();
        
        return services;
    }

    /// <summary>
    /// Регистрация инциализатора топиков
    /// </summary>
    public static IServiceCollection AddKafkaTopicsInitializer(this IServiceCollection services, IConfiguration configuration)
    {
        services.Configure<KafkaOptions>(configuration.GetSection("Kafka"));
        services.AddTransient<IKafkaTopicsInitializer, KafkaTopicInitializer>();
        
        return services;
    }
}