namespace Planara.Kafka.Interfaces;

public interface IKafkaTopicsInitializer
{
    /// <summary>
    /// Инициализация топиков из настроек в конфигурации
    /// </summary>
    Task EnsureTopicsCreatedAsync();
}