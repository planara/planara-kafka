namespace Planara.Kafka.Options;

/// <summary>
/// Настройки для Kafka
/// </summary>
public class KafkaOptions
{
    /// <summary>
    /// Адреса Kafka-брокеров
    /// </summary>
    public required string BootstrapServers { get; set; }

    /// <summary>
    /// Топики для продюсеров
    /// </summary>
    public required Dictionary<string, string> ProducerTopics { get; set; } = new();

    /// <summary>
    /// Топики для консюмеров
    /// </summary>
    public required Dictionary<string, string> ConsumerTopics { get; set; } = new();
    
    /// <summary>
    /// Консьюмер-группа 
    /// </summary>
    public string? ConsumerGroupId { get; set; }
    
    public bool EnableAutoCommit { get; set; } = false;
    public bool EnableAutoOffsetStore { get; set; } = false;
}