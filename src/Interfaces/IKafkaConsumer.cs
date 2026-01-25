namespace Planara.Kafka.Interfaces;

/// <summary>
/// Kafka-consumer, обрабатывающий входящие сообщения.
/// </summary>
public interface IKafkaConsumer<TMessage> where TMessage : class
{
    /// <summary>
    /// Получение сообщений из Kafka.
    /// </summary>
    /// <param name="topicKey">Ключ топика из настроек</param>
    /// <param name="cancellationToken">Токен отмены операции.</param>
    Task<TMessage?> ConsumeAsync(string topicKey, CancellationToken cancellationToken);

    /// <summary>
    /// Закрывает Kafka-consumer и освобождает ресурсы.
    /// </summary>
    void Close();
}