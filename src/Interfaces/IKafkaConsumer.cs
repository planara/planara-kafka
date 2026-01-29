using Confluent.Kafka;

namespace Planara.Kafka.Interfaces;

/// <summary>
/// Kafka-consumer, обрабатывающий входящие сообщения.
/// </summary>
public interface IKafkaConsumer<TMessage> where TMessage : class
{
    /// <summary>
    /// Получение сообщения из Kafka по ключу топика из настроек.
    /// </summary>
    /// <param name="topicKey">Ключ топика из настроек.</param>
    /// <param name="cancellationToken">Токен отмены операции.</param>
    /// <returns>
    /// <see cref="ConsumeResult{TKey,TValue}"/> при успешном чтении сообщения или <c>null</c>,
    /// если данных нет / достигнут EOF / получено сообщение из другого топика (при подписке на несколько).
    /// </returns>
    Task<ConsumeResult<string, TMessage>?> ConsumeAsync(string topicKey, CancellationToken cancellationToken);

    /// <summary>
    /// Коммитит offset прочитанного сообщения.
    /// Вызывать только после успешной обработки сообщения (например, после записи в БД).
    /// </summary>
    /// <remarks>
    /// Используется для семантики доставки <b>at-least-once</b>:
    /// если обработка упала и commit не был выполнен, сообщение будет доставлено повторно.
    /// Поэтому обработчик должен быть идемпотентным (например, через уникальные ключи/проверки в БД).
    ///
    /// Если в конфигурации consumer включён <c>EnableAutoCommit=true</c>, реализация может игнорировать вызов
    /// (commit выполняется автоматически).
    /// </remarks>
    /// <param name="result">Результат consume, содержащий topic/partition/offset для коммита.</param>
    /// <param name="cancellationToken">Токен отмены операции.</param>
    Task CommitAsync(ConsumeResult<string, TMessage> result, CancellationToken cancellationToken);
    
    /// <summary>
    /// Закрывает Kafka-consumer и освобождает ресурсы.
    /// </summary>
    void Close();
}