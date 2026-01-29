using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Planara.Kafka.Configurations;
using Planara.Kafka.Exceptions;
using Planara.Kafka.Interfaces;
using Planara.Kafka.Options;

namespace Planara.Kafka.Services;

/// <summary>
/// Kafka-consumer для обработки сообщений определённого типа.
/// </summary>
/// <typeparam name="TMessage">Тип ожидаемого сообщения.</typeparam>
public class KafkaConsumer<TMessage> : IKafkaConsumer<TMessage> where TMessage : class
{
    private readonly IConsumer<string, TMessage> _consumer;
    private readonly Dictionary<string, string> _consumerTopics;
    private readonly ILogger<KafkaConsumer<TMessage>> _logger;
    private readonly bool _autoCommit;
    private readonly bool _autoOffsetStore;

    public KafkaConsumer(IOptions<KafkaOptions> options, ILogger<KafkaConsumer<TMessage>> logger)
    {
        var opt = options.Value;
        
        _consumerTopics = options.Value.ConsumerTopics;
        _logger = logger;
        _autoCommit = opt.EnableAutoCommit;
        _autoOffsetStore = opt.EnableAutoOffsetStore;
        
        var groupId = !string.IsNullOrWhiteSpace(opt.ConsumerGroupId)
            ? opt.ConsumerGroupId
            : $"group-{typeof(TMessage).Name.ToLower()}";

        var config = new ConsumerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = opt.EnableAutoCommit,
            EnableAutoOffsetStore = opt.EnableAutoOffsetStore
        };

        _consumer = new ConsumerBuilder<string, TMessage>(config)
            .SetValueDeserializer(new KafkaJsonDeserializer<TMessage>())
            .SetErrorHandler((_, e) => _logger.LogError("Kafka consume error: { 0 }", e.Reason))
            .Build();

        _consumer.Subscribe(_consumerTopics.Values.Distinct());
    }

    public Task<ConsumeResult<string, TMessage>?> ConsumeAsync(string topicKey, CancellationToken cancellationToken)
    {
        if (!_consumerTopics.TryGetValue(topicKey, out var topicName))
            throw new ArgumentException($"Topic key '{topicKey}' not found in ConsumerTopics.", nameof(topicKey));

        return Task.Run(() =>
        {
            try
            {
                var result = _consumer.Consume(cancellationToken);
                if (result is null || result.IsPartitionEOF) return null;

                if (result.Topic != topicName) return null;

                if (_autoCommit && !_autoOffsetStore)
                    _consumer.StoreOffset(result);

                return result;
            }
            catch (ConsumeException ex)
            {
                throw new KafkaConsumeException($"Consume error from topic '{topicName}': {ex.Error.Reason}", ex);
            }
        }, cancellationToken);
    }
    
    public Task CommitAsync(ConsumeResult<string, TMessage> result, CancellationToken cancellationToken)
    {
        // если авто-коммит включён — ручной commit не нужен
        if (_autoCommit) return Task.CompletedTask;

        return Task.Run(() =>
        {
            try
            {
                if (_autoOffsetStore)
                    _consumer.Commit(result);
                else
                {
                    _consumer.StoreOffset(result);
                    _consumer.Commit(result);
                }
            }
            catch (KafkaException ex)
            {
                _logger.LogWarning(ex, "Kafka commit failed for {TPO}", result.TopicPartitionOffset);
                throw;
            }
        }, cancellationToken);
    }

    public void Close() => _consumer.Close();
}