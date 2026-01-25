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

    public KafkaConsumer(IOptions<KafkaOptions> options, ILogger<KafkaConsumer<TMessage>> logger)
    {
        _consumerTopics = options.Value.ConsumerTopics;
        _logger = logger;

        var config = new ConsumerConfig
        {
            BootstrapServers = options.Value.BootstrapServers,
            GroupId = $"group-{typeof(TMessage).Name.ToLower()}",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = true
        };

        _consumer = new ConsumerBuilder<string, TMessage>(config)
            .SetValueDeserializer(new KafkaJsonDeserializer<TMessage>())
            .SetErrorHandler((_, e) =>
            {
                _logger.LogError("Kafka consume error: { 0 }", e.Reason);
            })
            .Build();

        _consumer.Subscribe(_consumerTopics.Values);
    }

    public Task<TMessage?> ConsumeAsync(string topicKey, CancellationToken cancellationToken)
    {
        if (!_consumerTopics.TryGetValue(topicKey, out var topicName))
            throw new ArgumentException($"Topic key '{topicKey}' not found in ConsumerTopics.", nameof(topicKey));

        return Task.Run(() =>
        {
            try
            {
                _consumer.Subscribe(topicName);
                var result = _consumer.Consume(cancellationToken);
                return result?.Message?.Value;
            }
            catch (ConsumeException ex)
            {
                throw new KafkaConsumeException($"Consume error from topic '{topicName}': {ex.Error.Reason}", ex);
            }
        }, cancellationToken);
    }

    public void Close() => _consumer.Close();
}