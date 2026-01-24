using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Options;
using Planara.Kafka.Interfaces;
using Planara.Kafka.Options;

namespace Planara.Kafka.Services;

public class KafkaTopicInitializer(IOptions<KafkaOptions> options): IKafkaTopicsInitializer
{
    private readonly KafkaOptions _kafkaOptions = options.Value;

    public async Task EnsureTopicsCreatedAsync()
    {
        var allTopics = _kafkaOptions.ProducerTopics.Values
            .Distinct()
            .ToList();

        if (!allTopics.Any())
            return;

        using var adminClient = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = _kafkaOptions.BootstrapServers
        }).Build();

        var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
        var existingTopics = metadata.Topics.Select(t => t.Topic).ToHashSet();

        var topicsToCreate = allTopics
            .Where(t => !existingTopics.Contains(t))
            .Select(topic => new TopicSpecification
            {
                Name = topic,
                NumPartitions = 1,
                ReplicationFactor = 1
            }).ToList();

        if (topicsToCreate.Any())
        {
            try
            {
                await adminClient.CreateTopicsAsync(topicsToCreate);
            }
            catch (CreateTopicsException ex)
            {
                foreach (var result in ex.Results)
                {
                    if (result.Error.Code != ErrorCode.TopicAlreadyExists)
                        throw;
                }
            }
        }
    }
}