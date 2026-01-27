using System.Text.Json;
using Confluent.Kafka;

namespace Planara.Kafka.Configurations;

/// <summary>
/// JSON-сериализатор сообщений Kafka
/// </summary>
public class KafkaJsonSerializer<TMessage> : ISerializer<TMessage>
{
    public byte[] Serialize(TMessage data, SerializationContext context)
    {
        if (data is null)
            return [];

        return JsonSerializer.SerializeToUtf8Bytes(data, KafkaJson.SerializerOptions);
    }
}