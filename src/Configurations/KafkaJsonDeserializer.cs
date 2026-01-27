using System.Text.Json;
using Confluent.Kafka;

namespace Planara.Kafka.Configurations;

public class KafkaJsonDeserializer<TMessage>: IDeserializer<TMessage>
{
    public TMessage Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
    {
        if (isNull || data.IsEmpty)
            return default!;
        
        return JsonSerializer.Deserialize<TMessage>(data, KafkaJson.DeserializerOptions)!;
    }
}