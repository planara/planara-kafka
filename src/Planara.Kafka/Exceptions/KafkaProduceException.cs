namespace Planara.Kafka.Exceptions;

public class KafkaProduceException: Exception
{
    public KafkaProduceException(string message, Exception? innerException = null)
        : base(message, innerException) { }
    
    public KafkaProduceException(string message, params object[] args)
        : base(string.Format(message, args))
    {
    }
}