namespace Planara.Kafka.Exceptions;

public class KafkaConsumeException : Exception
{
    public KafkaConsumeException(string message, Exception? innerException = null)
        : base(message, innerException) { }
    
    public KafkaConsumeException(string message, params object[] args)
        : base(string.Format(message, args))
    {
    }
}