using Confluent.Kafka;

namespace kafka.producer
{
    public class ProducerService
    {
        private readonly ILogger<ProducerService> _logger;

        public ProducerService(ILogger<ProducerService> logger)
        {
            _logger = logger;
        }
        public async Task ProducerAsync(CancellationToken cancellation)
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                AllowAutoCreateTopics = true,
                Acks = Acks.All
            };

            using var producer = new ProducerBuilder<Null, string>(config).Build();
            try
            {
                var deliveryResult = await producer.ProduceAsync(topic: "test-topic",
                    new Message<Null, string>
                    {
                        Value = $"Hello, Kafka! {DateTime.UtcNow}"
                    }, cancellation);
                _logger.LogInformation($"Delivered message to {deliveryResult.Value}, Offset: {deliveryResult.Offset}");
            }
            catch (ProduceException<Null, string> ex)
            {
                _logger.LogError($"Delivered failed: {ex.Error.Reason}");
            }

            producer.Flush(cancellation);
        }
    }
}
