﻿
using Confluent.Kafka;

namespace kafka.consumer
{
    public class EventConsumerJob : BackgroundService
    {
        private readonly ILogger<EventConsumerJob> _logger;

        public EventConsumerJob(ILogger<EventConsumerJob> logger)
        {
            _logger = logger;
        }
        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "test-group",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };

            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var consumeResult = consumer.Consume(TimeSpan.FromSeconds(5));

                    if (consumeResult == null)
                    {
                        continue;
                    }

                    _logger.LogInformation($"Consumed message '{consumeResult.Message.Value}' at: '{consumeResult.Offset}'");
                }
                catch (Exception)
                {
                    // Ignore
                }
            }
            return Task.CompletedTask;
        }
    }
}
