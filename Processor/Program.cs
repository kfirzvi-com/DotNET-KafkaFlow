using KafkaFlow;
using KafkaFlow.Serializer;
using Processor.Handlers;
using Processor.Messages;

await Host
    .CreateDefaultBuilder(args)
    .ConfigureServices((hostContext, services) =>
    {
        var brokers = hostContext.Configuration.GetSection("Kafka:Brokers").Get<string[]>() ?? new[] { "localhost:9092" };

        services.AddKafka(kafka => kafka
            .AddCluster(cluster => cluster
                .WithBrokers(brokers)
                .AddConsumer(consumer => consumer
                    .Topic("input-topic")
                    .WithGroupId("message-processor-group")
                    .WithBufferSize(100)
                    .WithWorkersCount(10)
                    .AddMiddlewares(middlewares => middlewares
                        .AddDeserializer<JsonCoreDeserializer>()
                        .AddTypedHandlers(handlers => handlers
                            .AddHandler<MessageHandler>()
                        )
                    )
                )
                .AddProducer<OutputMessage>(producer => producer
                    .DefaultTopic("output-topic")
                    .AddMiddlewares(middlewares => middlewares
                        .AddSerializer<JsonCoreSerializer>()
                    )
                )
            )
        );
    })
    .Build()
    .RunAsync();
