using KafkaFlow;
using KafkaFlow.Configuration;
using KafkaFlow.OpenTelemetry;
using KafkaFlow.Serializer;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Processor.Builders.Core;
using Processor.Builders.FieldBuilders;
using Processor.Diagnostics;
using Processor.Handlers;
using Processor.Messages;

await Host
    .CreateDefaultBuilder(args)
    .ConfigureServices((hostContext, services) =>
    {
        var brokers = hostContext.Configuration.GetSection("Kafka:Brokers").Get<string[]>() ?? new[] { "localhost:9092" };
        var otlpEndpoint = hostContext.Configuration["OpenTelemetry:OtlpEndpoint"] ?? "http://localhost:8200";
        var serviceName = hostContext.Configuration["OpenTelemetry:ServiceName"] ?? "kafkaflow-processor";
        var serviceVersion = hostContext.Configuration["OpenTelemetry:ServiceVersion"] ?? "1.0.0";
        var environment = hostContext.Configuration["OpenTelemetry:Environment"] ?? "development";
        var site = hostContext.Configuration["OpenTelemetry:Site"] ?? "local";

        services.AddSingleton<OutputIdBuilder>();
        services.AddSingleton<ProcessedContentBuilder>();
        services.AddSingleton<ProcessedAtBuilder>();
        services.AddSingleton<ProcessorNameBuilder>();
        services.AddSingleton<IOutputMessageBuilder, OutputMessageBuilder>();

        services.AddOpenTelemetry()
            .ConfigureResource(resource => resource
                .AddService(
                    serviceName: serviceName,
                    serviceVersion: serviceVersion)
                .AddAttributes(new Dictionary<string, object>
                {
                    ["deployment.environment"] = environment,
                    ["service.site"] = site,
                }))
            .WithTracing(tracing => tracing
                .AddSource(KafkaFlowInstrumentation.ActivitySourceName)
                .AddOtlpExporter(options =>
                {
                    options.Endpoint = new Uri(otlpEndpoint);
                    options.Protocol = OpenTelemetry.Exporter.OtlpExportProtocol.Grpc;
                }))
            .WithMetrics(metrics => metrics
                .AddMeter(ProcessorMetrics.MeterName)
                .AddMeter(KafkaFlowInstrumentation.ActivitySourceName)
                .AddRuntimeInstrumentation()
                .AddOtlpExporter(options =>
                {
                    options.Endpoint = new Uri(otlpEndpoint);
                    options.Protocol = OpenTelemetry.Exporter.OtlpExportProtocol.Grpc;
                }));

        services.AddKafkaFlowHostedService(kafka => kafka
            .UseLogHandler<Processor.Diagnostics.MicrosoftLogHandler>()
            .AddOpenTelemetryInstrumentation()
            .AddCluster(cluster => cluster
                .WithBrokers(brokers)
                .AddConsumer(consumer => consumer
                    .Topic("input-topic")
                    .WithGroupId("message-processor-group")
                    .WithBufferSize(100)
                    .WithWorkersCount(10)
                    .AddMiddlewares(middlewares => middlewares
                        .AddSingleTypeDeserializer<InputMessage, JsonCoreDeserializer>()
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
                .AddProducer<DeadLetterMessage>(producer => producer
                    .DefaultTopic("dead-letter-topic")
                    .AddMiddlewares(middlewares => middlewares
                        .AddSerializer<JsonCoreSerializer>()
                    )
                )
            )
        );
    })
    .Build()
    .RunAsync();
