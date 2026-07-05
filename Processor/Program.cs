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
using Processor.Settings;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

var configuration = builder.Configuration;

var brokers = configuration.GetSection("Kafka:Brokers").Get<string[]>() ?? new[] { "localhost:9092" };
var otlpEndpoint = configuration["OpenTelemetry:OtlpEndpoint"] ?? "http://localhost:8200";
var serviceName = configuration["OpenTelemetry:ServiceName"] ?? "kafkaflow-processor";
var serviceVersion = configuration["OpenTelemetry:ServiceVersion"] ?? "1.0.0";
var environment = configuration["OpenTelemetry:Environment"] ?? "development";
var site = configuration["OpenTelemetry:Site"] ?? "local";
var metricsPort = configuration.GetValue<int?>("Metrics:Port") ?? 8080;
var redisConnectionString = configuration["Redis:ConnectionString"] ?? "localhost:6379";

// Expose the /metrics scraping endpoint (and nothing else) on a dedicated port.
builder.WebHost.ConfigureKestrel(options => options.ListenAnyIP(metricsPort));

var services = builder.Services;

// Data-type settings: Redis-backed repository with an in-memory TTL cache.
services.Configure<DataTypeSettingsOptions>(configuration.GetSection(DataTypeSettingsOptions.SectionName));
services.AddSingleton<IConnectionMultiplexer>(_ =>
{
    var config = ConfigurationOptions.Parse(redisConnectionString);
    config.AbortOnConnectFail = false; // don't crash startup if Redis is briefly unavailable
    return ConnectionMultiplexer.Connect(config);
});
services.AddSingleton<IDataTypeSettingsRepository, RedisDataTypeSettingsRepository>();

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
        // Scraped by Prometheus at GET /metrics
        .AddPrometheusExporter()
        // Also push to the OTLP collector (Elastic APM) when available
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

var app = builder.Build();

// GET /metrics -> Prometheus exposition format
app.MapPrometheusScrapingEndpoint();

// Simple liveness probe
app.MapGet("/", () => "KafkaFlow Processor is running. Metrics available at /metrics");

await app.RunAsync();
