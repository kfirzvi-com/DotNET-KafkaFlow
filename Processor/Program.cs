using KafkaFlow;
using KafkaFlow.Configuration;
using KafkaFlow.Consumers.DistributionStrategies;
using KafkaFlow.OpenTelemetry;
using KafkaFlow.Serializer;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Processor.Domain.Building;
using Processor.Domain.Building.FieldBuilders;
using Processor.Diagnostics;
using Processor.Infrastructure.Diagnostics;
using Processor.Application;
using Processor.Infrastructure.Health;
using Processor.Domain.Messages;
using Processor.Domain.DataTypes;
using Processor.Infrastructure.DataTypes;
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
var inputTopic = configuration["Kafka:InputTopic"] ?? "input-topic";
var workersCount = configuration.GetValue<int?>("Kafka:WorkersCount") ?? 10;
var bufferSize = configuration.GetValue<int?>("Kafka:BufferSize") ?? 100;
var statisticsIntervalMs = configuration.GetValue<int?>("Kafka:StatisticsIntervalMs") ?? 5000;

// Bridges librdkafka statistics -> OTel metrics; updated by the consumer's statistics handler.
var kafkaStats = new KafkaConsumerStatistics();

// Expose the /metrics scraping endpoint (and nothing else) on a dedicated port.
builder.WebHost.ConfigureKestrel(options => options.ListenAnyIP(metricsPort));

var services = builder.Services;

// Benchmark knob: synthetic per-message CPU work for worker/buffer tuning.
services.Configure<BenchmarkOptions>(configuration.GetSection(BenchmarkOptions.SectionName));

// Data-type settings: Redis-backed repository with an in-memory TTL cache.
services.Configure<DataTypeSettingsOptions>(configuration.GetSection(DataTypeSettingsOptions.SectionName));
services.AddSingleton<IConnectionMultiplexer>(_ =>
{
    var config = ConfigurationOptions.Parse(redisConnectionString);
    // Tolerate transient runtime blips; startup failure is enforced explicitly by the refresh
    // service's initial load (so the app crashes and k8s restarts it when Redis is down at start).
    config.AbortOnConnectFail = false;
    return ConnectionMultiplexer.Connect(config);
});
services.AddSingleton<RedisDataTypeSettingsRepository>();
// Domain repository (FindBy/FindAll) and infra cache lifecycle (Init/Refresh) are separate
// interfaces over the same singleton, so domain consumers never see the cache methods.
services.AddSingleton<IDataTypeSettingsRepository>(sp => sp.GetRequiredService<RedisDataTypeSettingsRepository>());
services.AddSingleton<IDataTypeSettingsCache>(sp => sp.GetRequiredService<RedisDataTypeSettingsRepository>());

// Kafka statistics -> metrics bridge (updated by the consumer statistics handler below).
services.AddSingleton(kafkaStats);

// Owns the settings snapshot: initial load (crash-on-failure) + periodic refresh (stale-tolerant).
// Registered before the KafkaFlow hosted service so it starts (and can fail startup) first.
services.AddHostedService<DataTypeSettingsRefreshService>();

// Health checks: liveness (no deps) and readiness (Redis + settings snapshot).
services.AddHealthChecks()
    .AddCheck("self", () => HealthCheckResult.Healthy("alive"), tags: new[] { "live" })
    .AddCheck<RedisHealthCheck>("redis", tags: new[] { "ready" })
    .AddCheck<SettingsHealthCheck>("settings", tags: new[] { "ready" });

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
        .AddMeter(KafkaConsumerStatistics.MeterName)
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
    .UseLogHandler<Processor.Infrastructure.Diagnostics.MicrosoftLogHandler>()
    .AddOpenTelemetryInstrumentation()
    .AddCluster(cluster => cluster
        .WithBrokers(brokers)
        .AddConsumer(consumer => consumer
            .Topic(inputTopic)
            .WithGroupId("message-processor-group")
            .WithBufferSize(bufferSize)
            .WithWorkersCount(workersCount)
            // Emit librdkafka statistics (lag, rebalances, RTT, ...) -> parsed into OTel metrics.
            .WithStatisticsIntervalMs(statisticsIntervalMs)
            .WithStatisticsHandler(json => kafkaStats.Handle(json))
            // Keyless workload: FreeWorker distributes each message to any free worker
            // (the default BytesSum would pin all null-key messages to worker 0).
            .WithWorkerDistributionStrategy<FreeWorkerDistributionStrategy>()
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

// Kubernetes probes:
//  - /health/live  : process is up (no dependency checks, so a Redis blip won't get the pod killed)
//  - /health/ready : dependencies OK (Redis reachable + settings snapshot loaded)
app.MapHealthChecks("/health/live", new HealthCheckOptions { Predicate = r => r.Tags.Contains("live") });
app.MapHealthChecks("/health/ready", new HealthCheckOptions { Predicate = r => r.Tags.Contains("ready") });

app.MapGet("/", () => "KafkaFlow Processor is running. See /metrics, /health/live, /health/ready");

await app.RunAsync();
