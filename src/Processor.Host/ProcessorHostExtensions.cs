using KafkaFlow;
using KafkaFlow.Configuration;
using KafkaFlow.Consumers.DistributionStrategies;
using KafkaFlow.OpenTelemetry;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Processor.Core;
using Processor.Core.Application;
using Processor.Core.Diagnostics;
using Processor.Core.Domains;
using Processor.Domains.Posts;
using Processor.Domains.Profiles;

namespace Processor.Host;

/// <summary>
/// The host's composition, kept out of <c>Program.cs</c> so the wiring — domain selection, DI graph,
/// KafkaFlow registration — is exercised by <c>Processor.Host.Tests</c> rather than only at runtime.
/// </summary>
public static class ProcessorHostExtensions
{
    /// <summary>
    /// Every domain this image can run. All are compiled in; exactly one is activated per deployment
    /// according to <c>Processor:Domain</c>. Adding a domain means adding its module here.
    /// </summary>
    public static IReadOnlyList<IDomainModule> AvailableDomains() => new IDomainModule[]
    {
        new PostsDomainModule(),
        new ProfilesDomainModule()
    };

    /// <summary>
    /// Wires Core, the selected domain, telemetry and KafkaFlow. Returns the activated module so the
    /// caller (and tests) can assert which domain is running.
    /// </summary>
    public static IDomainModule AddProcessor(this WebApplicationBuilder builder)
    {
        var configuration = builder.Configuration;
        var services = builder.Services;

        var kafkaOptions = configuration.GetSection(KafkaOptions.SectionName).Get<KafkaOptions>()
            ?? new KafkaOptions();
        var metricsPort = configuration.GetValue<int?>("Metrics:Port") ?? 8080;

        if (kafkaOptions.Brokers.Length == 0)
        {
            throw new InvalidOperationException(
                $"'{KafkaOptions.SectionName}:{nameof(KafkaOptions.Brokers)}' must list at least one " +
                "broker. There is no default on purpose: silently pointing a deployment at localhost is " +
                "worse than refusing to start.");
        }

        // Domain selection happens before anything is registered: an unknown name must fail here,
        // loudly, rather than surface later as a missing DI registration.
        var domain = configuration[$"{ProcessorOptions.SectionName}:{nameof(ProcessorOptions.Domain)}"];
        var module = DomainModuleSelector.Select(AvailableDomains(), domain);

        // Expose the /metrics scraping endpoint (and nothing else) on a dedicated port.
        builder.WebHost.ConfigureKestrel(options => options.ListenAnyIP(metricsPort));

        services.AddProcessorCore(configuration);
        services.AddProcessorHealthChecks();

        // Registered as an instance because the KafkaFlow statistics handler below is a closure
        // configured before the container exists.
        var kafkaStats = new KafkaConsumerStatistics();
        services.AddSingleton(kafkaStats);

        // The only domain-specific registrations in the whole host.
        module.RegisterServices(services);

        services.AddProcessorTelemetry(configuration, module.Name);
        services.AddProcessorKafka(module, kafkaOptions, kafkaStats);

        return module;
    }

    /// <summary>
    /// Maps the metrics endpoint and the Kubernetes probes. Shared with the end-to-end tests so they
    /// exercise the same endpoints the deployed host exposes.
    /// </summary>
    public static WebApplication MapProcessorEndpoints(this WebApplication app, IDomainModule domain)
    {
        // GET /metrics -> Prometheus exposition format
        app.MapPrometheusScrapingEndpoint();

        // Kubernetes probes:
        //  - /health/live  : process is up (no dependency checks, so a store blip won't kill the pod)
        //  - /health/ready : dependencies OK (data store reachable + settings snapshot loaded)
        app.MapHealthChecks("/health/live", new HealthCheckOptions
        {
            Predicate = r => r.Tags.Contains("live")
        });
        app.MapHealthChecks("/health/ready", new HealthCheckOptions
        {
            Predicate = r => r.Tags.Contains("ready")
        });

        app.MapGet("/", () =>
            $"KafkaFlow Processor is running domain '{domain.Name}'. " +
            "See /metrics, /health/live, /health/ready");

        return app;
    }

    private static IServiceCollection AddProcessorTelemetry(
        this IServiceCollection services, IConfiguration configuration, string domainName)
    {
        var otlpEndpoint = configuration["OpenTelemetry:OtlpEndpoint"] ?? "http://localhost:8200";
        var serviceName = configuration["OpenTelemetry:ServiceName"] ?? "kafkaflow-processor";
        var serviceVersion = configuration["OpenTelemetry:ServiceVersion"] ?? "1.0.0";
        var environment = configuration["OpenTelemetry:Environment"] ?? "development";
        var site = configuration["OpenTelemetry:Site"] ?? "local";

        services.AddOpenTelemetry()
            .ConfigureResource(resource => resource
                .AddService(
                    serviceName: serviceName,
                    serviceVersion: serviceVersion)
                .AddAttributes(new Dictionary<string, object>
                {
                    ["deployment.environment"] = environment,
                    ["service.site"] = site,
                    // Resource-level domain tag: distinguishes the per-domain deployments of this
                    // same service in traces as well as metrics.
                    ["processor.domain"] = domainName,
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

        return services;
    }

    private static IServiceCollection AddProcessorKafka(
        this IServiceCollection services,
        IDomainModule module,
        KafkaOptions kafka,
        KafkaConsumerStatistics kafkaStats)
    {
        services.AddKafkaFlowHostedService(builder => builder
            .UseLogHandler<MicrosoftLogHandler>()
            .AddOpenTelemetryInstrumentation()
            .AddCluster(cluster =>
            {
                cluster
                    .WithBrokers(kafka.Brokers)
                    .AddConsumer(consumer => consumer
                        .Topic(kafka.InputTopic)
                        .WithGroupId(kafka.ConsumerGroupId)
                        .WithBufferSize(kafka.BufferSize)
                        .WithWorkersCount(kafka.WorkersCount)
                        .WithAutoOffsetReset(kafka.ResolveAutoOffsetReset())
                        // Emit librdkafka statistics (lag, rebalances, RTT, ...) -> OTel metrics.
                        .WithStatisticsIntervalMs(kafka.StatisticsIntervalMs)
                        .WithStatisticsHandler(json => kafkaStats.Handle(json))
                        // Keyless workload: FreeWorker distributes each message to any free worker
                        // (the default BytesSum would pin all null-key messages to worker 0).
                        .WithWorkerDistributionStrategy<FreeWorkerDistributionStrategy>()
                        // The deserializer and typed handler are the selected domain's closed types.
                        .AddMiddlewares(middlewares => module.ConfigureConsumer(middlewares)));

                module.ConfigureProducers(cluster, kafka);
            }));

        return services;
    }
}
