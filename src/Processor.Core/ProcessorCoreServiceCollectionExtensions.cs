using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Processor.Core.Application;
using Processor.Core.Building.FieldBuilders;
using Processor.Core.DataTypes;
using Processor.Core.Health;

namespace Processor.Core;

/// <summary>
/// Registers everything that is identical across domains. The host calls this once, then activates a
/// single <see cref="Domains.IDomainModule"/> on top — that split is what keeps the domain-specific
/// surface down to one builder per domain.
/// </summary>
public static class ProcessorCoreServiceCollectionExtensions
{
    /// <summary>In-memory store name accepted by <c>DataTypeSettings:Store</c>.</summary>
    public const string InMemoryStoreName = "inmemory";

    /// <summary>Oracle store name accepted by <c>DataTypeSettings:Store</c>.</summary>
    public const string OracleStoreName = "oracle";

    public static IServiceCollection AddProcessorCore(
        this IServiceCollection services, IConfiguration configuration)
    {
        services.AddOptions<ProcessorOptions>()
            .Bind(configuration.GetSection(ProcessorOptions.SectionName))
            .Validate(o => !string.IsNullOrWhiteSpace(o.Domain),
                $"{ProcessorOptions.SectionName}:{nameof(ProcessorOptions.Domain)} must be set — " +
                "each deployment runs exactly one domain.")
            .ValidateOnStart();

        services.Configure<KafkaOptions>(configuration.GetSection(KafkaOptions.SectionName));
        services.Configure<BenchmarkOptions>(configuration.GetSection(BenchmarkOptions.SectionName));
        services.Configure<DataTypeSettingsOptions>(configuration.GetSection(DataTypeSettingsOptions.SectionName));
        services.Configure<OracleOptions>(configuration.GetSection(OracleOptions.SectionName));

        services.AddSharedFieldBuilders();
        services.AddDataTypeSettings(configuration);

        return services;
    }

    /// <summary>
    /// The domain-agnostic field builders. Registered as concrete types because
    /// <c>OutputMessageBuilder</c> takes them directly; each one is declared against
    /// <c>IInputMessage</c>, so these single instances serve every domain.
    /// </summary>
    public static IServiceCollection AddSharedFieldBuilders(this IServiceCollection services)
    {
        services.AddSingleton<OutputIdBuilder>();
        services.AddSingleton<ProcessedContentBuilder>();
        services.AddSingleton<ProcessedAtBuilder>();
        services.AddSingleton<ProcessorNameBuilder>();
        return services;
    }

    /// <summary>
    /// Wires the settings store, its cache and the refresh service. The store is chosen by
    /// configuration so the identical caching/refresh path runs against Oracle or in-memory data.
    /// </summary>
    public static IServiceCollection AddDataTypeSettings(
        this IServiceCollection services, IConfiguration configuration)
    {
        var storeName = configuration[$"{DataTypeSettingsOptions.SectionName}:Store"];
        if (string.IsNullOrWhiteSpace(storeName))
        {
            storeName = OracleStoreName;
        }

        if (string.Equals(storeName, InMemoryStoreName, StringComparison.OrdinalIgnoreCase))
        {
            services.AddSingleton<IDataTypeSettingsStore>(_ => new InMemoryDataTypeSettingsStore());
        }
        else if (string.Equals(storeName, OracleStoreName, StringComparison.OrdinalIgnoreCase))
        {
            services.AddSingleton<IDataTypeSettingsStore, OracleDataTypeSettingsStore>();
        }
        else
        {
            throw new InvalidOperationException(
                $"Unknown {DataTypeSettingsOptions.SectionName}:Store '{storeName}'. " +
                $"Expected '{OracleStoreName}' or '{InMemoryStoreName}'.");
        }

        // One instance behind two interfaces: the message path sees only the read surface, the refresh
        // service and health checks see only the cache lifecycle.
        services.AddSingleton<CachingDataTypeSettingsRepository>();
        services.AddSingleton<IDataTypeSettingsRepository>(sp =>
            sp.GetRequiredService<CachingDataTypeSettingsRepository>());
        services.AddSingleton<IDataTypeSettingsCache>(sp =>
            sp.GetRequiredService<CachingDataTypeSettingsRepository>());

        // Registered before the KafkaFlow hosted service so it loads (and can fail startup) first.
        services.AddHostedService<DataTypeSettingsRefreshService>();

        return services;
    }

    /// <summary>
    /// Liveness (no dependencies, so a store blip won't get the pod killed) and readiness (store
    /// reachable + settings snapshot loaded and fresh).
    /// </summary>
    public static IServiceCollection AddProcessorHealthChecks(this IServiceCollection services)
    {
        services.AddHealthChecks()
            .AddCheck("self", () => HealthCheckResult.Healthy("alive"), tags: new[] { "live" })
            .AddCheck<DataStoreHealthCheck>("datastore", tags: new[] { "ready" })
            .AddCheck<SettingsHealthCheck>("settings", tags: new[] { "ready" });

        return services;
    }
}
