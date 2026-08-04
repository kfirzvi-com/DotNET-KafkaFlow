using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Processor.Core;
using Processor.Core.Application;
using Processor.Core.DataTypes;
using Processor.Core.Domains;
using Processor.Host;

namespace Processor.HostE2ETests.Support;

/// <summary>
/// The real host, composed through the same <c>AddProcessor</c> / <c>MapProcessorEndpoints</c> path as
/// <c>Program.cs</c>, pointed at the containerised Kafka and Oracle. Nothing is mocked: the consumer,
/// producers, Oracle store, caching repository and refresh service are all the production types.
/// </summary>
public sealed class HostUnderTest : IAsyncDisposable
{
    private readonly WebApplication _app;
    private bool _started;

    private HostUnderTest(WebApplication app, IDomainModule domain, TopicSet topics)
    {
        _app = app;
        Domain = domain;
        Topics = topics;
    }

    public IDomainModule Domain { get; }

    public TopicSet Topics { get; }

    public IServiceProvider Services => _app.Services;

    /// <summary>Topic names for one run, unique per test so runs cannot see each other's messages.</summary>
    public sealed record TopicSet(string Input, string Output, string DeadLetter, string ConsumerGroup);

    public static TopicSet TopicsFor(string domain, string discriminator) => new(
        Input: $"{domain}-input-{discriminator}",
        Output: $"{domain}-output-{discriminator}",
        DeadLetter: $"{domain}-dlq-{discriminator}",
        ConsumerGroup: $"{domain}-group-{discriminator}");

    /// <summary>
    /// Builds the host for a domain against the given infrastructure. <paramref name="overrides"/> can
    /// change any configuration key, e.g. the refresh interval.
    /// </summary>
    public static HostUnderTest Create(
        InfrastructureFixture infrastructure,
        string domain,
        TopicSet topics,
        IDictionary<string, string?>? overrides = null)
    {
        var settings = new Dictionary<string, string?>
        {
            [$"{ProcessorOptions.SectionName}:{nameof(ProcessorOptions.Domain)}"] = domain,

            [$"{KafkaOptions.SectionName}:Brokers:0"] = infrastructure.BootstrapServers,
            [$"{KafkaOptions.SectionName}:{nameof(KafkaOptions.InputTopic)}"] = topics.Input,
            [$"{KafkaOptions.SectionName}:{nameof(KafkaOptions.OutputTopic)}"] = topics.Output,
            [$"{KafkaOptions.SectionName}:{nameof(KafkaOptions.DeadLetterTopic)}"] = topics.DeadLetter,
            [$"{KafkaOptions.SectionName}:{nameof(KafkaOptions.ConsumerGroupId)}"] = topics.ConsumerGroup,
            [$"{KafkaOptions.SectionName}:{nameof(KafkaOptions.WorkersCount)}"] = "2",
            [$"{KafkaOptions.SectionName}:{nameof(KafkaOptions.BufferSize)}"] = "10",
            // AutoOffsetReset is deliberately NOT set: leaving it at the production default (earliest)
            // means this suite proves that default is what makes message delivery deterministic rather
            // than a race against the first partition assignment.

            // Oracle is the data store for this suite — that is the point of it.
            [$"{DataTypeSettingsOptions.SectionName}:Store"] =
                ProcessorCoreServiceCollectionExtensions.OracleStoreName,
            [$"{DataTypeSettingsOptions.SectionName}:{nameof(DataTypeSettingsOptions.StartupRetryDelaySeconds)}"] = "1",
            [$"{OracleOptions.SectionName}:{nameof(OracleOptions.ConnectionString)}"] =
                infrastructure.OracleConnectionString,
            [$"{OracleOptions.SectionName}:{nameof(OracleOptions.SettingsTable)}"] =
                InfrastructureFixture.SettingsTable,

            // Port 0 lets the OS pick, so parallel hosts never collide on the metrics port.
            ["Metrics:Port"] = "0",
            ["Logging:LogLevel:Default"] = "Warning"
        };

        if (overrides is not null)
        {
            foreach (var pair in overrides)
            {
                settings[pair.Key] = pair.Value;
            }
        }

        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            EnvironmentName = Environments.Development
        });
        builder.Configuration.Sources.Clear();
        builder.Configuration.AddInMemoryCollection(settings);
        builder.Logging.SetMinimumLevel(LogLevel.Warning);

        var module = builder.AddProcessor();
        var app = builder.Build();
        app.MapProcessorEndpoints(module);

        return new HostUnderTest(app, module, topics);
    }

    /// <summary>
    /// Starts the host, which runs the real startup sequence: Oracle load first (fatal on failure),
    /// then the Kafka consumer.
    /// </summary>
    public async Task StartAsync()
    {
        await _app.StartAsync();
        _started = true;
    }

    public IDataTypeSettingsCache SettingsCache =>
        Services.GetRequiredService<IDataTypeSettingsCache>();

    /// <summary>Forces the reload the refresh timer would eventually do.</summary>
    public Task<bool> RefreshSettingsAsync() => SettingsCache.RefreshAsync();

    public async Task<HealthStatus> ReadinessAsync()
    {
        var report = await Services.GetRequiredService<HealthCheckService>()
            .CheckHealthAsync(r => r.Tags.Contains("ready"));
        return report.Status;
    }

    public async ValueTask DisposeAsync()
    {
        if (_started)
        {
            await _app.StopAsync();
        }

        await _app.DisposeAsync();
    }
}
