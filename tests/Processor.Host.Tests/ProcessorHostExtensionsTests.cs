using KafkaFlow;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Processor.Core;
using Processor.Core.Application;
using Processor.Core.Building;
using Processor.Core.DataTypes;
using Processor.Core.Diagnostics;
using Processor.Core.Domains;
using Processor.Core.Messages;
using Processor.Domains.Posts;
using Processor.Domains.Profiles;

namespace Processor.Host.Tests;

/// <summary>
/// The host's composition. These tests are the reason the wiring lives in
/// <see cref="ProcessorHostExtensions"/> rather than in <c>Program.cs</c>: domain selection and the DI
/// graph are what a misconfigured deployment would get wrong, and both are checkable without Kafka or
/// Oracle running.
/// </summary>
public class ProcessorHostExtensionsTests
{
    /// <summary>
    /// Minimal configuration for a host: a domain plus the in-memory store, so no Oracle is needed to
    /// prove the graph is complete.
    /// </summary>
    private static Dictionary<string, string?> Config(string? domain, params (string Key, string Value)[] extra)
    {
        var values = new Dictionary<string, string?>
        {
            [$"{DataTypeSettingsOptions.SectionName}:Store"] =
                ProcessorCoreServiceCollectionExtensions.InMemoryStoreName,
            [$"{KafkaOptions.SectionName}:Brokers:0"] = "localhost:9092",
            ["Metrics:Port"] = "0"
        };

        if (domain is not null)
        {
            values[$"{ProcessorOptions.SectionName}:{nameof(ProcessorOptions.Domain)}"] = domain;
        }

        foreach (var (key, value) in extra)
        {
            values[key] = value;
        }

        return values;
    }

    private static WebApplicationBuilder BuilderWith(Dictionary<string, string?> configuration)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            EnvironmentName = Environments.Development
        });

        // Drop appsettings.json so a test never depends on the shipped defaults.
        builder.Configuration.Sources.Clear();
        builder.Configuration.AddInMemoryCollection(configuration);
        return builder;
    }

    [Fact]
    public void AvailableDomains_ExposesBothDomains_WithUniqueNames()
    {
        var domains = ProcessorHostExtensions.AvailableDomains();

        Assert.Equal(2, domains.Count);
        Assert.Contains(domains, d => d is PostsDomainModule);
        Assert.Contains(domains, d => d is ProfilesDomainModule);
        Assert.Equal(
            domains.Select(d => d.Name).Distinct(StringComparer.OrdinalIgnoreCase).Count(),
            domains.Count);
    }

    [Theory]
    [InlineData("posts", typeof(PostsDomainModule))]
    [InlineData("profiles", typeof(ProfilesDomainModule))]
    [InlineData("PROFILES", typeof(ProfilesDomainModule))]
    public void AddProcessor_ActivatesTheConfiguredDomain(string domain, Type expectedModule)
    {
        var builder = BuilderWith(Config(domain));

        var module = builder.AddProcessor();

        Assert.IsType(expectedModule, module);
    }

    [Fact]
    public void AddProcessor_Throws_OnUnknownDomain()
    {
        var builder = BuilderWith(Config("stocks"));

        var ex = Assert.Throws<InvalidOperationException>(() => builder.AddProcessor());

        Assert.Contains("Unknown domain 'stocks'", ex.Message);
    }

    [Fact]
    public void AddProcessor_Throws_WhenDomainMissing()
    {
        var builder = BuilderWith(Config(domain: null));

        var ex = Assert.Throws<InvalidOperationException>(() => builder.AddProcessor());

        Assert.Contains("Processor:Domain", ex.Message);
    }

    [Fact]
    public void AddProcessor_Throws_WhenNoBrokersConfigured()
    {
        var config = Config("posts");
        config.Remove($"{KafkaOptions.SectionName}:Brokers:0");
        var builder = BuilderWith(config);

        var ex = Assert.Throws<InvalidOperationException>(() => builder.AddProcessor());

        Assert.Contains("Kafka:Brokers", ex.Message);
    }

    [Fact]
    public void AddProcessor_Throws_OnUnknownStore()
    {
        var builder = BuilderWith(Config("posts",
            ($"{DataTypeSettingsOptions.SectionName}:Store", "mongodb")));

        var ex = Assert.Throws<InvalidOperationException>(() => builder.AddProcessor());

        Assert.Contains("Unknown DataTypeSettings:Store", ex.Message);
    }

    [Fact]
    public void AddProcessor_BuildsAResolvablePostsGraph()
    {
        var builder = BuilderWith(Config("posts"));
        builder.AddProcessor();

        using var app = builder.Build();

        // If the generics were wired wrong anywhere, this is where it would surface.
        Assert.NotNull(app.Services.GetRequiredService<IOutputMessageBuilder<PostInputMessage, PostData>>());
        Assert.NotNull(app.Services.GetRequiredService<IDomainDataBuilder<PostInputMessage, PostData>>());
        Assert.NotNull(app.Services.GetRequiredService<IDataTypeSettingsRepository>());
        Assert.NotNull(app.Services.GetRequiredService<IDataTypeSettingsCache>());
        Assert.NotNull(app.Services.GetRequiredService<KafkaConsumerStatistics>());
    }

    [Fact]
    public void AddProcessor_BuildsAResolvableProfilesGraph()
    {
        var builder = BuilderWith(Config("profiles"));
        builder.AddProcessor();

        using var app = builder.Build();

        Assert.NotNull(
            app.Services.GetRequiredService<IOutputMessageBuilder<ProfileInputMessage, ProfileData>>());
        Assert.NotNull(app.Services.GetRequiredService<IDomainDataBuilder<ProfileInputMessage, ProfileData>>());
    }

    [Fact]
    public void AddProcessor_RegistersOnlyTheSelectedDomainsTypes()
    {
        var builder = BuilderWith(Config("posts"));
        builder.AddProcessor();

        using var app = builder.Build();

        // A posts deployment must not carry the profiles pipeline: one image, one domain per run.
        Assert.Null(app.Services.GetService<IOutputMessageBuilder<ProfileInputMessage, ProfileData>>());
        Assert.Null(app.Services.GetService<IDomainDataBuilder<ProfileInputMessage, ProfileData>>());
    }

    [Fact]
    public void AddProcessor_RegistersTheSettingsRefreshHostedService()
    {
        var builder = BuilderWith(Config("posts"));
        builder.AddProcessor();

        using var app = builder.Build();

        var hostedServices = app.Services.GetServices<IHostedService>().ToList();
        Assert.Contains(hostedServices, s => s is DataTypeSettingsRefreshService);
    }

    [Fact]
    public void AddProcessor_StartsTheSettingsServiceBeforeTheKafkaConsumer()
    {
        var builder = BuilderWith(Config("posts"));
        builder.AddProcessor();

        using var app = builder.Build();

        var hostedServices = app.Services.GetServices<IHostedService>().ToList();
        var settingsIndex = hostedServices.FindIndex(s => s is DataTypeSettingsRefreshService);
        var kafkaIndex = hostedServices.FindIndex(s => s.GetType().Namespace?.StartsWith("KafkaFlow") == true);

        Assert.True(settingsIndex >= 0, "the settings refresh service is not registered");
        Assert.True(kafkaIndex >= 0, "the KafkaFlow hosted service is not registered");
        // Ordering is the fail-fast guarantee: settings load (and can abort startup) before consuming.
        Assert.True(
            settingsIndex < kafkaIndex,
            $"settings service (index {settingsIndex}) must start before KafkaFlow (index {kafkaIndex})");
    }

    [Fact]
    public void AddProcessor_BindsKafkaOptionsFromConfiguration()
    {
        var builder = BuilderWith(Config("posts",
            ($"{KafkaOptions.SectionName}:{nameof(KafkaOptions.InputTopic)}", "posts-in"),
            ($"{KafkaOptions.SectionName}:{nameof(KafkaOptions.OutputTopic)}", "posts-out"),
            ($"{KafkaOptions.SectionName}:{nameof(KafkaOptions.DeadLetterTopic)}", "posts-dlq"),
            ($"{KafkaOptions.SectionName}:{nameof(KafkaOptions.WorkersCount)}", "4"),
            ($"{KafkaOptions.SectionName}:{nameof(KafkaOptions.BufferSize)}", "50"),
            ($"{KafkaOptions.SectionName}:Brokers:0", "broker-1:9092"),
            ($"{KafkaOptions.SectionName}:Brokers:1", "broker-2:9092")));
        builder.AddProcessor();

        using var app = builder.Build();

        var kafka = app.Services.GetRequiredService<IOptions<KafkaOptions>>().Value;
        Assert.Equal("posts-in", kafka.InputTopic);
        Assert.Equal("posts-out", kafka.OutputTopic);
        Assert.Equal("posts-dlq", kafka.DeadLetterTopic);
        Assert.Equal(4, kafka.WorkersCount);
        Assert.Equal(50, kafka.BufferSize);
        Assert.Equal(new[] { "broker-1:9092", "broker-2:9092" }, kafka.Brokers);
    }

    [Fact]
    public void AddProcessor_BindsDataTypeSettingsOptions_WithDocumentedDefaults()
    {
        var builder = BuilderWith(Config("posts"));
        builder.AddProcessor();

        using var app = builder.Build();

        var settings = app.Services.GetRequiredService<IOptions<DataTypeSettingsOptions>>().Value;
        Assert.Equal(10, settings.RefreshMinutes);
        Assert.Equal(3, settings.StartupAttempts);
        Assert.Equal("data-type-id", settings.HeaderName);
    }

    [Fact]
    public void AddProcessor_DefaultsToConsumingFromEarliest()
    {
        var builder = BuilderWith(Config("posts"));
        builder.AddProcessor();

        using var app = builder.Build();

        // Overrides KafkaFlow's own `latest`: a deployment must not skip the existing backlog.
        var kafka = app.Services.GetRequiredService<IOptions<KafkaOptions>>().Value;
        Assert.Equal(KafkaFlow.AutoOffsetReset.Earliest, kafka.ResolveAutoOffsetReset());
    }

    [Fact]
    public void AddProcessor_Throws_OnUnknownAutoOffsetReset()
    {
        var builder = BuilderWith(Config("posts",
            ($"{KafkaOptions.SectionName}:{nameof(KafkaOptions.AutoOffsetReset)}", "beginning")));

        var ex = Assert.Throws<InvalidOperationException>(() => builder.AddProcessor());

        Assert.Contains("Kafka:AutoOffsetReset", ex.Message);
    }

    [Fact]
    public void AddProcessor_ConfiguresTheKafkaConsumerAndBothProducers()
    {
        var builder = BuilderWith(Config("posts"));
        builder.AddProcessor();

        using var app = builder.Build();

        // Producer registrations come from the domain module; resolving them proves the closed generic
        // producer types reached KafkaFlow's configuration.
        Assert.NotNull(app.Services.GetRequiredService<IMessageProducer<OutputMessage<PostData>>>());
        Assert.NotNull(app.Services.GetRequiredService<IMessageProducer<DeadLetterMessage<PostData>>>());
    }

    [Fact]
    public void AddProcessor_ResolvesTheClosedMessageHandlerForTheSelectedDomain()
    {
        var builder = BuilderWith(Config("profiles"));
        builder.AddProcessor();

        using var app = builder.Build();

        var handler = app.Services.GetRequiredService<MessageHandler<ProfileInputMessage, ProfileData>>();

        Assert.IsAssignableFrom<IMessageHandler<ProfileInputMessage>>(handler);
    }
}
