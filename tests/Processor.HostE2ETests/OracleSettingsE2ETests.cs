using Dapper;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Oracle.ManagedDataAccess.Client;
using Processor.Core.DataTypes;
using Processor.Core.Messages;
using Processor.Domains.Posts;
using Processor.HostE2ETests.Support;
using PostWatermark = Processor.HostE2ETests.Support.Watermark<
    Processor.Domains.Posts.PostInputMessage, Processor.Domains.Posts.PostData>;

namespace Processor.HostE2ETests;

/// <summary>
/// The Oracle-specific behaviour that only a real database can prove: the SQL and column mapping, the
/// per-domain row scoping, live settings changes picked up by a refresh, and fail-fast startup when the
/// database is unreachable.
/// <para>
/// Every test creates its own settings table, so no test can see or clobber another's rows.
/// </para>
/// </summary>
[Collection(InfrastructureCollection.Name)]
public class OracleSettingsE2ETests
{
    /// <summary>Failure guard on reads that expect a message, not an expected wait.</summary>
    private static readonly TimeSpan ProduceTimeout = TimeSpan.FromSeconds(30);

    private readonly InfrastructureFixture _infrastructure;

    public OracleSettingsE2ETests(InfrastructureFixture infrastructure) =>
        _infrastructure = infrastructure;

    private static PostInputMessage Post(string id, string content = "hello world") => new()
    {
        Id = id,
        Content = content,
        DomainData = new PostData { AuthorHandle = "ada", Network = "x", Likes = 2, Shares = 1 }
    };

    /// <summary>Creates this test's own topics and settings table.</summary>
    private async Task<(HostUnderTest.TopicSet Topics, SettingsTable Settings)> ScaffoldAsync(
        string discriminator, string domain = PostData.Domain)
    {
        var topics = HostUnderTest.TopicsFor(domain, discriminator);
        await _infrastructure.CreateTopicsAsync(topics.Input, topics.Output, topics.DeadLetter);
        var settings = await _infrastructure.CreateSettingsTableAsync(discriminator);
        return (topics, settings);
    }

    [Fact]
    public async Task OracleStore_LoadsSettings_AndMapsNumberOneToActive()
    {
        var (topics, settings) = await ScaffoldAsync("oracle-load");
        await settings.SeedAsync(PostData.Domain, ("active-feed", true), ("dormant-feed", false));

        await using var host = HostUnderTest.Create(_infrastructure, PostData.Domain, topics, settings);
        await host.StartAsync();

        var repository = host.Services.GetRequiredService<IDataTypeSettingsRepository>();
        var all = await repository.FindAllAsync();

        Assert.Equal(2, all.Count);
        Assert.True((await repository.FindByIdAsync("active-feed"))!.IsActive);
        // NUMBER(1) = 0 must map to false, not to "any value present means active".
        Assert.False((await repository.FindByIdAsync("dormant-feed"))!.IsActive);
    }

    [Fact]
    public async Task Settings_AreScopedToTheDeploymentsDomain()
    {
        // One table holding both domains' rows; only the posts rows may reach a posts deployment.
        var (topics, settings) = await ScaffoldAsync("domain-scope");
        await settings.SeedAsync(PostData.Domain, ("posts-only", true));
        await settings.SeedAsync("profiles", ("profiles-only", true));

        await using var host = HostUnderTest.Create(_infrastructure, PostData.Domain, topics, settings);
        await host.StartAsync();

        var repository = host.Services.GetRequiredService<IDataTypeSettingsRepository>();

        Assert.NotNull(await repository.FindByIdAsync("posts-only"));
        Assert.Null(await repository.FindByIdAsync("profiles-only"));
        Assert.Single(await repository.FindAllAsync());

        // Both rows really are in the table — the filtering is the query's doing, not the seed's.
        Assert.Equal(2, (await settings.ReadAllAsync()).Count);
    }

    [Fact]
    public async Task InactiveDataTypeInOracle_FiltersTheMessage()
    {
        var (topics, settings) = await ScaffoldAsync("inactive");
        await PostWatermark.SeedAsync(settings, PostData.Domain, ("retired", false));

        await using var host = HostUnderTest.Create(_infrastructure, PostData.Domain, topics, settings);
        await host.StartAsync();

        using var kafka = new KafkaClient(_infrastructure.BootstrapServers);
        var watermark = new PostWatermark(WatermarkFactories.Post);

        await kafka.ProduceAsync(topics.Input, Post("post-inactive"), "retired");
        await watermark.ProduceAsync(kafka, topics.Input);

        // Watermarked, so these are exact: the processor has passed the filtered message.
        Assert.Empty(watermark.ReadOutput(kafka, topics.Output));
        Assert.Empty(watermark.ReadDeadLetters(kafka, topics.DeadLetter));
    }

    [Fact]
    public async Task ActivatingADataTypeInOracle_TakesEffectAfterARefresh()
    {
        var (topics, settings) = await ScaffoldAsync("toggle");
        await PostWatermark.SeedAsync(settings, PostData.Domain, ("toggled", false));

        await using var host = HostUnderTest.Create(_infrastructure, PostData.Domain, topics, settings);
        await host.StartAsync();

        using var kafka = new KafkaClient(_infrastructure.BootstrapServers);

        // Inactive: filtered. A watermark proves it was passed over rather than merely slow.
        var firstPass = new PostWatermark(WatermarkFactories.Post);
        await kafka.ProduceAsync(topics.Input, Post("before"), "toggled");
        await firstPass.ProduceAsync(kafka, topics.Input);
        Assert.Empty(firstPass.ReadOutput(kafka, topics.Output));

        // Flip the row in Oracle and force the reload the timer would do.
        await settings.SetActiveAsync(PostData.Domain, "toggled", isActive: true);
        Assert.True(await host.RefreshSettingsAsync());

        // A second watermark bounds the read: everything up to it, which now includes "after".
        var secondPass = new PostWatermark(WatermarkFactories.Post);
        await kafka.ProduceAsync(topics.Input, Post("after"), "toggled");
        await secondPass.ProduceAsync(kafka, topics.Input);

        // The first watermark's own output message is on this topic too, so match the one under test.
        Assert.Contains(secondPass.ReadOutput(kafka, topics.Output), m => m.Id == "after");
    }

    [Fact]
    public async Task DeactivatingADataTypeInOracle_TakesEffectAfterARefresh()
    {
        var (topics, settings) = await ScaffoldAsync("untoggle");
        await PostWatermark.SeedAsync(settings, PostData.Domain, ("live-feed", true));

        await using var host = HostUnderTest.Create(_infrastructure, PostData.Domain, topics, settings);
        await host.StartAsync();

        using var kafka = new KafkaClient(_infrastructure.BootstrapServers);

        // Active: processed.
        var firstPass = new PostWatermark(WatermarkFactories.Post);
        await kafka.ProduceAsync(topics.Input, Post("while-active"), "live-feed");
        await firstPass.ProduceAsync(kafka, topics.Input);
        Assert.Contains(firstPass.ReadOutput(kafka, topics.Output), m => m.Id == "while-active");

        await settings.SetActiveAsync(PostData.Domain, "live-feed", isActive: false);
        Assert.True(await host.RefreshSettingsAsync());

        var secondPass = new PostWatermark(WatermarkFactories.Post);
        await kafka.ProduceAsync(topics.Input, Post("while-inactive"), "live-feed");
        await secondPass.ProduceAsync(kafka, topics.Input);

        // Now filtered: nothing new beyond what the earlier pass already produced.
        Assert.DoesNotContain(secondPass.ReadOutput(kafka, topics.Output), m => m.Id == "while-inactive");
    }

    [Fact]
    public async Task DataTypeId_IsAcceptedFromTheKafkaHeader()
    {
        var (topics, settings) = await ScaffoldAsync("header");
        await settings.SeedAsync(PostData.Domain, ("header-feed", true));

        await using var host = HostUnderTest.Create(_infrastructure, PostData.Domain, topics, settings);
        await host.StartAsync();

        using var kafka = new KafkaClient(_infrastructure.BootstrapServers);
        await kafka.ProduceAsync(topics.Input, Post("post-header"), "header-feed", useHeader: true);

        var produced = kafka.Consume<OutputMessage<PostData>>(topics.Output, 1, ProduceTimeout);
        Assert.Equal("post-header", Assert.Single(produced).Id);
    }

    [Fact]
    public async Task Readiness_IsHealthy_OnceOracleIsLoaded()
    {
        var (topics, settings) = await ScaffoldAsync("ready");
        await settings.SeedAsync(PostData.Domain, ("ready-feed", true));

        await using var host = HostUnderTest.Create(_infrastructure, PostData.Domain, topics, settings);
        await host.StartAsync();

        Assert.Equal(HealthStatus.Healthy, await host.ReadinessAsync());
    }

    [Fact]
    public async Task Startup_Fails_WhenOracleIsUnreachable()
    {
        var (topics, settings) = await ScaffoldAsync("oracle-down");

        await using var host = HostUnderTest.Create(
            _infrastructure, PostData.Domain, topics, settings,
            new Dictionary<string, string?>
            {
                // A port nothing listens on: every startup attempt must fail.
                ["Oracle:ConnectionString"] =
                    "User Id=nobody;Password=nothing;Data Source=127.0.0.1:1/NOPDB",
                ["DataTypeSettings:StartupRetryDelaySeconds"] = "0"
            });

        // Exhausting the retry budget is fatal, so the host refuses to start.
        await Assert.ThrowsAnyAsync<Exception>(() => host.StartAsync());
    }

    [Fact]
    public async Task Startup_Fails_WhenTheSettingsTableDoesNotExist()
    {
        var (topics, settings) = await ScaffoldAsync("no-table");

        await using var host = HostUnderTest.Create(
            _infrastructure, PostData.Domain, topics, settings,
            new Dictionary<string, string?>
            {
                ["Oracle:SettingsTable"] = "NO_SUCH_TABLE",
                ["DataTypeSettings:StartupRetryDelaySeconds"] = "0"
            });

        await Assert.ThrowsAnyAsync<Exception>(() => host.StartAsync());
    }

    [Fact]
    public async Task Startup_Succeeds_WhenTheSettingsTableIsEmpty()
    {
        // An empty table is a valid state — everything filters as unknown — not a startup failure.
        var (topics, settings) = await ScaffoldAsync("empty-table");

        await using var host = HostUnderTest.Create(_infrastructure, PostData.Domain, topics, settings);
        await host.StartAsync();

        Assert.Equal(HealthStatus.Healthy, await host.ReadinessAsync());
        Assert.Empty(await host.Services.GetRequiredService<IDataTypeSettingsRepository>().FindAllAsync());
    }

    [Fact]
    public async Task OracleOutage_AfterStartup_DoesNotStopProcessing()
    {
        var (topics, settings) = await ScaffoldAsync("outage");
        await settings.SeedAsync(PostData.Domain, ("resilient", true));

        await using var host = HostUnderTest.Create(_infrastructure, PostData.Domain, topics, settings);
        await host.StartAsync();

        // Deleting every row is the database-side equivalent of the settings vanishing. The snapshot
        // must keep serving the loaded values until a *successful* refresh replaces them.
        await using (var connection = new OracleConnection(_infrastructure.OracleConnectionString))
        {
            await connection.OpenAsync();
            await connection.ExecuteAsync(
                $"DELETE FROM {settings.Name} WHERE DOMAIN_NAME = :domain",
                new { domain = PostData.Domain });
        }

        using var kafka = new KafkaClient(_infrastructure.BootstrapServers);
        await kafka.ProduceAsync(topics.Input, Post("still-working"), "resilient");

        var produced = kafka.Consume<OutputMessage<PostData>>(topics.Output, 1, ProduceTimeout);
        Assert.Equal("still-working", Assert.Single(produced).Id);
    }

    [Fact]
    public async Task BothDomains_ProcessTheirOwnMessages_FromTheSameCodeAndTable()
    {
        // One shared table for both deployments, to show the domain column is what separates them.
        var postsTopics = HostUnderTest.TopicsFor(PostData.Domain, "both-posts");
        var profilesTopics = HostUnderTest.TopicsFor("profiles", "both-profiles");
        await _infrastructure.CreateTopicsAsync(
            postsTopics.Input, postsTopics.Output, postsTopics.DeadLetter,
            profilesTopics.Input, profilesTopics.Output, profilesTopics.DeadLetter);

        var shared = await _infrastructure.CreateSettingsTableAsync("both-domains");
        await shared.SeedAsync(PostData.Domain, ("engagement", true));
        await shared.SeedAsync("profiles", ("directory", true));

        // Two deployments of the same image, differing only in Processor:Domain.
        await using var postsHost = HostUnderTest.Create(
            _infrastructure, PostData.Domain, postsTopics, shared);
        await using var profilesHost = HostUnderTest.Create(
            _infrastructure, "profiles", profilesTopics, shared);
        await postsHost.StartAsync();
        await profilesHost.StartAsync();

        Assert.Equal("posts", postsHost.Domain.Name);
        Assert.Equal("profiles", profilesHost.Domain.Name);

        using var kafka = new KafkaClient(_infrastructure.BootstrapServers);
        await kafka.ProduceAsync(postsTopics.Input, Post("post-x"), "engagement");
        await kafka.ProduceAsync(
            profilesTopics.Input,
            new Domains.Profiles.ProfileInputMessage
            {
                Id = "profile-x",
                Content = "a bio",
                DomainData = new Domains.Profiles.ProfileData
                {
                    Handle = "ada", Network = "x", Followers = 5000, Following = 250
                }
            },
            "directory");

        var posts = kafka.Consume<OutputMessage<PostData>>(postsTopics.Output, 1, ProduceTimeout);
        var profiles = kafka.Consume<OutputMessage<Domains.Profiles.ProfileData>>(
            profilesTopics.Output, 1, ProduceTimeout);

        Assert.Equal("posts", Assert.Single(posts).Domain);
        Assert.Equal("profiles", Assert.Single(profiles).Domain);
        Assert.Equal("micro", profiles[0].DomainData.AudienceTier);
    }
}
