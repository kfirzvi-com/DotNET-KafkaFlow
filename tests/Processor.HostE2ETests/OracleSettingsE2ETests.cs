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

    [Fact]
    public async Task OracleStore_LoadsSettings_AndMapsNumberOneToActive()
    {
        await _infrastructure.SeedSettingsAsync(PostData.Domain, ("active-feed", true), ("dormant-feed", false));

        var topics = HostUnderTest.TopicsFor(PostData.Domain, "oracle-load");
        await _infrastructure.CreateTopicsAsync(topics.Input, topics.Output, topics.DeadLetter);

        await using var host = HostUnderTest.Create(_infrastructure, PostData.Domain, topics);
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
        // The same table holds both domains' rows; only the posts rows may reach a posts deployment.
        await _infrastructure.SeedSettingsAsync(PostData.Domain, ("posts-only", true));
        await _infrastructure.SeedSettingsAsync("profiles", ("profiles-only", true));

        var topics = HostUnderTest.TopicsFor(PostData.Domain, "domain-scope");
        await _infrastructure.CreateTopicsAsync(topics.Input, topics.Output, topics.DeadLetter);

        await using var host = HostUnderTest.Create(_infrastructure, PostData.Domain, topics);
        await host.StartAsync();

        var repository = host.Services.GetRequiredService<IDataTypeSettingsRepository>();

        Assert.NotNull(await repository.FindByIdAsync("posts-only"));
        Assert.Null(await repository.FindByIdAsync("profiles-only"));
        Assert.Single(await repository.FindAllAsync());
    }

    [Fact]
    public async Task InactiveDataTypeInOracle_FiltersTheMessage()
    {
        await PostWatermark.SeedAsync(_infrastructure, PostData.Domain, ("retired", false));

        var topics = HostUnderTest.TopicsFor(PostData.Domain, "inactive");
        await _infrastructure.CreateTopicsAsync(topics.Input, topics.Output, topics.DeadLetter);

        await using var host = HostUnderTest.Create(_infrastructure, PostData.Domain, topics);
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
        await PostWatermark.SeedAsync(_infrastructure, PostData.Domain, ("toggled", false));

        var topics = HostUnderTest.TopicsFor(PostData.Domain, "toggle");
        await _infrastructure.CreateTopicsAsync(topics.Input, topics.Output, topics.DeadLetter);

        await using var host = HostUnderTest.Create(_infrastructure, PostData.Domain, topics);
        await host.StartAsync();

        using var kafka = new KafkaClient(_infrastructure.BootstrapServers);

        // Inactive: filtered. A watermark proves it was passed over rather than merely slow.
        var firstPass = new PostWatermark(WatermarkFactories.Post);
        await kafka.ProduceAsync(topics.Input, Post("before"), "toggled");
        await firstPass.ProduceAsync(kafka, topics.Input);
        Assert.Empty(firstPass.ReadOutput(kafka, topics.Output));

        // Flip the row in Oracle and force the reload the timer would do.
        await _infrastructure.SetActiveAsync(PostData.Domain, "toggled", isActive: true);
        Assert.True(await host.RefreshSettingsAsync());

        // A second watermark bounds the read: everything up to it, which now includes "after".
        var secondPass = new PostWatermark(WatermarkFactories.Post);
        await kafka.ProduceAsync(topics.Input, Post("after"), "toggled");
        await secondPass.ProduceAsync(kafka, topics.Input);

        // The first watermark's own output message is on this topic too, so match the one under test.
        Assert.Contains(secondPass.ReadOutput(kafka, topics.Output), m => m.Id == "after");
    }

    [Fact]
    public async Task DataTypeId_IsAcceptedFromTheKafkaHeader()
    {
        await _infrastructure.SeedSettingsAsync(PostData.Domain, ("header-feed", true));

        var topics = HostUnderTest.TopicsFor(PostData.Domain, "header");
        await _infrastructure.CreateTopicsAsync(topics.Input, topics.Output, topics.DeadLetter);

        await using var host = HostUnderTest.Create(_infrastructure, PostData.Domain, topics);
        await host.StartAsync();

        using var kafka = new KafkaClient(_infrastructure.BootstrapServers);
        await kafka.ProduceAsync(topics.Input, Post("post-header"), "header-feed", useHeader: true);

        var produced = kafka.Consume<OutputMessage<PostData>>(topics.Output, 1, ProduceTimeout);
        Assert.Equal("post-header", Assert.Single(produced).Id);
    }

    [Fact]
    public async Task Readiness_IsHealthy_OnceOracleIsLoaded()
    {
        await _infrastructure.SeedSettingsAsync(PostData.Domain, ("ready-feed", true));

        var topics = HostUnderTest.TopicsFor(PostData.Domain, "ready");
        await _infrastructure.CreateTopicsAsync(topics.Input, topics.Output, topics.DeadLetter);

        await using var host = HostUnderTest.Create(_infrastructure, PostData.Domain, topics);
        await host.StartAsync();

        Assert.Equal(HealthStatus.Healthy, await host.ReadinessAsync());
    }

    [Fact]
    public async Task Startup_Fails_WhenOracleIsUnreachable()
    {
        var topics = HostUnderTest.TopicsFor(PostData.Domain, "oracle-down");
        await _infrastructure.CreateTopicsAsync(topics.Input, topics.Output, topics.DeadLetter);

        await using var host = HostUnderTest.Create(
            _infrastructure, PostData.Domain, topics,
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
        var topics = HostUnderTest.TopicsFor(PostData.Domain, "no-table");
        await _infrastructure.CreateTopicsAsync(topics.Input, topics.Output, topics.DeadLetter);

        await using var host = HostUnderTest.Create(
            _infrastructure, PostData.Domain, topics,
            new Dictionary<string, string?>
            {
                ["Oracle:SettingsTable"] = "NO_SUCH_TABLE",
                ["DataTypeSettings:StartupRetryDelaySeconds"] = "0"
            });

        await Assert.ThrowsAnyAsync<Exception>(() => host.StartAsync());
    }

    [Fact]
    public async Task OracleOutage_AfterStartup_DoesNotStopProcessing()
    {
        await _infrastructure.SeedSettingsAsync(PostData.Domain, ("resilient", true));

        var topics = HostUnderTest.TopicsFor(PostData.Domain, "outage");
        await _infrastructure.CreateTopicsAsync(topics.Input, topics.Output, topics.DeadLetter);

        await using var host = HostUnderTest.Create(_infrastructure, PostData.Domain, topics);
        await host.StartAsync();

        // Deleting every row is the database-side equivalent of the settings vanishing. The snapshot
        // must keep serving the loaded values until a *successful* refresh replaces them.
        await using (var connection = new OracleConnection(_infrastructure.OracleConnectionString))
        {
            await connection.OpenAsync();
            await connection.ExecuteAsync(
                $"DELETE FROM {InfrastructureFixture.SettingsTable} WHERE DOMAIN_NAME = :domain",
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
        await _infrastructure.SeedSettingsAsync(PostData.Domain, ("engagement", true));
        await _infrastructure.SeedSettingsAsync("profiles", ("directory", true));

        var postsTopics = HostUnderTest.TopicsFor(PostData.Domain, "both-posts");
        var profilesTopics = HostUnderTest.TopicsFor("profiles", "both-profiles");
        await _infrastructure.CreateTopicsAsync(
            postsTopics.Input, postsTopics.Output, postsTopics.DeadLetter,
            profilesTopics.Input, profilesTopics.Output, profilesTopics.DeadLetter);

        // Two deployments of the same image, differing only in Processor:Domain.
        await using var postsHost = HostUnderTest.Create(_infrastructure, PostData.Domain, postsTopics);
        await using var profilesHost = HostUnderTest.Create(_infrastructure, "profiles", profilesTopics);
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
