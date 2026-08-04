using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.DependencyInjection;
using Processor.Core.Building;
using Processor.Core.DataTypes;
using Processor.Domains.Posts;
using Processor.Domains.Profiles;
using Processor.MockTests.Support;

namespace Processor.MockTests;

/// <summary>
/// Whole-app behaviours that a single message's outcome cannot express: fail-fast startup, live
/// settings changes, header-vs-key routing, and that a deployment only ever runs its own domain.
/// </summary>
public class AppBehaviourTests
{
    private const string PostsDataType = "engagement";

    private static List<DataTypeSetting> Active(string id) =>
        new() { new DataTypeSetting { DataTypeId = id, IsActive = true } };

    private static PostInputMessage Post(string id = "post-1", string content = "hello") => new()
    {
        Id = id,
        Content = content,
        DomainData = new PostData { AuthorHandle = "ada", Network = "x", Likes = 1, Shares = 1 }
    };

    private static MockProcessorApp<PostInputMessage, PostData> PostsApp(
        IEnumerable<DataTypeSetting> settings) =>
        MockProcessorApp<PostInputMessage, PostData>.Create(new PostsDomainModule(), settings);

    [Fact]
    public async Task Startup_Fails_WhenTheStoreIsDownForEveryAttempt()
    {
        await using var app = PostsApp(Active(PostsDataType));
        app.Store.FailWith = new InvalidOperationException("oracle down");

        // The refresh service exhausts its attempts and throws, so the host would exit non-zero.
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => app.StartAsync());

        Assert.Contains("refusing to start", ex.Message);
        Assert.False(app.SettingsCache.IsLoaded);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    public async Task Startup_Succeeds_AfterATransientStoreFailure(int failures)
    {
        await using var app = PostsApp(Active(PostsDataType));
        // Fails this many times, then recovers — inside the 3-attempt budget.
        app.Store.FailNextLoads = failures;

        await app.StartAsync();

        Assert.True(app.SettingsCache.IsLoaded);
        Assert.Equal(failures + 1, app.Store.LoadCount);

        // And the app works normally afterwards.
        await app.DeliverAsync(Post(), PostsDataType);
        Assert.Single(app.Produced);
    }

    [Fact]
    public async Task Startup_Fails_WhenFailuresExceedTheAttemptBudget()
    {
        await using var app = PostsApp(Active(PostsDataType));
        // One more failure than the 3 attempts allow.
        app.Store.FailNextLoads = 4;

        await Assert.ThrowsAsync<InvalidOperationException>(() => app.StartAsync());

        Assert.Equal(3, app.Store.LoadCount);
        Assert.False(app.SettingsCache.IsLoaded);
    }

    [Fact]
    public async Task Startup_LoadsTheSnapshotBeforeAnyMessageIsHandled()
    {
        await using var app = PostsApp(Active(PostsDataType));

        // Without startup the snapshot is empty, so an otherwise-valid message is filtered as unknown.
        await app.DeliverAsync(Post(), PostsDataType);
        Assert.Empty(app.Produced);

        await app.StartAsync();
        await app.DeliverAsync(Post(), PostsDataType);

        Assert.Single(app.Produced);
    }

    [Fact]
    public async Task StoreOutage_AfterStartup_KeepsProcessing_FromTheStaleSnapshot()
    {
        await using var app = PostsApp(Active(PostsDataType));
        await app.StartAsync();

        app.Store.FailWith = new InvalidOperationException("oracle down");
        var loadsBefore = app.Store.LoadCount;

        for (var i = 0; i < 25; i++)
        {
            await app.DeliverAsync(Post(id: $"post-{i}"), PostsDataType);
        }

        // The whole point of the snapshot: throughput during an outage costs zero store round-trips.
        Assert.Equal(25, app.Produced.Count);
        Assert.Equal(loadsBefore, app.Store.LoadCount);
    }

    [Fact]
    public async Task SettingsChange_TakesEffect_AfterARefresh()
    {
        await using var app = PostsApp(Active(PostsDataType));
        await app.StartAsync();

        await app.DeliverAsync(Post(id: "before"), PostsDataType);
        Assert.Single(app.Produced);

        // Deactivate the data type in the store, then force the refresh the timer would have done.
        app.Store.Replace(new[] { new DataTypeSetting { DataTypeId = PostsDataType, IsActive = false } });
        Assert.True(await app.SettingsCache.RefreshAsync());

        await app.DeliverAsync(Post(id: "after"), PostsDataType);

        Assert.Single(app.Produced);
        var outcome = await app.BuildAsync(Post(id: "after"), PostsDataType);
        Assert.Equal(BuildStatus.Filtered, outcome.Status);
        Assert.Equal("inactive_data_type", outcome.Reason);
    }

    [Fact]
    public async Task DataTypeId_IsAcceptedFromTheKafkaHeader()
    {
        await using var app = PostsApp(Active(PostsDataType));
        await app.StartAsync();

        await app.DeliverAsync(Post(), PostsDataType, useHeader: true);

        Assert.Single(app.Produced);
    }

    [Fact]
    public async Task Readiness_IsUnhealthyBeforeStartup_AndHealthyAfter()
    {
        await using var app = PostsApp(Active(PostsDataType));

        Assert.Equal(HealthStatus.Unhealthy, await ReadinessAsync(app));

        await app.StartAsync();

        Assert.Equal(HealthStatus.Healthy, await ReadinessAsync(app));
    }

    private static async Task<HealthStatus> ReadinessAsync(
        MockProcessorApp<PostInputMessage, PostData> app)
    {
        var service = app.HealthCheckService;
        var report = await service.CheckHealthAsync(r => r.Tags.Contains("ready"));
        return report.Status;
    }

    [Fact]
    public async Task EachDomainRunsIndependently_WithItsOwnDataTypesAndPayloads()
    {
        await using var postsApp = PostsApp(Active("engagement"));
        await using var profilesApp = MockProcessorApp<ProfileInputMessage, ProfileData>.Create(
            new ProfilesDomainModule(), Active("directory"));

        await postsApp.StartAsync();
        await profilesApp.StartAsync();

        await postsApp.DeliverAsync(Post(), "engagement");
        await profilesApp.DeliverAsync(
            new ProfileInputMessage
            {
                Id = "profile-1",
                Content = "bio",
                DomainData = new ProfileData
                {
                    Handle = "ada", Network = "x", Followers = 100, Following = 10
                }
            },
            "directory");

        Assert.Equal("posts", Assert.Single(postsApp.Produced).Domain);
        Assert.Equal("profiles", Assert.Single(profilesApp.Produced).Domain);

        // A posts deployment must not recognise the profiles data type, and vice versa.
        var crossOutcome = await postsApp.BuildAsync(Post(), "directory");
        Assert.Equal(BuildStatus.Filtered, crossOutcome.Status);
        Assert.Equal("unknown_data_type", crossOutcome.Reason);
    }

    [Fact]
    public async Task RefreshHostedService_IsRegistered_SoTheHostLoadsSettingsFirst()
    {
        await using var app = PostsApp(Active(PostsDataType));

        Assert.Contains(app.HostedServices, s => s is DataTypeSettingsRefreshService);
    }
}
