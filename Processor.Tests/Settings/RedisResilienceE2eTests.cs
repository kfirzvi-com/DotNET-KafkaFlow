using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Processor.Settings;
using StackExchange.Redis;
using Testcontainers.Redis;
using Xunit;

namespace Processor.Tests.Settings;

/// <summary>
/// End-to-end resilience tests against a real Redis (Testcontainers). Requires a running Docker daemon.
/// </summary>
[Trait("Category", "e2e")]
public class RedisResilienceE2eTests
{
    private const string Prefix = "datatypesettings:";

    private static IConnectionMultiplexer Connect(string connectionString)
    {
        var config = ConfigurationOptions.Parse(connectionString);
        config.AbortOnConnectFail = false;
        config.ConnectTimeout = 1000;
        config.ConnectRetry = 1;
        return ConnectionMultiplexer.Connect(config);
    }

    private static RedisDataTypeSettingsRepository Repo(IConnectionMultiplexer mux, bool useCache = true) =>
        new(mux, Options.Create(new DataTypeSettingsOptions { KeyPrefix = Prefix, UseCache = useCache, RefreshSeconds = 1 }),
            NullLogger<RedisDataTypeSettingsRepository>.Instance);

    private static RedisContainer NewRedis() => new RedisBuilder().WithImage("redis:7.4-alpine").Build();

    [Fact]
    public async Task InitialLoad_Succeeds_AndServesFromSnapshot()
    {
        await using var redis = NewRedis();
        await redis.StartAsync();
        var mux = Connect(redis.GetConnectionString());

        var db = mux.GetDatabase();
        await db.StringSetAsync(Prefix + "weather", "{\"dataTypeId\":\"weather\",\"isActive\":true}");
        await db.StringSetAsync(Prefix + "news", "{\"dataTypeId\":\"news\",\"isActive\":false}");

        var repo = Repo(mux);
        await repo.InitializeAsync();

        Assert.True(repo.IsLoaded);
        Assert.Equal(2, repo.Count);
        Assert.True((await repo.FindByIdAsync("weather"))!.IsActive);
        Assert.False((await repo.FindByIdAsync("news"))!.IsActive);
    }

    [Fact]
    public async Task RedisOutageAfterLoad_KeepsStaleSnapshot_AndNeverHammersRedis()
    {
        await using var redis = NewRedis();
        await redis.StartAsync();
        var mux = Connect(redis.GetConnectionString());
        await mux.GetDatabase().StringSetAsync(Prefix + "weather", "{\"dataTypeId\":\"weather\",\"isActive\":true}");

        var repo = Repo(mux);
        await repo.InitializeAsync();
        Assert.True(repo.IsLoaded);

        // Redis goes away.
        await redis.StopAsync();

        // A refresh now fails but retains the stale snapshot.
        Assert.False(await repo.RefreshAsync());
        Assert.True(repo.IsLoaded);
        Assert.Equal(1, repo.Count);

        // The message path keeps serving from the snapshot for many reads with Redis down —
        // proving reads don't depend on (or hammer) Redis.
        for (var i = 0; i < 10_000; i++)
        {
            var setting = await repo.FindByIdAsync("weather");
            Assert.NotNull(setting);
            Assert.True(setting!.IsActive);
        }
    }

    [Fact]
    public async Task InitialLoad_Throws_WhenRedisUnreachable()
    {
        // Nothing listening on this port -> initial load must fail so the app would crash at startup.
        var mux = Connect("localhost:6399");
        var repo = Repo(mux);

        await Assert.ThrowsAsync<InvalidOperationException>(() => repo.InitializeAsync());
        Assert.False(repo.IsLoaded);
    }
}
