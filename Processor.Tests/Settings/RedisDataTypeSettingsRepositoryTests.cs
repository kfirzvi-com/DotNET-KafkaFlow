using System.Net;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Processor.Settings;
using StackExchange.Redis;
using Xunit;

namespace Processor.Tests.Settings;

public class RedisDataTypeSettingsRepositoryTests
{
    private const string Prefix = "datatypesettings:";

    /// <summary>
    /// Builds a repository over an in-memory key/value store. Settings live under
    /// <c>datatypesettings:{id}</c> String keys; GET and SCAN+MGET are backed by the dictionary.
    /// When <paramref name="failing"/> is true, Redis calls throw (simulating an outage).
    /// </summary>
    private static RedisDataTypeSettingsRepository CreateRepository(
        (string id, string value)[] settings,
        out Mock<IDatabase> db,
        bool useCache = true,
        bool failing = false)
    {
        var store = settings.ToDictionary(s => Prefix + s.id, s => s.value, StringComparer.OrdinalIgnoreCase);
        RedisValue ValueFor(RedisKey k) => store.TryGetValue(k.ToString(), out var v) ? v : RedisValue.Null;

        db = new Mock<IDatabase>();
        var server = new Mock<IServer>();
        if (failing)
        {
            var boom = new RedisConnectionException(ConnectionFailureType.UnableToConnect, "down");
            db.Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>())).ThrowsAsync(boom);
            db.Setup(d => d.StringGetAsync(It.IsAny<RedisKey[]>(), It.IsAny<CommandFlags>())).ThrowsAsync(boom);
            server.Setup(s => s.Keys(It.IsAny<int>(), It.IsAny<RedisValue>(), It.IsAny<int>(),
                                     It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CommandFlags>())).Throws(boom);
        }
        else
        {
            db.Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
                .ReturnsAsync((RedisKey k, CommandFlags _) => ValueFor(k));
            db.Setup(d => d.StringGetAsync(It.IsAny<RedisKey[]>(), It.IsAny<CommandFlags>()))
                .ReturnsAsync((RedisKey[] ks, CommandFlags _) => ks.Select(ValueFor).ToArray());
            server.Setup(s => s.Keys(It.IsAny<int>(), It.IsAny<RedisValue>(), It.IsAny<int>(),
                                     It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CommandFlags>()))
                .Returns(store.Keys.Select(k => (RedisKey)k));
        }

        var mux = new Mock<IConnectionMultiplexer>();
        mux.Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object>())).Returns(db.Object);
        mux.Setup(m => m.GetEndPoints(It.IsAny<bool>())).Returns(new EndPoint[] { new DnsEndPoint("localhost", 6379) });
        mux.Setup(m => m.GetServer(It.IsAny<EndPoint>(), It.IsAny<object>())).Returns(server.Object);

        var options = Options.Create(new DataTypeSettingsOptions { KeyPrefix = Prefix, UseCache = useCache });
        return new RedisDataTypeSettingsRepository(mux.Object, options, NullLogger<RedisDataTypeSettingsRepository>.Instance);
    }

    [Fact]
    public async Task InitializeAsync_LoadsSnapshot_ThenReadsAreServedFromMemory()
    {
        var repo = CreateRepository(new[] { ("weather", "true"), ("news", "false") }, out var db);

        await repo.InitializeAsync();

        Assert.True(repo.IsLoaded);
        Assert.Equal(2, repo.Count);
        Assert.True((await repo.FindByIdAsync("weather"))!.IsActive);
        Assert.False((await repo.FindByIdAsync("news"))!.IsActive);
        Assert.Null(await repo.FindByIdAsync("unknown"));

        // The snapshot loaded once (MGET); the three reads touched Redis zero more times.
        db.Verify(d => d.StringGetAsync(It.IsAny<RedisKey[]>(), It.IsAny<CommandFlags>()), Times.Once);
        db.Verify(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()), Times.Never);
    }

    [Fact]
    public async Task CachedReads_NeverTouchRedis_EvenBeforeLoad()
    {
        var repo = CreateRepository(new[] { ("weather", "true") }, out var db);

        // No load yet: reads return null but must not hit Redis (the refresh service owns loading).
        Assert.Null(await repo.FindByIdAsync("weather"));
        db.Verify(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()), Times.Never);
        db.Verify(d => d.StringGetAsync(It.IsAny<RedisKey[]>(), It.IsAny<CommandFlags>()), Times.Never);
    }

    [Fact]
    public async Task FindByIdAsync_IsCaseInsensitive()
    {
        var repo = CreateRepository(new[] { ("Weather", "true") }, out _);
        await repo.InitializeAsync();
        Assert.True((await repo.FindByIdAsync("weather"))!.IsActive);
    }

    [Fact]
    public async Task InitializeAsync_Throws_WhenRedisDown()
    {
        var repo = CreateRepository(Array.Empty<(string, string)>(), out _, failing: true);

        await Assert.ThrowsAsync<InvalidOperationException>(() => repo.InitializeAsync());
        Assert.False(repo.IsLoaded);
    }

    [Fact]
    public async Task RefreshAsync_Failure_KeepsStaleSnapshot()
    {
        // Load once successfully...
        var repo = CreateRepository(new[] { ("weather", "true") }, out var db);
        await repo.InitializeAsync();
        Assert.True(repo.IsLoaded);
        var loadedAt = repo.LastSuccessfulLoadUtc;

        // ...then Redis goes down: refresh fails but the stale snapshot is retained.
        var boom = new RedisConnectionException(ConnectionFailureType.UnableToConnect, "down");
        db.Setup(d => d.StringGetAsync(It.IsAny<RedisKey[]>(), It.IsAny<CommandFlags>())).ThrowsAsync(boom);

        var ok = await repo.RefreshAsync();

        Assert.False(ok);
        Assert.True(repo.IsLoaded);                                   // still serving
        Assert.Equal(1, repo.Count);                                  // stale entry kept
        Assert.True((await repo.FindByIdAsync("weather"))!.IsActive); // still resolves
        Assert.Equal(loadedAt, repo.LastSuccessfulLoadUtc);           // timestamp not advanced
    }

    [Fact]
    public async Task Uncached_HitsRedisOnEveryFind()
    {
        var repo = CreateRepository(new[] { ("weather", "true") }, out var db, useCache: false);

        Assert.True((await repo.FindByIdAsync("weather"))!.IsActive);
        Assert.True((await repo.FindByIdAsync("weather"))!.IsActive);

        db.Verify(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()), Times.Exactly(2));
        db.Verify(d => d.StringGetAsync(It.IsAny<RedisKey[]>(), It.IsAny<CommandFlags>()), Times.Never);
    }
}
