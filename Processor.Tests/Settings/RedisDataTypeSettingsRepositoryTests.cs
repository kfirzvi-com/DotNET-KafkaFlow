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
    /// </summary>
    private static RedisDataTypeSettingsRepository CreateRepository(
        (string id, string value)[] settings,
        out Mock<IDatabase> db,
        out Mock<IServer> server,
        int refreshSeconds = 15,
        bool useCache = true)
    {
        var store = settings.ToDictionary(s => Prefix + s.id, s => s.value, StringComparer.OrdinalIgnoreCase);

        RedisValue ValueFor(RedisKey k) =>
            store.TryGetValue(k.ToString(), out var v) ? v : RedisValue.Null;

        db = new Mock<IDatabase>();
        // Single GET (uncached path)
        db.Setup(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync((RedisKey k, CommandFlags _) => ValueFor(k));
        // MGET (cache reload)
        db.Setup(d => d.StringGetAsync(It.IsAny<RedisKey[]>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync((RedisKey[] ks, CommandFlags _) => ks.Select(ValueFor).ToArray());

        server = new Mock<IServer>();
        server.Setup(s => s.Keys(It.IsAny<int>(), It.IsAny<RedisValue>(), It.IsAny<int>(),
                                 It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CommandFlags>()))
            .Returns(store.Keys.Select(k => (RedisKey)k));

        var mux = new Mock<IConnectionMultiplexer>();
        mux.Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object>())).Returns(db.Object);
        mux.Setup(m => m.GetEndPoints(It.IsAny<bool>())).Returns(new EndPoint[] { new DnsEndPoint("localhost", 6379) });
        mux.Setup(m => m.GetServer(It.IsAny<EndPoint>(), It.IsAny<object>())).Returns(server.Object);

        var options = Options.Create(new DataTypeSettingsOptions
        {
            KeyPrefix = Prefix, RefreshSeconds = refreshSeconds, UseCache = useCache,
        });
        return new RedisDataTypeSettingsRepository(mux.Object, options, NullLogger<RedisDataTypeSettingsRepository>.Instance);
    }

    [Fact]
    public async Task FindByIdAsync_ReturnsActiveSetting_FromBareBoolValue()
    {
        var repo = CreateRepository(new[] { ("weather", "true") }, out _, out _);

        var setting = await repo.FindByIdAsync("weather");

        Assert.NotNull(setting);
        Assert.Equal("weather", setting!.DataTypeId);
        Assert.True(setting.IsActive);
    }

    [Fact]
    public async Task FindByIdAsync_ReturnsSetting_FromJsonValue()
    {
        var repo = CreateRepository(
            new[] { ("news", "{\"dataTypeId\":\"news\",\"isActive\":false}") }, out _, out _);

        var setting = await repo.FindByIdAsync("news");

        Assert.NotNull(setting);
        Assert.False(setting!.IsActive);
    }

    [Fact]
    public async Task FindByIdAsync_IsCaseInsensitive()
    {
        var repo = CreateRepository(new[] { ("Weather", "true") }, out _, out _);

        var setting = await repo.FindByIdAsync("weather");

        Assert.NotNull(setting);
        Assert.True(setting!.IsActive);
    }

    [Fact]
    public async Task FindByIdAsync_ReturnsNull_ForUnknownDataType()
    {
        var repo = CreateRepository(new[] { ("weather", "true") }, out _, out _);

        Assert.Null(await repo.FindByIdAsync("does-not-exist"));
    }

    [Fact]
    public async Task FindByIdAsync_ReturnsNull_ForNullOrEmptyId()
    {
        var repo = CreateRepository(new[] { ("weather", "true") }, out var db, out _);

        Assert.Null(await repo.FindByIdAsync(null!));
        Assert.Null(await repo.FindByIdAsync("  "));

        // A blank id must not even touch Redis.
        db.Verify(d => d.StringGetAsync(It.IsAny<RedisKey[]>(), It.IsAny<CommandFlags>()), Times.Never);
        db.Verify(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()), Times.Never);
    }

    [Fact]
    public async Task Reads_AreCached_WithinTtl()
    {
        var repo = CreateRepository(new[] { ("weather", "true") }, out var db, out _, refreshSeconds: 60);

        await repo.FindByIdAsync("weather");
        await repo.FindByIdAsync("weather");
        await repo.FindAllAsync();

        // Only the first query loaded from Redis (MGET); the rest were served from cache.
        db.Verify(d => d.StringGetAsync(It.IsAny<RedisKey[]>(), It.IsAny<CommandFlags>()), Times.Once);
    }

    [Fact]
    public async Task Cache_Reloads_WhenTtlIsZero()
    {
        var repo = CreateRepository(new[] { ("weather", "true") }, out var db, out _, refreshSeconds: 0);

        await repo.FindByIdAsync("weather");
        await repo.FindByIdAsync("weather");

        // TTL of 0 makes every read reload from Redis (SCAN + MGET).
        db.Verify(d => d.StringGetAsync(It.IsAny<RedisKey[]>(), It.IsAny<CommandFlags>()), Times.Exactly(2));
    }

    [Fact]
    public async Task Uncached_HitsRedisOnEveryFind()
    {
        var repo = CreateRepository(new[] { ("weather", "true") }, out var db, out _, useCache: false);

        var s1 = await repo.FindByIdAsync("weather");
        var s2 = await repo.FindByIdAsync("weather");

        Assert.True(s1!.IsActive);
        Assert.True(s2!.IsActive);

        // No caching: each lookup is a single GET, and the bulk MGET is never used.
        db.Verify(d => d.StringGetAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()), Times.Exactly(2));
        db.Verify(d => d.StringGetAsync(It.IsAny<RedisKey[]>(), It.IsAny<CommandFlags>()), Times.Never);
    }

    [Fact]
    public async Task FindAllAsync_ReturnsAllParsedSettings()
    {
        var repo = CreateRepository(new[] { ("weather", "true"), ("news", "false") }, out _, out _);

        var all = await repo.FindAllAsync();

        Assert.Equal(2, all.Count);
    }
}
