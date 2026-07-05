using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Processor.Settings;
using StackExchange.Redis;
using Xunit;

namespace Processor.Tests.Settings;

public class RedisDataTypeSettingsRepositoryTests
{
    private static RedisDataTypeSettingsRepository CreateRepository(
        HashEntry[] entries,
        out Mock<IDatabase> db,
        int refreshSeconds = 15)
    {
        db = new Mock<IDatabase>();
        db.Setup(d => d.HashGetAllAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()))
            .ReturnsAsync(entries);

        var mux = new Mock<IConnectionMultiplexer>();
        mux.Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object>())).Returns(db.Object);

        var options = Options.Create(new DataTypeSettingsOptions { RefreshSeconds = refreshSeconds });
        return new RedisDataTypeSettingsRepository(mux.Object, options, NullLogger<RedisDataTypeSettingsRepository>.Instance);
    }

    [Fact]
    public async Task FindByIdAsync_ReturnsActiveSetting_FromBareBoolValue()
    {
        var repo = CreateRepository(new[] { new HashEntry("weather", "true") }, out _);

        var setting = await repo.FindByIdAsync("weather");

        Assert.NotNull(setting);
        Assert.Equal("weather", setting!.DataTypeId);
        Assert.True(setting.IsActive);
    }

    [Fact]
    public async Task FindByIdAsync_ReturnsSetting_FromJsonValue()
    {
        var repo = CreateRepository(
            new[] { new HashEntry("news", "{\"dataTypeId\":\"news\",\"isActive\":false}") }, out _);

        var setting = await repo.FindByIdAsync("news");

        Assert.NotNull(setting);
        Assert.False(setting!.IsActive);
    }

    [Fact]
    public async Task FindByIdAsync_IsCaseInsensitive()
    {
        var repo = CreateRepository(new[] { new HashEntry("Weather", "true") }, out _);

        var setting = await repo.FindByIdAsync("weather");

        Assert.NotNull(setting);
        Assert.True(setting!.IsActive);
    }

    [Fact]
    public async Task FindByIdAsync_ReturnsNull_ForUnknownDataType()
    {
        var repo = CreateRepository(new[] { new HashEntry("weather", "true") }, out _);

        Assert.Null(await repo.FindByIdAsync("does-not-exist"));
    }

    [Fact]
    public async Task FindByIdAsync_ReturnsNull_ForNullOrEmptyId()
    {
        var repo = CreateRepository(new[] { new HashEntry("weather", "true") }, out var db);

        Assert.Null(await repo.FindByIdAsync(null!));
        Assert.Null(await repo.FindByIdAsync("  "));

        // A blank id must not even touch Redis.
        db.Verify(d => d.HashGetAllAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()), Times.Never);
    }

    [Fact]
    public async Task Reads_AreCached_WithinTtl()
    {
        var repo = CreateRepository(new[] { new HashEntry("weather", "true") }, out var db, refreshSeconds: 60);

        await repo.FindByIdAsync("weather");
        await repo.FindByIdAsync("weather");
        await repo.FindAllAsync();

        // Only the first query loaded from Redis; the rest were served from cache.
        db.Verify(d => d.HashGetAllAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()), Times.Once);
    }

    [Fact]
    public async Task Cache_Reloads_WhenTtlIsZero()
    {
        var repo = CreateRepository(new[] { new HashEntry("weather", "true") }, out var db, refreshSeconds: 0);

        await repo.FindByIdAsync("weather");
        await repo.FindByIdAsync("weather");

        // TTL of 0 makes every read reload from Redis.
        db.Verify(d => d.HashGetAllAsync(It.IsAny<RedisKey>(), It.IsAny<CommandFlags>()), Times.Exactly(2));
    }

    [Fact]
    public async Task FindAllAsync_ReturnsAllParsedSettings()
    {
        var repo = CreateRepository(
            new[]
            {
                new HashEntry("weather", "true"),
                new HashEntry("news", "false"),
            },
            out _);

        var all = await repo.FindAllAsync();

        Assert.Equal(2, all.Count);
    }
}
