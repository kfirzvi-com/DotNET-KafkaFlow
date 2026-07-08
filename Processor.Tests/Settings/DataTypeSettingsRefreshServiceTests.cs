using System.Net;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Processor.Domain.DataTypes;
using Processor.Infrastructure.DataTypes;
using Processor.Application;
using StackExchange.Redis;
using Xunit;

namespace Processor.Tests.Settings;

public class DataTypeSettingsRefreshServiceTests
{
    private const string Prefix = "datatypesettings:";

    private static (DataTypeSettingsRefreshService service, RedisDataTypeSettingsRepository repo) Build(
        bool useCache, bool redisUp)
    {
        var db = new Mock<IDatabase>();
        var server = new Mock<IServer>();

        if (redisUp)
        {
            db.Setup(d => d.PingAsync(It.IsAny<CommandFlags>())).ReturnsAsync(TimeSpan.Zero);
            db.Setup(d => d.StringGetAsync(It.IsAny<RedisKey[]>(), It.IsAny<CommandFlags>()))
                .ReturnsAsync((RedisKey[] ks, CommandFlags _) => ks.Select(_ => (RedisValue)"true").ToArray());
            server.Setup(s => s.Keys(It.IsAny<int>(), It.IsAny<RedisValue>(), It.IsAny<int>(),
                                     It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CommandFlags>()))
                .Returns(new RedisKey[] { Prefix + "weather" });
        }
        else
        {
            var boom = new RedisConnectionException(ConnectionFailureType.UnableToConnect, "down");
            db.Setup(d => d.PingAsync(It.IsAny<CommandFlags>())).ThrowsAsync(boom);
            db.Setup(d => d.StringGetAsync(It.IsAny<RedisKey[]>(), It.IsAny<CommandFlags>())).ThrowsAsync(boom);
            server.Setup(s => s.Keys(It.IsAny<int>(), It.IsAny<RedisValue>(), It.IsAny<int>(),
                                     It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CommandFlags>())).Throws(boom);
        }

        var mux = new Mock<IConnectionMultiplexer>();
        mux.Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object>())).Returns(db.Object);
        mux.Setup(m => m.GetEndPoints(It.IsAny<bool>())).Returns(new EndPoint[] { new DnsEndPoint("localhost", 6379) });
        mux.Setup(m => m.GetServer(It.IsAny<EndPoint>(), It.IsAny<object>())).Returns(server.Object);

        var options = Options.Create(new DataTypeSettingsOptions { KeyPrefix = Prefix, UseCache = useCache, RefreshSeconds = 1 });
        var repo = new RedisDataTypeSettingsRepository(mux.Object, options, NullLogger<RedisDataTypeSettingsRepository>.Instance);
        var service = new DataTypeSettingsRefreshService(repo, mux.Object, options, NullLogger<DataTypeSettingsRefreshService>.Instance);
        return (service, repo);
    }

    [Fact]
    public async Task StartAsync_Throws_WhenRedisDown_Cached()
    {
        var (service, repo) = Build(useCache: true, redisUp: false);
        await Assert.ThrowsAsync<InvalidOperationException>(() => service.StartAsync(CancellationToken.None));
        Assert.False(repo.IsLoaded);
    }

    [Fact]
    public async Task StartAsync_Throws_WhenRedisDown_Uncached()
    {
        var (service, _) = Build(useCache: false, redisUp: false);
        await Assert.ThrowsAsync<InvalidOperationException>(() => service.StartAsync(CancellationToken.None));
    }

    [Fact]
    public async Task StartAsync_LoadsSnapshot_WhenRedisUp_Cached()
    {
        var (service, repo) = Build(useCache: true, redisUp: true);

        await service.StartAsync(CancellationToken.None);
        try
        {
            Assert.True(repo.IsLoaded);
            Assert.Equal(1, repo.Count);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task StartAsync_Succeeds_WhenRedisUp_Uncached()
    {
        var (service, _) = Build(useCache: false, redisUp: true);
        await service.StartAsync(CancellationToken.None);
        await service.StopAsync(CancellationToken.None);
    }
}
