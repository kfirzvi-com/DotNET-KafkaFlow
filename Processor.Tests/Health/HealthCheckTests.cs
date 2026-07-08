using System.Net;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Moq;
using Processor.Health;
using Processor.Settings;
using StackExchange.Redis;
using Xunit;

namespace Processor.Tests.Health;

public class HealthCheckTests
{
    private static readonly HealthCheckContext Context = new();

    private static Mock<IConnectionMultiplexer> Mux(bool up, out Mock<IDatabase> db, out Mock<IServer> server)
    {
        db = new Mock<IDatabase>();
        server = new Mock<IServer>();
        if (up)
        {
            db.Setup(d => d.PingAsync(It.IsAny<CommandFlags>())).ReturnsAsync(TimeSpan.FromMilliseconds(1));
            db.Setup(d => d.StringGetAsync(It.IsAny<RedisKey[]>(), It.IsAny<CommandFlags>()))
                .ReturnsAsync((RedisKey[] ks, CommandFlags _) => ks.Select(_ => (RedisValue)"true").ToArray());
            server.Setup(s => s.Keys(It.IsAny<int>(), It.IsAny<RedisValue>(), It.IsAny<int>(),
                                     It.IsAny<long>(), It.IsAny<int>(), It.IsAny<CommandFlags>()))
                .Returns(new RedisKey[] { "datatypesettings:weather" });
        }
        else
        {
            db.Setup(d => d.PingAsync(It.IsAny<CommandFlags>()))
                .ThrowsAsync(new RedisConnectionException(ConnectionFailureType.UnableToConnect, "down"));
        }

        var mux = new Mock<IConnectionMultiplexer>();
        mux.Setup(m => m.GetDatabase(It.IsAny<int>(), It.IsAny<object>())).Returns(db.Object);
        mux.Setup(m => m.GetEndPoints(It.IsAny<bool>())).Returns(new EndPoint[] { new DnsEndPoint("localhost", 6379) });
        mux.Setup(m => m.GetServer(It.IsAny<EndPoint>(), It.IsAny<object>())).Returns(server.Object);
        return mux;
    }

    [Fact]
    public async Task Redis_Healthy_WhenPingSucceeds()
    {
        var check = new RedisHealthCheck(Mux(up: true, out _, out _).Object);
        var result = await check.CheckHealthAsync(Context);
        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact]
    public async Task Redis_Unhealthy_WhenPingFails()
    {
        var check = new RedisHealthCheck(Mux(up: false, out _, out _).Object);
        var result = await check.CheckHealthAsync(Context);
        Assert.Equal(HealthStatus.Unhealthy, result.Status);
    }

    [Fact]
    public async Task Settings_Unhealthy_WhenNeverLoaded()
    {
        var options = Options.Create(new DataTypeSettingsOptions { KeyPrefix = "datatypesettings:", UseCache = true });
        var repo = new RedisDataTypeSettingsRepository(Mux(up: true, out _, out _).Object, options, NullLogger<RedisDataTypeSettingsRepository>.Instance);
        var check = new SettingsHealthCheck(repo, options);

        var result = await check.CheckHealthAsync(Context);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
    }

    [Fact]
    public async Task Settings_Healthy_WhenFreshlyLoaded()
    {
        var options = Options.Create(new DataTypeSettingsOptions { KeyPrefix = "datatypesettings:", UseCache = true, RefreshSeconds = 15 });
        var repo = new RedisDataTypeSettingsRepository(Mux(up: true, out _, out _).Object, options, NullLogger<RedisDataTypeSettingsRepository>.Instance);
        await repo.InitializeAsync();
        var check = new SettingsHealthCheck(repo, options);

        var result = await check.CheckHealthAsync(Context);

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact]
    public async Task Settings_Healthy_WhenUncached()
    {
        var options = Options.Create(new DataTypeSettingsOptions { KeyPrefix = "datatypesettings:", UseCache = false });
        var repo = new RedisDataTypeSettingsRepository(Mux(up: true, out _, out _).Object, options, NullLogger<RedisDataTypeSettingsRepository>.Instance);
        var check = new SettingsHealthCheck(repo, options);

        var result = await check.CheckHealthAsync(Context);

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }
}
