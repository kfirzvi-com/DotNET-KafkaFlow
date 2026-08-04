using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Processor.Core.Application;
using Processor.Core.DataTypes;
using Processor.Core.Health;

namespace Processor.Core.Tests.Health;

public class HealthCheckTests
{
    private static readonly HealthCheckContext Context = new();

    private static CachingDataTypeSettingsRepository Repository(IDataTypeSettingsStore store) =>
        new(store, NullLogger<CachingDataTypeSettingsRepository>.Instance);

    private static IOptions<DataTypeSettingsOptions> Settings(int refreshMinutes = 10) =>
        Options.Create(new DataTypeSettingsOptions { RefreshMinutes = refreshMinutes });

    [Fact]
    public async Task DataStore_Healthy_WhenProbeSucceeds()
    {
        var check = new DataStoreHealthCheck(new InMemoryDataTypeSettingsStore());

        var result = await check.CheckHealthAsync(Context);

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact]
    public async Task DataStore_Unhealthy_WhenProbeFails()
    {
        var store = new InMemoryDataTypeSettingsStore { FailWith = new InvalidOperationException("down") };
        var check = new DataStoreHealthCheck(store);

        var result = await check.CheckHealthAsync(Context);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.NotNull(result.Exception);
    }

    [Fact]
    public async Task Settings_Unhealthy_WhenNeverLoaded()
    {
        var repository = Repository(new InMemoryDataTypeSettingsStore());
        var check = new SettingsHealthCheck(repository, Settings());

        var result = await check.CheckHealthAsync(Context);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Contains("never been loaded", result.Description);
    }

    [Fact]
    public async Task Settings_Healthy_WhenFreshlyLoaded()
    {
        var store = new InMemoryDataTypeSettingsStore(new[]
        {
            new DataTypeSetting { DataTypeId = "weather", IsActive = true }
        });
        var repository = Repository(store);
        await repository.RefreshAsync();
        var check = new SettingsHealthCheck(repository, Settings());

        var result = await check.CheckHealthAsync(Context);

        Assert.Equal(HealthStatus.Healthy, result.Status);
        Assert.Contains("1 settings", result.Description);
    }

    [Fact]
    public async Task Settings_Healthy_WhenLoadedEmpty()
    {
        // An empty settings table is a valid state (everything filtered), not a failure.
        var repository = Repository(new InMemoryDataTypeSettingsStore());
        await repository.RefreshAsync();
        var check = new SettingsHealthCheck(repository, Settings());

        var result = await check.CheckHealthAsync(Context);

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    [Fact]
    public async Task Settings_StaysUnhealthy_WhenTheOnlyLoadFailed()
    {
        var store = new InMemoryDataTypeSettingsStore { FailWith = new InvalidOperationException("down") };
        var repository = Repository(store);
        await repository.RefreshAsync();
        var check = new SettingsHealthCheck(repository, Settings());

        var result = await check.CheckHealthAsync(Context);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
    }

    [Fact]
    public async Task Settings_Degraded_WhenLastLoadOlderThanThreeIntervals()
    {
        // Loaded 2 hours ago against a 10-minute interval: past the 3× staleness threshold, so the pod
        // stays ready (it is still serving settings) but the state is surfaced.
        var cache = new LoadedAtCache(DateTimeOffset.UtcNow.AddHours(-2));
        var check = new SettingsHealthCheck(cache, Settings(refreshMinutes: 10));

        var result = await check.CheckHealthAsync(Context);

        Assert.Equal(HealthStatus.Degraded, result.Status);
        Assert.Contains("stale snapshot", result.Description);
    }

    [Fact]
    public async Task Settings_Healthy_WhenJustInsideTheStalenessThreshold()
    {
        // 25 minutes against a 10-minute interval is inside the 30-minute threshold.
        var cache = new LoadedAtCache(DateTimeOffset.UtcNow.AddMinutes(-25));
        var check = new SettingsHealthCheck(cache, Settings(refreshMinutes: 10));

        var result = await check.CheckHealthAsync(Context);

        Assert.Equal(HealthStatus.Healthy, result.Status);
    }

    /// <summary>A loaded cache whose last successful load happened at a chosen time.</summary>
    private sealed class LoadedAtCache : IDataTypeSettingsCache
    {
        public LoadedAtCache(DateTimeOffset loadedAtUtc) => LastSuccessfulLoadUtc = loadedAtUtc;

        public bool IsLoaded => true;

        public DateTimeOffset? LastSuccessfulLoadUtc { get; }

        public int Count => 1;

        public Task<bool> RefreshAsync(CancellationToken cancellationToken = default) => Task.FromResult(true);
    }
}
