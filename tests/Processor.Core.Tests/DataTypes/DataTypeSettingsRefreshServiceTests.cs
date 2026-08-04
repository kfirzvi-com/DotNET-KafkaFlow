using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Processor.Core.Application;
using Processor.Core.DataTypes;

namespace Processor.Core.Tests.DataTypes;

/// <summary>
/// The startup and refresh contract: retry the initial load a bounded number of times then crash, and
/// afterwards tolerate failures forever without crashing.
/// </summary>
public class DataTypeSettingsRefreshServiceTests
{
    private static DataTypeSettingsOptions Settings(
        int startupAttempts = 3, int retryDelaySeconds = 5, int refreshMinutes = 10) => new()
        {
            StartupAttempts = startupAttempts,
            StartupRetryDelaySeconds = retryDelaySeconds,
            RefreshMinutes = refreshMinutes
        };

    private static DataTypeSettingsRefreshService Service(
        IDataTypeSettingsCache cache, DataTypeSettingsOptions options, TimeProvider timeProvider) =>
        new(cache,
            Options.Create(options),
            NullLogger<DataTypeSettingsRefreshService>.Instance,
            timeProvider);

    /// <summary>Cache whose refresh outcome is scripted per call, and which counts the calls.</summary>
    private sealed class ScriptedCache : IDataTypeSettingsCache
    {
        private readonly Queue<bool> _results;
        private readonly bool _fallback;

        public ScriptedCache(bool fallback, params bool[] results)
        {
            _results = new Queue<bool>(results);
            _fallback = fallback;
        }

        public int RefreshCalls { get; private set; }

        public bool IsLoaded { get; private set; }

        public DateTimeOffset? LastSuccessfulLoadUtc { get; private set; }

        public int Count => IsLoaded ? 1 : 0;

        public Task<bool> RefreshAsync(CancellationToken cancellationToken = default)
        {
            RefreshCalls++;
            var ok = _results.Count > 0 ? _results.Dequeue() : _fallback;
            if (ok)
            {
                IsLoaded = true;
                LastSuccessfulLoadUtc = DateTimeOffset.UtcNow;
            }

            return Task.FromResult(ok);
        }
    }

    [Fact]
    public async Task StartAsync_LoadsOnce_WhenFirstAttemptSucceeds()
    {
        var cache = new ScriptedCache(fallback: true);
        var service = Service(cache, Settings(), new FakeTimeProvider());

        await service.StartAsync(CancellationToken.None);
        try
        {
            Assert.Equal(1, cache.RefreshCalls);
            Assert.True(cache.IsLoaded);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task StartAsync_RetriesUntilSuccess_WithinTheAttemptBudget()
    {
        // Fails twice, succeeds on the third attempt — the default budget exactly.
        var cache = new ScriptedCache(fallback: true, false, false, true);
        var timeProvider = new FakeTimeProvider();
        var service = Service(cache, Settings(startupAttempts: 3, retryDelaySeconds: 0), timeProvider);

        await service.StartAsync(CancellationToken.None);
        try
        {
            Assert.Equal(3, cache.RefreshCalls);
            Assert.True(cache.IsLoaded);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task StartAsync_Throws_AfterExhaustingThreeAttempts()
    {
        var cache = new ScriptedCache(fallback: false);
        var service = Service(cache, Settings(startupAttempts: 3, retryDelaySeconds: 0), new FakeTimeProvider());

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => service.StartAsync(CancellationToken.None));

        // Exactly the configured number of attempts, then fatal: the host must not start unloaded.
        Assert.Equal(3, cache.RefreshCalls);
        Assert.Contains("3 attempt", ex.Message);
        Assert.False(cache.IsLoaded);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(5)]
    public async Task StartAsync_HonoursConfiguredAttemptCount(int attempts)
    {
        var cache = new ScriptedCache(fallback: false);
        var service = Service(cache, Settings(startupAttempts: attempts, retryDelaySeconds: 0), new FakeTimeProvider());

        await Assert.ThrowsAsync<InvalidOperationException>(() => service.StartAsync(CancellationToken.None));

        Assert.Equal(attempts, cache.RefreshCalls);
    }

    [Fact]
    public async Task StartAsync_WaitsTheConfiguredDelayBetweenAttempts()
    {
        var cache = new ScriptedCache(fallback: false);
        var timeProvider = new FakeTimeProvider();
        var service = Service(cache, Settings(startupAttempts: 3, retryDelaySeconds: 5), timeProvider);

        var start = service.StartAsync(CancellationToken.None);

        // With virtual time frozen, the service is parked on the retry delay after the first failure.
        await WaitForRefreshCallsAsync(cache, 1);
        Assert.False(start.IsCompleted);

        timeProvider.Advance(TimeSpan.FromSeconds(5));
        await WaitForRefreshCallsAsync(cache, 2);

        timeProvider.Advance(TimeSpan.FromSeconds(5));
        await Assert.ThrowsAsync<InvalidOperationException>(() => start);
        Assert.Equal(3, cache.RefreshCalls);
    }

    [Fact]
    public async Task RefreshLoop_RefreshesOnTheConfiguredInterval()
    {
        var interval = TimeSpan.FromMinutes(10);
        var cache = new ScriptedCache(fallback: true);
        var timeProvider = new FakeTimeProvider();
        var service = Service(cache, Settings(refreshMinutes: 10), timeProvider);

        await service.StartAsync(CancellationToken.None);
        try
        {
            Assert.Equal(1, cache.RefreshCalls); // the initial load

            var ticks = await SyncToLoopAsync(timeProvider, cache, interval);

            timeProvider.Advance(interval);
            await WaitForRefreshCallsAsync(cache, ticks + 1);

            // Short of the interval: no extra refresh.
            timeProvider.Advance(interval - TimeSpan.FromMinutes(1));
            await Task.Delay(100);
            Assert.Equal(ticks + 1, cache.RefreshCalls);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task RefreshLoop_KeepsRunning_WhenARefreshFails()
    {
        var interval = TimeSpan.FromMinutes(10);
        // Initial load succeeds; every later refresh fails.
        var cache = new ScriptedCache(fallback: false, true);
        var timeProvider = new FakeTimeProvider();
        var service = Service(cache, Settings(refreshMinutes: 10), timeProvider);

        await service.StartAsync(CancellationToken.None);
        try
        {
            var ticks = await SyncToLoopAsync(timeProvider, cache, interval);

            timeProvider.Advance(interval);
            await WaitForRefreshCallsAsync(cache, ticks + 1);

            // Failures are tolerated indefinitely: the loop must not die and the app must not crash.
            Assert.NotNull(service.ExecuteTask);
            Assert.False(service.ExecuteTask!.IsFaulted);
            Assert.False(service.ExecuteTask!.IsCompleted);
        }
        finally
        {
            await service.StopAsync(CancellationToken.None);
        }
    }

    [Fact]
    public async Task StopAsync_CompletesTheLoopWithoutFaulting()
    {
        var cache = new ScriptedCache(fallback: true);
        var service = Service(cache, Settings(), new FakeTimeProvider());

        await service.StartAsync(CancellationToken.None);
        await service.StopAsync(CancellationToken.None);

        Assert.NotNull(service.ExecuteTask);
        Assert.True(service.ExecuteTask!.IsCompleted);
        Assert.False(service.ExecuteTask!.IsFaulted);
    }

    /// <summary>
    /// Waits until the refresh loop's timer actually exists, then returns the refresh count at that
    /// point. <c>StartAsync</c> returns before <c>ExecuteAsync</c> has created the timer, so advancing
    /// virtual time immediately can land in the gap and be missed — this drives the clock forward until
    /// a tick is observed, giving the later exact-count assertions a synchronized starting point.
    /// </summary>
    private static async Task<int> SyncToLoopAsync(
        FakeTimeProvider timeProvider, ScriptedCache cache, TimeSpan interval)
    {
        var baseline = cache.RefreshCalls;
        var deadline = DateTime.UtcNow.AddSeconds(10);

        while (cache.RefreshCalls == baseline && DateTime.UtcNow < deadline)
        {
            timeProvider.Advance(interval);
            await Task.Delay(10);
        }

        Assert.True(cache.RefreshCalls > baseline, "the refresh loop never ticked");
        return cache.RefreshCalls;
    }

    /// <summary>
    /// Waits for the background loop to observe an advanced clock. Virtual time fires the timer
    /// synchronously but the continuation still runs on the scheduler.
    /// </summary>
    private static async Task WaitForRefreshCallsAsync(ScriptedCache cache, int expected)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (cache.RefreshCalls < expected && DateTime.UtcNow < deadline)
        {
            await Task.Delay(10);
        }

        Assert.Equal(expected, cache.RefreshCalls);
    }
}
