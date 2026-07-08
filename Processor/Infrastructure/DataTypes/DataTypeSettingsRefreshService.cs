using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Processor.Application;
using StackExchange.Redis;

namespace Processor.Infrastructure.DataTypes;

/// <summary>
/// Owns the data-type settings snapshot lifecycle. Registered before the KafkaFlow hosted service so
/// it runs first:
/// <list type="bullet">
/// <item>at startup it performs the initial load (cached mode) or a Redis connectivity check (uncached
/// mode); either failing throws from <see cref="StartAsync"/>, so the host fails to start and k8s
/// restarts the pod;</item>
/// <item>while running (cached mode) it refreshes the snapshot every <c>RefreshSeconds</c>; a failed
/// refresh keeps the stale snapshot (logged + counted) rather than clearing it. The message path only
/// reads the snapshot, so a Redis outage never produces per-message Redis calls.</item>
/// </list>
/// </summary>
public class DataTypeSettingsRefreshService : BackgroundService
{
    private readonly IDataTypeSettingsCache _cache;
    private readonly IConnectionMultiplexer _redis;
    private readonly DataTypeSettingsOptions _options;
    private readonly ILogger<DataTypeSettingsRefreshService> _logger;

    public DataTypeSettingsRefreshService(
        IDataTypeSettingsCache cache,
        IConnectionMultiplexer redis,
        IOptions<DataTypeSettingsOptions> options,
        ILogger<DataTypeSettingsRefreshService> logger)
    {
        _cache = cache;
        _redis = redis;
        _options = options.Value;
        _logger = logger;
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        if (_options.UseCache)
        {
            // Throws if Redis is down / the load fails -> host fails to start -> pod restarts.
            await _cache.InitializeAsync(cancellationToken);
        }
        else
        {
            await EnsureRedisReachableAsync(cancellationToken);
        }

        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_options.UseCache)
        {
            return; // uncached mode reads Redis per message; nothing to refresh in the background
        }

        var interval = TimeSpan.FromSeconds(Math.Max(1, _options.RefreshSeconds));
        _logger.LogInformation("Data-type settings refresh loop starting (every {Interval})", interval);

        using var timer = new PeriodicTimer(interval);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                await _cache.RefreshAsync(stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown.
        }
    }

    private async Task EnsureRedisReachableAsync(CancellationToken cancellationToken)
    {
        try
        {
            await _redis.GetDatabase().PingAsync();
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException(
                "Redis is not reachable at startup; refusing to start so the pod restarts.", ex);
        }
    }
}
