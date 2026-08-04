using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Processor.Core.Application;

namespace Processor.Core.DataTypes;

/// <summary>
/// Owns the data-type settings snapshot lifecycle. Registered before the KafkaFlow hosted service so
/// it runs first:
/// <list type="bullet">
/// <item>at startup it attempts the initial load up to <c>DataTypeSettings:StartupAttempts</c> times
/// (default 3, spaced by <c>StartupRetryDelaySeconds</c>). If every attempt fails,
/// <see cref="StartAsync"/> throws — the host fails to start, exits non-zero, and the orchestrator
/// restarts the pod. The processor never consumes without settings;</item>
/// <item>while running it refreshes the snapshot every <c>RefreshMinutes</c> (default 10). A failed
/// refresh logs a warning and keeps the stale snapshot rather than clearing it or crashing — the next
/// tick simply tries again. The message path only reads the snapshot, so a store outage never
/// produces per-message queries.</item>
/// </list>
/// </summary>
public class DataTypeSettingsRefreshService : BackgroundService
{
    private readonly IDataTypeSettingsCache _cache;
    private readonly DataTypeSettingsOptions _options;
    private readonly ILogger<DataTypeSettingsRefreshService> _logger;
    private readonly TimeProvider _timeProvider;

    public DataTypeSettingsRefreshService(
        IDataTypeSettingsCache cache,
        IOptions<DataTypeSettingsOptions> options,
        ILogger<DataTypeSettingsRefreshService> logger,
        TimeProvider? timeProvider = null)
    {
        _cache = cache;
        _options = options.Value;
        _logger = logger;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        await LoadWithRetriesAsync(cancellationToken);
        await base.StartAsync(cancellationToken);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var interval = TimeSpan.FromMinutes(Math.Max(1, _options.RefreshMinutes));
        _logger.LogInformation("Data-type settings refresh loop starting (every {Interval})", interval);

        using var timer = new PeriodicTimer(interval, _timeProvider);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                // Never throws: a failed refresh keeps the current snapshot and logs a warning.
                await _cache.RefreshAsync(stoppingToken);
            }
        }
        catch (OperationCanceledException)
        {
            // Normal shutdown.
        }
    }

    /// <summary>
    /// Attempts the initial load up to <c>StartupAttempts</c> times. Throws when all attempts fail so
    /// the host refuses to start.
    /// </summary>
    private async Task LoadWithRetriesAsync(CancellationToken cancellationToken)
    {
        var attempts = Math.Max(1, _options.StartupAttempts);
        var delay = TimeSpan.FromSeconds(Math.Max(0, _options.StartupRetryDelaySeconds));

        for (var attempt = 1; attempt <= attempts; attempt++)
        {
            if (await _cache.RefreshAsync(cancellationToken))
            {
                _logger.LogInformation(
                    "Initial data-type settings load succeeded on attempt {Attempt}/{Attempts}",
                    attempt, attempts);
                return;
            }

            if (attempt < attempts)
            {
                _logger.LogWarning(
                    "Initial data-type settings load failed (attempt {Attempt}/{Attempts}); retrying in {Delay}",
                    attempt, attempts, delay);

                if (delay > TimeSpan.Zero)
                {
                    await Task.Delay(delay, _timeProvider, cancellationToken);
                }
            }
        }

        throw new InvalidOperationException(
            $"Initial load of data-type settings failed after {attempts} attempt(s); refusing to start. " +
            "Check data store connectivity and the settings table.");
    }
}
