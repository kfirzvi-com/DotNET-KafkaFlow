using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Processor.Settings;

namespace Processor.Health;

/// <summary>
/// Readiness check for the data-type settings snapshot (cached mode). Unhealthy if it never loaded;
/// Degraded (still ready, but flagged) if the last successful load is older than the staleness
/// threshold — i.e. refreshes have been failing while a stale snapshot is served.
/// </summary>
public class SettingsHealthCheck : IHealthCheck
{
    private readonly IDataTypeSettingsCache _cache;
    private readonly DataTypeSettingsOptions _options;

    public SettingsHealthCheck(
        IDataTypeSettingsCache cache, IOptions<DataTypeSettingsOptions> options)
    {
        _cache = cache;
        _options = options.Value;
    }

    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (!_options.UseCache)
        {
            return Task.FromResult(HealthCheckResult.Healthy("Uncached mode: settings read from Redis per request"));
        }

        if (!_cache.IsLoaded || _cache.LastSuccessfulLoadUtc is null)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy("Data-type settings have never been loaded"));
        }

        var age = DateTimeOffset.UtcNow - _cache.LastSuccessfulLoadUtc.Value;
        var stalenessThreshold = TimeSpan.FromSeconds(Math.Max(1, _options.RefreshSeconds) * 3);

        if (age > stalenessThreshold)
        {
            return Task.FromResult(HealthCheckResult.Degraded(
                $"Serving a stale snapshot: last successful load {age.TotalSeconds:F0}s ago ({_cache.Count} entries)"));
        }

        return Task.FromResult(HealthCheckResult.Healthy(
            $"{_cache.Count} settings, loaded {age.TotalSeconds:F0}s ago"));
    }
}
