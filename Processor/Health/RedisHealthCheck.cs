using Microsoft.Extensions.Diagnostics.HealthChecks;
using StackExchange.Redis;

namespace Processor.Health;

/// <summary>Readiness check: Redis must answer a PING.</summary>
public class RedisHealthCheck : IHealthCheck
{
    private readonly IConnectionMultiplexer _redis;

    public RedisHealthCheck(IConnectionMultiplexer redis) => _redis = redis;

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            var latency = await _redis.GetDatabase().PingAsync();
            return HealthCheckResult.Healthy($"Redis reachable ({latency.TotalMilliseconds:F1} ms)");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Redis unreachable", ex);
        }
    }
}
