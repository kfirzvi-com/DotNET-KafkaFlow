using Microsoft.Extensions.Diagnostics.HealthChecks;
using Processor.Core.DataTypes;

namespace Processor.Core.Health;

/// <summary>Readiness check: the settings data store must answer a probe query.</summary>
public class DataStoreHealthCheck : IHealthCheck
{
    private readonly IDataTypeSettingsStore _store;

    public DataStoreHealthCheck(IDataTypeSettingsStore store) => _store = store;

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            await _store.ProbeAsync(cancellationToken);
            return HealthCheckResult.Healthy("Data store reachable");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Data store unreachable", ex);
        }
    }
}
