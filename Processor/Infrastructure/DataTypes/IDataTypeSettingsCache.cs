namespace Processor.Infrastructure.DataTypes;

/// <summary>
/// Infrastructure concern: the lifecycle of the in-memory settings snapshot (initial load + refresh
/// + load state). Deliberately separate from the domain <see cref="IDataTypeSettingsRepository"/>
/// (which only exposes FindBy/FindAll) so caching/refresh never leaks into the domain. Consumed by
/// the refresh background service and the health checks — not by domain code.
/// </summary>
public interface IDataTypeSettingsCache
{
    /// <summary>Performs the first load. Throws if it fails so the host refuses to start.</summary>
    Task InitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>Reloads the snapshot; keeps the stale snapshot and returns false on failure.</summary>
    Task<bool> RefreshAsync(CancellationToken cancellationToken = default);

    /// <summary>True once at least one successful load has populated the snapshot.</summary>
    bool IsLoaded { get; }

    /// <summary>UTC time of the last successful load, or null if never loaded.</summary>
    DateTimeOffset? LastSuccessfulLoadUtc { get; }

    /// <summary>Number of settings currently in the snapshot.</summary>
    int Count { get; }
}
