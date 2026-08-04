using Microsoft.Extensions.Logging;
using Processor.Core.Diagnostics;

namespace Processor.Core.DataTypes;

/// <summary>
/// Serves data-type settings from an in-memory snapshot over any <see cref="IDataTypeSettingsStore"/>.
/// The message path only ever reads the snapshot, so a store outage produces zero per-message queries
/// regardless of throughput; <c>DataTypeSettingsRefreshService</c> owns reloads.
/// <para>
/// Store-agnostic on purpose: the same caching behaviour applies to Oracle in production and to the
/// in-memory store used by the mock end-to-end tests.
/// </para>
/// </summary>
public class CachingDataTypeSettingsRepository : IDataTypeSettingsRepository, IDataTypeSettingsCache
{
    private readonly IDataTypeSettingsStore _store;
    private readonly ILogger<CachingDataTypeSettingsRepository> _logger;

    // Volatile reference swap gives lock-free reads; a reader always sees a fully-built snapshot.
    // No lock is needed on writes: RefreshAsync is only ever called by the single background refresh
    // service, serially (the initial load completes before the periodic loop starts).
    private volatile IReadOnlyDictionary<string, DataTypeSetting> _snapshot =
        new Dictionary<string, DataTypeSetting>(StringComparer.OrdinalIgnoreCase);

    private volatile bool _loaded;
    private long _lastLoadTicksUtc; // 0 = never; read off-thread by the health check

    public CachingDataTypeSettingsRepository(
        IDataTypeSettingsStore store,
        ILogger<CachingDataTypeSettingsRepository> logger)
    {
        _store = store;
        _logger = logger;
    }

    public bool IsLoaded => _loaded;

    public DateTimeOffset? LastSuccessfulLoadUtc
    {
        get
        {
            var ticks = Interlocked.Read(ref _lastLoadTicksUtc);
            return ticks == 0 ? null : new DateTimeOffset(ticks, TimeSpan.Zero);
        }
    }

    public int Count => _snapshot.Count;

    public Task<IReadOnlyList<DataTypeSetting>> FindAllAsync(CancellationToken cancellationToken = default) =>
        Task.FromResult<IReadOnlyList<DataTypeSetting>>(_snapshot.Values.ToList());

    public Task<DataTypeSetting?> FindByIdAsync(string dataTypeId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(dataTypeId))
        {
            return Task.FromResult<DataTypeSetting?>(null);
        }

        return Task.FromResult(_snapshot.TryGetValue(dataTypeId, out var found) ? found : null);
    }

    /// <summary>
    /// Reloads the snapshot from the store. On success swaps the snapshot and records success. On
    /// failure keeps the previous snapshot, logs a warning and increments the failure metric — a
    /// data type must never be treated as inactive just because a reload failed. Never throws.
    /// </summary>
    public async Task<bool> RefreshAsync(CancellationToken cancellationToken = default)
    {
        IReadOnlyList<DataTypeSetting> settings;
        try
        {
            settings = await _store.LoadAllAsync(cancellationToken);
        }
        catch (Exception ex)
        {
            ProcessorMetrics.RecordSettingsLoadFailure();
            _logger.LogWarning(ex,
                "Data-type settings load failed; keeping {State} snapshot of {Count} entries",
                _loaded ? "previous" : "empty", _snapshot.Count);
            return false;
        }

        var map = new Dictionary<string, DataTypeSetting>(StringComparer.OrdinalIgnoreCase);
        foreach (var setting in settings)
        {
            if (string.IsNullOrWhiteSpace(setting.DataTypeId))
            {
                _logger.LogWarning("Skipping data-type setting with an empty id");
                continue;
            }

            map[setting.DataTypeId] = setting;
        }

        _snapshot = map;
        _loaded = true;
        Interlocked.Exchange(ref _lastLoadTicksUtc, DateTime.UtcNow.Ticks);
        ProcessorMetrics.RecordSettingsLoadSuccess(map.Count);
        _logger.LogInformation(
            "Data-type settings loaded: {Count} entries ({ActiveCount} active)",
            map.Count, map.Values.Count(s => s.IsActive));
        return true;
    }
}
