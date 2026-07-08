using System.Diagnostics;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Processor.Application;
using Processor.Diagnostics;
using Processor.Domain.DataTypes;
using StackExchange.Redis;

namespace Processor.Infrastructure.DataTypes;

/// <summary>
/// Redis-backed <see cref="IDataTypeSettingsRepository"/>. Each setting is stored under its own
/// String key <c>{KeyPrefix}{dataTypeId}</c> holding a JSON <see cref="DataTypeSetting"/>
/// (a bare "true"/"false" is also accepted). Behaviour depends on
/// <see cref="DataTypeSettingsOptions.UseCache"/>:
/// <list type="bullet">
/// <item>cached (default): reads are served purely from an in-memory snapshot. The snapshot is loaded
/// once at startup (<see cref="InitializeAsync"/>, which throws so the app crashes if Redis is down)
/// and refreshed on a schedule by <c>DataTypeSettingsRefreshService</c> via <see cref="RefreshAsync"/>.
/// A failed refresh keeps the previous snapshot (logged + counted), so the message path never touches
/// Redis and never hammers it during an outage;</item>
/// <item>uncached: every lookup issues a Redis GET — used only for the cache-vs-lookup experiment.</item>
/// </list>
/// </summary>
public class RedisDataTypeSettingsRepository : IDataTypeSettingsRepository, IDataTypeSettingsCache
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly IConnectionMultiplexer _redis;
    private readonly DataTypeSettingsOptions _options;
    private readonly ILogger<RedisDataTypeSettingsRepository> _logger;

    // Volatile reference swap gives lock-free reads; a reader always sees a fully-built snapshot.
    // No lock is needed on writes: RefreshAsync is only ever called by the single background
    // refresh service, serially (initial load completes before the periodic loop starts).
    private volatile IReadOnlyDictionary<string, DataTypeSetting> _cache =
        new Dictionary<string, DataTypeSetting>(StringComparer.OrdinalIgnoreCase);

    private volatile bool _loaded;
    private long _lastLoadTicksUtc; // 0 = never; read off-thread by the health check

    public RedisDataTypeSettingsRepository(
        IConnectionMultiplexer redis,
        IOptions<DataTypeSettingsOptions> options,
        ILogger<RedisDataTypeSettingsRepository> logger)
    {
        _redis = redis;
        _options = options.Value;
        _logger = logger;
    }

    /// <summary>True once at least one successful load has populated the snapshot.</summary>
    public bool IsLoaded => _loaded;

    /// <summary>UTC time of the last successful load (for health reporting), or null if never.</summary>
    public DateTimeOffset? LastSuccessfulLoadUtc
    {
        get
        {
            var ticks = Interlocked.Read(ref _lastLoadTicksUtc);
            return ticks == 0 ? null : new DateTimeOffset(ticks, TimeSpan.Zero);
        }
    }

    /// <summary>Number of settings currently in the snapshot.</summary>
    public int Count => _cache.Count;

    public async Task<IReadOnlyList<DataTypeSetting>> FindAllAsync(CancellationToken cancellationToken = default)
    {
        if (!_options.UseCache)
        {
            var map = await LoadAllFromRedisAsync();
            return map is null ? Array.Empty<DataTypeSetting>() : map.Values.ToList();
        }

        return _cache.Values.ToList();
    }

    public async Task<DataTypeSetting?> FindByIdAsync(string dataTypeId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(dataTypeId))
        {
            return null;
        }

        if (!_options.UseCache)
        {
            return await FetchOneFromRedisAsync(dataTypeId);
        }

        // Cached path: snapshot only, never touches Redis (the refresh service owns reloads).
        return _cache.TryGetValue(dataTypeId, out var found) ? found : null;
    }

    /// <summary>
    /// Loads the snapshot for the first time. Throws if the load fails so the host fails to start
    /// (k8s then restarts the pod) — we must never serve traffic having never loaded settings.
    /// </summary>
    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        var ok = await RefreshAsync(cancellationToken);
        if (!ok)
        {
            throw new InvalidOperationException(
                "Initial load of data-type settings from Redis failed; refusing to start. " +
                "Check Redis connectivity at " + string.Join(",", _redis.GetEndPoints().Select(e => e.ToString())) + ".");
        }
    }

    /// <summary>
    /// Reloads the snapshot from Redis. On success swaps the snapshot and records success. On failure
    /// keeps the previous snapshot, logs a warning and increments the failure metric. Never throws.
    /// Returns true on success.
    /// </summary>
    public async Task<bool> RefreshAsync(CancellationToken cancellationToken = default)
    {
        var map = await LoadAllFromRedisAsync();
        if (map is null)
        {
            ProcessorMetrics.RecordSettingsLoadFailure();
            _logger.LogWarning(
                "Data-type settings load from Redis failed; keeping {State} snapshot of {Count} entries",
                _loaded ? "previous" : "empty", _cache.Count);
            return false;
        }

        _cache = map;
        _loaded = true;
        Interlocked.Exchange(ref _lastLoadTicksUtc, DateTime.UtcNow.Ticks);
        ProcessorMetrics.RecordSettingsLoadSuccess(map.Count);
        _logger.LogInformation(
            "Data-type settings loaded: {Count} entries ({ActiveCount} active)",
            map.Count, map.Values.Count(s => s.IsActive));
        return true;
    }

    /// <summary>SCAN the key prefix and MGET all settings. Returns null on Redis failure (never throws).</summary>
    private async Task<Dictionary<string, DataTypeSetting>?> LoadAllFromRedisAsync()
    {
        var stopwatch = Stopwatch.StartNew();
        RedisKey[] keys;
        RedisValue[] values;
        try
        {
            var db = _redis.GetDatabase();
            var server = _redis.GetServer(_redis.GetEndPoints()[0]);
            keys = server.Keys(pattern: _options.KeyPrefix + "*", pageSize: 1000).ToArray();
            values = keys.Length == 0 ? Array.Empty<RedisValue>() : await db.StringGetAsync(keys);
            stopwatch.Stop();
            ProcessorMetrics.RecordRedisOperation("load_all", "ok", stopwatch.Elapsed.TotalMilliseconds);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            ProcessorMetrics.RecordRedisOperation("load_all", "error", stopwatch.Elapsed.TotalMilliseconds);
            _logger.LogError(ex, "Redis error while loading data-type settings");
            return null;
        }

        var map = new Dictionary<string, DataTypeSetting>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < keys.Length; i++)
        {
            var id = IdFromKey(keys[i].ToString());
            var raw = values[i].ToString();

            if (TryParse(id, raw, out var setting))
            {
                map[setting.DataTypeId] = setting;
            }
            else
            {
                _logger.LogWarning("Skipping unparseable data-type setting for key '{Key}': {Raw}", keys[i], raw);
            }
        }

        return map;
    }

    /// <summary>GET a single setting key. Returns null when absent or on Redis failure (never throws).</summary>
    private async Task<DataTypeSetting?> FetchOneFromRedisAsync(string dataTypeId)
    {
        var stopwatch = Stopwatch.StartNew();
        RedisValue raw;
        try
        {
            var db = _redis.GetDatabase();
            raw = await db.StringGetAsync(_options.KeyPrefix + dataTypeId);
            stopwatch.Stop();
            ProcessorMetrics.RecordRedisOperation("get", "ok", stopwatch.Elapsed.TotalMilliseconds);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            ProcessorMetrics.RecordRedisOperation("get", "error", stopwatch.Elapsed.TotalMilliseconds);
            _logger.LogError(ex, "Failed to fetch data-type setting for '{DataTypeId}'", dataTypeId);
            return null;
        }

        if (raw.IsNullOrEmpty || !TryParse(dataTypeId, raw.ToString(), out var setting))
        {
            return null;
        }

        return setting;
    }

    private string IdFromKey(string key) =>
        key.StartsWith(_options.KeyPrefix, StringComparison.Ordinal) ? key[_options.KeyPrefix.Length..] : key;

    private static bool TryParse(string id, string raw, out DataTypeSetting setting)
    {
        setting = new DataTypeSetting { DataTypeId = id };

        if (string.IsNullOrWhiteSpace(raw))
        {
            return false;
        }

        // Convenience: bare boolean value ("true"/"false").
        if (bool.TryParse(raw, out var flag))
        {
            setting.IsActive = flag;
            return true;
        }

        // JSON object: { "dataTypeId": "...", "isActive": true }
        try
        {
            var parsed = JsonSerializer.Deserialize<DataTypeSetting>(raw, JsonOptions);
            if (parsed is null)
            {
                return false;
            }

            setting.DataTypeId = string.IsNullOrWhiteSpace(parsed.DataTypeId) ? id : parsed.DataTypeId;
            setting.IsActive = parsed.IsActive;
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }
}
