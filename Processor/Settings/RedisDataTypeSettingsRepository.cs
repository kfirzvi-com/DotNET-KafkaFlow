using System.Diagnostics;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Processor.Diagnostics;
using StackExchange.Redis;

namespace Processor.Settings;

/// <summary>
/// Redis-backed <see cref="IDataTypeSettingsRepository"/>. Behaviour depends on
/// <see cref="DataTypeSettingsOptions.UseCache"/>:
/// <list type="bullet">
/// <item>cached (default): reads are served from an in-memory snapshot of the Redis hash, reloaded
/// only after the TTL (<see cref="DataTypeSettingsOptions.RefreshSeconds"/>) has passed, so the hot
/// path avoids Redis on every message;</item>
/// <item>uncached: every lookup hits Redis directly (HGET/HGETALL) — used to compare the two.</item>
/// </list>
/// Hash values may be JSON (<c>{ "dataTypeId": "...", "isActive": true }</c>) or a bare "true"/"false".
/// Every Redis round-trip is timed via <see cref="ProcessorMetrics.RecordRedisOperation"/>.
/// </summary>
public class RedisDataTypeSettingsRepository : IDataTypeSettingsRepository
{
    private static readonly JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web);

    private readonly IConnectionMultiplexer _redis;
    private readonly DataTypeSettingsOptions _options;
    private readonly ILogger<RedisDataTypeSettingsRepository> _logger;
    private readonly long _ttlMs;
    private readonly SemaphoreSlim _reloadLock = new(1, 1);

    // Volatile reference swap gives lock-free reads; a reader always sees a fully-built snapshot.
    private volatile IReadOnlyDictionary<string, DataTypeSetting> _cache =
        new Dictionary<string, DataTypeSetting>(StringComparer.OrdinalIgnoreCase);

    // Monotonic timestamp (ms) of the last load attempt. long.MinValue => never loaded.
    private long _lastLoadedTicks = long.MinValue;

    public RedisDataTypeSettingsRepository(
        IConnectionMultiplexer redis,
        IOptions<DataTypeSettingsOptions> options,
        ILogger<RedisDataTypeSettingsRepository> logger)
    {
        _redis = redis;
        _options = options.Value;
        _logger = logger;
        _ttlMs = (long)Math.Max(0, _options.RefreshSeconds) * 1000;
    }

    public async Task<IReadOnlyList<DataTypeSetting>> FindAllAsync(CancellationToken cancellationToken = default)
    {
        if (!_options.UseCache)
        {
            return (await LoadAllFromRedisAsync()).Values.ToList();
        }

        await EnsureFreshAsync(cancellationToken);
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

        await EnsureFreshAsync(cancellationToken);
        return _cache.TryGetValue(dataTypeId, out var found) ? found : null;
    }

    private bool IsStale()
    {
        var last = Volatile.Read(ref _lastLoadedTicks);
        return last == long.MinValue || Environment.TickCount64 - last >= _ttlMs;
    }

    private async Task EnsureFreshAsync(CancellationToken cancellationToken)
    {
        if (!IsStale())
        {
            return;
        }

        await _reloadLock.WaitAsync(cancellationToken);
        try
        {
            // Another caller may have reloaded while we waited for the lock.
            if (!IsStale())
            {
                return;
            }

            var map = await LoadAllFromRedisAsync();
            if (map is not null)
            {
                _cache = map;
            }

            // Stamp the attempt (success or failure) so a failing Redis is retried at most once per TTL.
            Volatile.Write(ref _lastLoadedTicks, Environment.TickCount64);
        }
        finally
        {
            _reloadLock.Release();
        }
    }

    /// <summary>HGETALL the settings hash and parse it. Returns null on Redis failure (never throws).</summary>
    private async Task<Dictionary<string, DataTypeSetting>?> LoadAllFromRedisAsync()
    {
        var stopwatch = Stopwatch.StartNew();
        HashEntry[] entries;
        try
        {
            var db = _redis.GetDatabase();
            entries = await db.HashGetAllAsync(_options.HashKey);
            stopwatch.Stop();
            ProcessorMetrics.RecordRedisOperation("hash_get_all", "ok", stopwatch.Elapsed.TotalMilliseconds);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            ProcessorMetrics.RecordRedisOperation("hash_get_all", "error", stopwatch.Elapsed.TotalMilliseconds);
            _logger.LogError(ex, "Failed to load data-type settings; keeping previous snapshot of {Count} entries", _cache.Count);
            return null;
        }

        var map = new Dictionary<string, DataTypeSetting>(StringComparer.OrdinalIgnoreCase);
        foreach (var entry in entries)
        {
            var id = entry.Name.ToString();
            var raw = entry.Value.ToString();

            if (TryParse(id, raw, out var setting))
            {
                map[setting.DataTypeId] = setting;
            }
            else
            {
                _logger.LogWarning("Skipping unparseable data-type setting for id '{DataTypeId}': {Raw}", id, raw);
            }
        }

        return map;
    }

    /// <summary>HGET a single field. Returns null when absent or on Redis failure (never throws).</summary>
    private async Task<DataTypeSetting?> FetchOneFromRedisAsync(string dataTypeId)
    {
        var stopwatch = Stopwatch.StartNew();
        RedisValue raw;
        try
        {
            var db = _redis.GetDatabase();
            raw = await db.HashGetAsync(_options.HashKey, dataTypeId);
            stopwatch.Stop();
            ProcessorMetrics.RecordRedisOperation("hash_get", "ok", stopwatch.Elapsed.TotalMilliseconds);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            ProcessorMetrics.RecordRedisOperation("hash_get", "error", stopwatch.Elapsed.TotalMilliseconds);
            _logger.LogError(ex, "Failed to fetch data-type setting for '{DataTypeId}'", dataTypeId);
            return null;
        }

        if (raw.IsNullOrEmpty || !TryParse(dataTypeId, raw.ToString(), out var setting))
        {
            return null;
        }

        return setting;
    }

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
