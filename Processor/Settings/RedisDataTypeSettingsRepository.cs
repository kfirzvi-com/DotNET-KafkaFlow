using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using StackExchange.Redis;

namespace Processor.Settings;

/// <summary>
/// Redis-backed <see cref="IDataTypeSettingsRepository"/> with a self-refreshing in-memory cache.
/// Reads are served from a cached snapshot of a Redis hash (field = data type id, value = JSON of
/// the setting, or a bare "true"/"false"). Each query checks how long ago the cache was loaded and
/// reloads from Redis only once the TTL (<see cref="DataTypeSettingsOptions.RefreshSeconds"/>) has
/// passed, so the hot path avoids Redis on every message.
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
        await EnsureFreshAsync(cancellationToken);
        return _cache.Values.ToList();
    }

    public async Task<DataTypeSetting?> FindByIdAsync(string dataTypeId, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(dataTypeId))
        {
            return null;
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

            await ReloadAsync();
        }
        finally
        {
            _reloadLock.Release();
        }
    }

    /// <summary>Reloads the cache from Redis. Never throws: on failure the previous snapshot is kept.</summary>
    private async Task ReloadAsync()
    {
        try
        {
            var db = _redis.GetDatabase();
            var entries = await db.HashGetAllAsync(_options.HashKey);

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

            _cache = map;
            _logger.LogInformation(
                "Data-type settings cache reloaded: {Count} entries ({ActiveCount} active)",
                map.Count, map.Values.Count(s => s.IsActive));
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to reload data-type settings; keeping previous snapshot of {Count} entries", _cache.Count);
        }
        finally
        {
            // Stamp the attempt (success or failure) so a failing Redis is retried at most once per TTL,
            // not on every message.
            Volatile.Write(ref _lastLoadedTicks, Environment.TickCount64);
        }
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
