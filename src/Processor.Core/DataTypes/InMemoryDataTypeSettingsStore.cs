namespace Processor.Core.DataTypes;

/// <summary>
/// Settings store backed by an in-process list. Selected with <c>DataTypeSettings:Store: "inmemory"</c>
/// for local runs without an Oracle instance, and used by the mock end-to-end tests so they exercise
/// the real caching/refresh/fail-fast machinery without a database.
/// </summary>
public class InMemoryDataTypeSettingsStore : IDataTypeSettingsStore
{
    private readonly object _gate = new();
    private List<DataTypeSetting> _settings;

    public InMemoryDataTypeSettingsStore(IEnumerable<DataTypeSetting>? settings = null) =>
        _settings = settings?.ToList() ?? new List<DataTypeSetting>();

    /// <summary>When set, the next <see cref="LoadAllAsync"/> throws this — lets tests drive outages.</summary>
    public Exception? FailWith { get; set; }

    /// <summary>
    /// When &gt; 0, that many loads fail before the store recovers, each decrementing the counter.
    /// Makes a transient outage deterministic (no timing races) — used to verify startup retries.
    /// </summary>
    public int FailNextLoads { get; set; }

    /// <summary>Number of loads served, so tests can assert the message path never queries the store.</summary>
    public int LoadCount { get; private set; }

    /// <summary>Replaces the stored settings, simulating an out-of-band change to the data store.</summary>
    public void Replace(IEnumerable<DataTypeSetting> settings)
    {
        lock (_gate)
        {
            _settings = settings.ToList();
        }
    }

    public Task<IReadOnlyList<DataTypeSetting>> LoadAllAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            LoadCount++;

            if (FailNextLoads > 0)
            {
                FailNextLoads--;
                return Task.FromException<IReadOnlyList<DataTypeSetting>>(
                    FailWith ?? new InvalidOperationException("Simulated transient store failure"));
            }

            if (FailWith is not null)
            {
                return Task.FromException<IReadOnlyList<DataTypeSetting>>(FailWith);
            }

            return Task.FromResult<IReadOnlyList<DataTypeSetting>>(_settings.ToList());
        }
    }

    public Task ProbeAsync(CancellationToken cancellationToken = default) =>
        FailWith is null ? Task.CompletedTask : Task.FromException(FailWith);
}
