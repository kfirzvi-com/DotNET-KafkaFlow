namespace Processor.Core.DataTypes;

/// <summary>
/// The backing store for data-type settings — the only place that knows about Oracle (or, in tests,
/// an in-memory list). Deliberately narrow: the caching repository asks for the full set for the
/// domain and nothing else, so the message path never issues a per-message query.
/// </summary>
public interface IDataTypeSettingsStore
{
    /// <summary>
    /// Loads every setting for the domain this deployment runs. Throws on store failure; the caller
    /// decides whether that is fatal (startup) or tolerable (periodic refresh).
    /// </summary>
    Task<IReadOnlyList<DataTypeSetting>> LoadAllAsync(CancellationToken cancellationToken = default);

    /// <summary>Cheap connectivity probe for the readiness health check. Throws when unreachable.</summary>
    Task ProbeAsync(CancellationToken cancellationToken = default);
}
