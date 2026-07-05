namespace Processor.Settings;

/// <summary>
/// Repository over data-type settings. Implementations decide how the data is stored and
/// whether it is cached; callers only deal with the query surface.
/// </summary>
public interface IDataTypeSettingsRepository
{
    /// <summary>Returns all known data-type settings.</summary>
    Task<IReadOnlyList<DataTypeSetting>> FindAllAsync(CancellationToken cancellationToken = default);

    /// <summary>Returns the setting for the given data type id, or null when none exists.</summary>
    Task<DataTypeSetting?> FindByIdAsync(string dataTypeId, CancellationToken cancellationToken = default);
}
