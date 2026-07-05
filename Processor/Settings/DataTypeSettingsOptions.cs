namespace Processor.Settings;

public class DataTypeSettingsOptions
{
    public const string SectionName = "DataTypeSettings";

    /// <summary>Redis hash key holding the settings (field = data type id, value = JSON of the setting).</summary>
    public string HashKey { get; set; } = "datatype:settings";

    /// <summary>How long the in-memory cache is served before the next query reloads it from Redis.</summary>
    public int RefreshSeconds { get; set; } = 15;

    /// <summary>Kafka header carrying the data type id; falls back to the message key when absent.</summary>
    public string HeaderName { get; set; } = "data-type-id";
}
