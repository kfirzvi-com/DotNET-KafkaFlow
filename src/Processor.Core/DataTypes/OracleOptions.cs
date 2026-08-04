namespace Processor.Core.DataTypes;

/// <summary>Connection and schema details for the Oracle data-type settings store.</summary>
public class OracleOptions
{
    public const string SectionName = "Oracle";

    public string ConnectionString { get; set; } = string.Empty;

    /// <summary>
    /// Table holding the settings. Interpolated into the SQL (it cannot be a bind parameter), so it
    /// is validated as a plain identifier before use — see <c>OracleDataTypeSettingsStore</c>.
    /// </summary>
    public string SettingsTable { get; set; } = "DATA_TYPE_SETTINGS";

    public int CommandTimeoutSeconds { get; set; } = 15;
}
