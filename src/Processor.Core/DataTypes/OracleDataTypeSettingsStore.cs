using System.Data;
using System.Diagnostics;
using System.Text.RegularExpressions;
using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Oracle.ManagedDataAccess.Client;
using Processor.Core.Application;
using Processor.Core.Diagnostics;

namespace Processor.Core.DataTypes;

/// <summary>
/// Oracle-backed settings store, read with Dapper. Rows are scoped to the domain this deployment
/// runs (<c>Processor:Domain</c>), so the same table serves every domain and a posts deployment can
/// never be switched on by a profiles row.
/// <para>
/// Expected schema:
/// <code>
/// CREATE TABLE DATA_TYPE_SETTINGS (
///   DOMAIN_NAME  VARCHAR2(64)  NOT NULL,
///   DATA_TYPE_ID VARCHAR2(128) NOT NULL,
///   IS_ACTIVE    NUMBER(1)     DEFAULT 0 NOT NULL,
///   CONSTRAINT PK_DATA_TYPE_SETTINGS PRIMARY KEY (DOMAIN_NAME, DATA_TYPE_ID)
/// )
/// </code>
/// </para>
/// </summary>
public class OracleDataTypeSettingsStore : IDataTypeSettingsStore
{
    /// <summary>
    /// A table name cannot be a bind parameter, so it is interpolated — and therefore restricted to a
    /// bare Oracle identifier, optionally schema-qualified. Anything else is a configuration error.
    /// </summary>
    private static readonly Regex TableNamePattern =
        new(@"^[A-Za-z][A-Za-z0-9_$#]{0,29}(\.[A-Za-z][A-Za-z0-9_$#]{0,29})?$", RegexOptions.Compiled);

    private readonly OracleOptions _options;
    private readonly string _domain;
    private readonly ILogger<OracleDataTypeSettingsStore> _logger;
    private readonly string _selectSql;

    public OracleDataTypeSettingsStore(
        IOptions<OracleOptions> options,
        IOptions<ProcessorOptions> processorOptions,
        ILogger<OracleDataTypeSettingsStore> logger)
    {
        _options = options.Value;
        _domain = processorOptions.Value.Domain;
        _logger = logger;

        if (string.IsNullOrWhiteSpace(_options.ConnectionString))
        {
            throw new InvalidOperationException(
                "Oracle:ConnectionString is not configured; the processor cannot load data-type settings.");
        }

        if (!TableNamePattern.IsMatch(_options.SettingsTable))
        {
            throw new InvalidOperationException(
                $"Oracle:SettingsTable '{_options.SettingsTable}' is not a valid Oracle table identifier.");
        }

        // Aliases are unquoted so Oracle upper-cases them; Dapper matches property names
        // case-insensitively, which is what lets DATA_TYPE_ID land on DataTypeId.
        _selectSql =
            $"SELECT DATA_TYPE_ID AS DataTypeId, IS_ACTIVE AS IsActive " +
            $"FROM {_options.SettingsTable} WHERE DOMAIN_NAME = :domain";
    }

    public async Task<IReadOnlyList<DataTypeSetting>> LoadAllAsync(CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            await using var connection = new OracleConnection(_options.ConnectionString);
            await connection.OpenAsync(cancellationToken);

            // Exactly one bind parameter: Oracle's default positional binding is unambiguous here,
            // so this does not depend on OracleCommand.BindByName.
            var command = new CommandDefinition(
                _selectSql,
                new { domain = _domain },
                commandTimeout: _options.CommandTimeoutSeconds,
                cancellationToken: cancellationToken);

            var rows = await connection.QueryAsync<SettingRow>(command);

            var settings = rows
                .Select(row => new DataTypeSetting
                {
                    DataTypeId = row.DataTypeId ?? string.Empty,
                    // NUMBER(1) arrives as a decimal; anything non-zero is active.
                    IsActive = row.IsActive != 0
                })
                .ToList();

            stopwatch.Stop();
            ProcessorMetrics.RecordDataStoreOperation("load_all", "ok", stopwatch.Elapsed.TotalMilliseconds);
            _logger.LogDebug(
                "Loaded {Count} data-type settings for domain '{Domain}' in {Elapsed:F1} ms",
                settings.Count, _domain, stopwatch.Elapsed.TotalMilliseconds);

            return settings;
        }
        catch (Exception)
        {
            stopwatch.Stop();
            ProcessorMetrics.RecordDataStoreOperation("load_all", "error", stopwatch.Elapsed.TotalMilliseconds);
            throw;
        }
    }

    public async Task ProbeAsync(CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        try
        {
            await using var connection = new OracleConnection(_options.ConnectionString);
            await connection.OpenAsync(cancellationToken);
            await connection.ExecuteScalarAsync<decimal>(new CommandDefinition(
                "SELECT 1 FROM DUAL",
                commandTimeout: _options.CommandTimeoutSeconds,
                cancellationToken: cancellationToken));

            stopwatch.Stop();
            ProcessorMetrics.RecordDataStoreOperation("probe", "ok", stopwatch.Elapsed.TotalMilliseconds);
        }
        catch (Exception)
        {
            stopwatch.Stop();
            ProcessorMetrics.RecordDataStoreOperation("probe", "error", stopwatch.Elapsed.TotalMilliseconds);
            throw;
        }
    }

    /// <summary>Row shape returned by Dapper; <c>IsActive</c> is a NUMBER(1), not a boolean.</summary>
    private sealed class SettingRow
    {
        public string? DataTypeId { get; set; }

        public decimal IsActive { get; set; }
    }
}
