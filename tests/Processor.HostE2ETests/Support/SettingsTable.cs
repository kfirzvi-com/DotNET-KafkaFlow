using Dapper;
using Oracle.ManagedDataAccess.Client;

namespace Processor.HostE2ETests.Support;

/// <summary>
/// A settings table in Oracle owned by a single test. The processor's table is configurable
/// (<c>Oracle:SettingsTable</c>), so giving each test its own is what makes cases genuinely
/// independent: a case's rows cannot be seen, overwritten, or deleted by another case, and the suite
/// stays correct if it is ever run in parallel.
/// <para>
/// The previous approach — one shared table cleared with <c>DELETE WHERE DOMAIN_NAME = :domain</c>
/// before each case — only worked because the collection runs serially.
/// </para>
/// </summary>
public sealed class SettingsTable
{
    private readonly string _connectionString;

    internal SettingsTable(string name, string connectionString)
    {
        Name = name;
        _connectionString = connectionString;
    }

    /// <summary>Table name to hand to <c>Oracle:SettingsTable</c>.</summary>
    public string Name { get; }

    /// <summary>
    /// Builds a unique, legal Oracle identifier. Oracle caps identifiers at 30 characters, and the
    /// processor additionally validates the configured name as a bare identifier, so the readable part
    /// is truncated and prefixed with a per-run sequence number.
    /// </summary>
    internal static string NameFor(int sequence, string discriminator)
    {
        var slug = new string(discriminator
            .Select(c => char.IsLetterOrDigit(c) ? char.ToUpperInvariant(c) : '_')
            .ToArray());

        // DTS_ (4) + 3-digit sequence + _ (1) leaves 22 for the readable part.
        if (slug.Length > 22)
        {
            slug = slug[..22];
        }

        return $"DTS_{sequence:D3}_{slug}";
    }

    /// <summary>Creates the table. Shape matches the production schema exactly.</summary>
    internal async Task CreateAsync()
    {
        await using var connection = new OracleConnection(_connectionString);
        await connection.OpenAsync();

        await connection.ExecuteAsync($"""
            CREATE TABLE {Name} (
              DOMAIN_NAME  VARCHAR2(64)  NOT NULL,
              DATA_TYPE_ID VARCHAR2(128) NOT NULL,
              IS_ACTIVE    NUMBER(1)     DEFAULT 0 NOT NULL,
              CONSTRAINT PK_{Name} PRIMARY KEY (DOMAIN_NAME, DATA_TYPE_ID),
              CONSTRAINT CK_{Name}_ACTIVE CHECK (IS_ACTIVE IN (0, 1))
            )
            """);
    }

    /// <summary>
    /// Inserts the rows a case declares. No delete first — the table belongs to this case and starts
    /// empty, so seeding is a plain insert and an empty list genuinely means "no settings at all".
    /// </summary>
    public async Task SeedAsync(string domain, params (string DataTypeId, bool IsActive)[] settings)
    {
        if (settings.Length == 0)
        {
            return;
        }

        await using var connection = new OracleConnection(_connectionString);
        await connection.OpenAsync();

        foreach (var (dataTypeId, isActive) in settings)
        {
            await connection.ExecuteAsync(
                $"INSERT INTO {Name} (DOMAIN_NAME, DATA_TYPE_ID, IS_ACTIVE) " +
                "VALUES (:domain, :dataTypeId, :isActive)",
                new { domain, dataTypeId, isActive = isActive ? 1 : 0 });
        }
    }

    /// <summary>Flips one row's active flag, to exercise a settings change made outside the app.</summary>
    public async Task SetActiveAsync(string domain, string dataTypeId, bool isActive)
    {
        await using var connection = new OracleConnection(_connectionString);
        await connection.OpenAsync();

        var affected = await connection.ExecuteAsync(
            $"UPDATE {Name} SET IS_ACTIVE = :isActive " +
            "WHERE DOMAIN_NAME = :domain AND DATA_TYPE_ID = :dataTypeId",
            new { isActive = isActive ? 1 : 0, domain, dataTypeId });

        if (affected == 0)
        {
            throw new InvalidOperationException(
                $"No row for domain '{domain}' / data type '{dataTypeId}' in {Name}; nothing was updated.");
        }
    }

    /// <summary>Rows currently in the table, for tests that assert on the database directly.</summary>
    public async Task<IReadOnlyList<(string DomainName, string DataTypeId, bool IsActive)>> ReadAllAsync()
    {
        await using var connection = new OracleConnection(_connectionString);
        await connection.OpenAsync();

        var rows = await connection.QueryAsync<(string, string, decimal)>(
            $"SELECT DOMAIN_NAME, DATA_TYPE_ID, IS_ACTIVE FROM {Name} ORDER BY DOMAIN_NAME, DATA_TYPE_ID");

        return rows.Select(r => (r.Item1, r.Item2, r.Item3 != 0)).ToList();
    }
}
