using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Dapper;
using DotNet.Testcontainers.Builders;
using Oracle.ManagedDataAccess.Client;
using Testcontainers.Kafka;
using Testcontainers.Oracle;

namespace Processor.HostE2ETests.Support;

/// <summary>
/// Real infrastructure for the end-to-end suite: a Kafka broker and an Oracle database, both in
/// containers, shared by every test in the collection (Oracle takes minutes to boot, so starting it per
/// test is not viable).
/// <para>
/// Oracle is set up the way the deployment expects it: a schema with the settings table, populated with
/// per-domain rows.
/// </para>
/// </summary>
public sealed class InfrastructureFixture : IAsyncLifetime
{
    /// <summary>The settings table, matching <c>Oracle:SettingsTable</c>.</summary>
    public const string SettingsTable = "DATA_TYPE_SETTINGS";

    private readonly KafkaContainer _kafka = new KafkaBuilder()
        .WithImage("confluentinc/cp-kafka:7.8.0")
        .Build();

    /// <summary>Application schema the processor connects as — mirrors the compose setup.</summary>
    private const string AppUser = "processor";

    private const string AppPassword = "processor";

    /// <summary>
    /// Pluggable-database service name of the <c>oracle-free</c> image. Testcontainers' built-in
    /// connection string assumes the older XE image's <c>XEPDB1</c>, so it cannot be used as-is here.
    /// </summary>
    private const string ServiceName = "FREEPDB1";

    private readonly OracleContainer _oracle = new OracleBuilder()
        .WithImage("gvenzl/oracle-free:23.5-slim-faststart")
        .WithEnvironment("ORACLE_PASSWORD", "oracle")
        .WithEnvironment("APP_USER", AppUser)
        .WithEnvironment("APP_USER_PASSWORD", AppPassword)
        .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("DATABASE IS READY TO USE!"))
        .Build();

    public string BootstrapServers => _kafka.GetBootstrapAddress();

    public string OracleConnectionString => BuildOracleConnectionString();

    public async Task InitializeAsync()
    {
        // Both are slow and independent, so start them together.
        await Task.WhenAll(_kafka.StartAsync(), _oracle.StartAsync());
        await CreateSettingsTableAsync();
    }

    public async Task DisposeAsync()
    {
        await Task.WhenAll(_kafka.DisposeAsync().AsTask(), _oracle.DisposeAsync().AsTask());
    }

    /// <summary>
    /// Built by hand rather than taken from <c>GetConnectionString()</c>, which hard-codes the XE
    /// image's <c>XEPDB1</c> service. Otherwise identical to what a real deployment would configure.
    /// </summary>
    private string BuildOracleConnectionString() =>
        $"User Id={AppUser};Password={AppPassword};" +
        $"Data Source={_oracle.Hostname}:{_oracle.GetMappedPublicPort(1521)}/{ServiceName}";

    private async Task CreateSettingsTableAsync()
    {
        await using var connection = new OracleConnection(OracleConnectionString);
        await connection.OpenAsync();

        // PL/SQL block so re-running against a warm container is harmless.
        await connection.ExecuteAsync($"""
            BEGIN
              EXECUTE IMMEDIATE '
                CREATE TABLE {SettingsTable} (
                  DOMAIN_NAME  VARCHAR2(64)  NOT NULL,
                  DATA_TYPE_ID VARCHAR2(128) NOT NULL,
                  IS_ACTIVE    NUMBER(1)     DEFAULT 0 NOT NULL,
                  CONSTRAINT PK_{SettingsTable} PRIMARY KEY (DOMAIN_NAME, DATA_TYPE_ID)
                )';
            EXCEPTION
              WHEN OTHERS THEN
                IF SQLCODE != -955 THEN RAISE; END IF;  -- -955 = name already used by an object
            END;
            """);
    }

    /// <summary>Replaces the settings rows for a domain with exactly the ones given.</summary>
    public async Task SeedSettingsAsync(string domain, params (string DataTypeId, bool IsActive)[] settings)
    {
        await using var connection = new OracleConnection(OracleConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteAsync(
            $"DELETE FROM {SettingsTable} WHERE DOMAIN_NAME = :domain",
            new { domain });

        foreach (var (dataTypeId, isActive) in settings)
        {
            // One parameter set per row: Oracle binds positionally by default, so the order here must
            // match the order the placeholders appear in the statement.
            await connection.ExecuteAsync(
                $"INSERT INTO {SettingsTable} (DOMAIN_NAME, DATA_TYPE_ID, IS_ACTIVE) " +
                "VALUES (:domain, :dataTypeId, :isActive)",
                new { domain, dataTypeId, isActive = isActive ? 1 : 0 });
        }
    }

    /// <summary>Sets the active flag on one existing row, to exercise a live settings change.</summary>
    public async Task SetActiveAsync(string domain, string dataTypeId, bool isActive)
    {
        await using var connection = new OracleConnection(OracleConnectionString);
        await connection.OpenAsync();

        await connection.ExecuteAsync(
            $"UPDATE {SettingsTable} SET IS_ACTIVE = :isActive " +
            "WHERE DOMAIN_NAME = :domain AND DATA_TYPE_ID = :dataTypeId",
            new { isActive = isActive ? 1 : 0, domain, dataTypeId });
    }

    /// <summary>Creates the topics up front so the consumer never races topic auto-creation.</summary>
    public async Task CreateTopicsAsync(params string[] topics)
    {
        using var admin = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = BootstrapServers }).Build();

        try
        {
            await admin.CreateTopicsAsync(topics.Select(t => new TopicSpecification
            {
                Name = t,
                NumPartitions = 1,
                ReplicationFactor = 1
            }));
        }
        catch (CreateTopicsException ex)
            when (ex.Results.All(r => r.Error.Code is ErrorCode.TopicAlreadyExists or ErrorCode.NoError))
        {
            // Already there from an earlier test in the collection.
        }
    }
}

/// <summary>
/// Binds the fixture to a single xUnit collection so Kafka and Oracle start once for the whole suite.
/// </summary>
[CollectionDefinition(Name)]
public class InfrastructureCollection : ICollectionFixture<InfrastructureFixture>
{
    public const string Name = "processor-infrastructure";
}
