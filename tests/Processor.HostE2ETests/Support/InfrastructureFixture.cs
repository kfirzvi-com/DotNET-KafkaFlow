using Confluent.Kafka;
using Confluent.Kafka.Admin;
using DotNet.Testcontainers.Builders;
using Testcontainers.Kafka;
using Testcontainers.Oracle;

namespace Processor.HostE2ETests.Support;

/// <summary>
/// Real infrastructure for the end-to-end suite: a Kafka broker and an Oracle database, both in
/// containers, shared by every test in the collection (Oracle takes a while to boot, so starting it per
/// test is not viable).
/// <para>
/// What is <em>not</em> shared is per-test state: each test asks for its own topics and its own
/// <see cref="SettingsTable"/>, so cases cannot collide over data type settings.
/// </para>
/// </summary>
public sealed class InfrastructureFixture : IAsyncLifetime
{
    /// <summary>Application schema the processor connects as — mirrors the compose setup.</summary>
    private const string AppUser = "processor";

    private const string AppPassword = "processor";

    /// <summary>
    /// Pluggable-database service name of the <c>oracle-free</c> image. Testcontainers' built-in
    /// connection string assumes the older XE image's <c>XEPDB1</c>, so it cannot be used as-is here.
    /// </summary>
    private const string ServiceName = "FREEPDB1";

    /// <summary>Numbers the per-test tables so their names are unique and short enough for Oracle.</summary>
    private int _tableSequence;

    private readonly KafkaContainer _kafka = new KafkaBuilder()
        .WithImage("confluentinc/cp-kafka:7.8.0")
        .Build();

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

    /// <summary>
    /// Creates a settings table owned by one test. <paramref name="discriminator"/> only makes the name
    /// readable; uniqueness comes from a per-run sequence number.
    /// </summary>
    public async Task<SettingsTable> CreateSettingsTableAsync(string discriminator)
    {
        var sequence = Interlocked.Increment(ref _tableSequence);
        var table = new SettingsTable(SettingsTable.NameFor(sequence, discriminator), OracleConnectionString);
        await table.CreateAsync();
        return table;
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
