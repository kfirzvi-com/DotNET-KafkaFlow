using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using Processor.Core.Application;
using Processor.Core.DataTypes;

namespace Processor.Core.Tests.DataTypes;

/// <summary>
/// Store-level tests. The Oracle store's <em>query</em> behaviour needs a database and lives in the
/// host end-to-end suite; what is unit-testable here is its configuration validation, which must fail
/// at construction rather than on the first refresh.
/// </summary>
public class StoreTests
{
    private static OracleDataTypeSettingsStore CreateOracleStore(
        string connectionString = "User Id=p;Password=p;Data Source=localhost:1521/FREEPDB1",
        string table = "DATA_TYPE_SETTINGS",
        string domain = "widgets") =>
        new(
            Options.Create(new OracleOptions { ConnectionString = connectionString, SettingsTable = table }),
            Options.Create(new ProcessorOptions { Domain = domain }),
            NullLogger<OracleDataTypeSettingsStore>.Instance);

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public void OracleStore_Throws_WhenConnectionStringMissing(string connectionString)
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => CreateOracleStore(connectionString: connectionString));

        Assert.Contains("Oracle:ConnectionString", ex.Message);
    }

    [Theory]
    [InlineData("DATA_TYPE_SETTINGS; DROP TABLE USERS")]
    [InlineData("DATA TYPE SETTINGS")]
    [InlineData("1_BAD_START")]
    [InlineData("SCHEMA.TABLE.EXTRA")]
    [InlineData("")]
    [InlineData("'quoted'")]
    public void OracleStore_Throws_OnNonIdentifierTableName(string table)
    {
        // The table name is interpolated into SQL, so anything but a bare identifier is rejected.
        var ex = Assert.Throws<InvalidOperationException>(() => CreateOracleStore(table: table));

        Assert.Contains("Oracle:SettingsTable", ex.Message);
    }

    [Theory]
    [InlineData("DATA_TYPE_SETTINGS")]
    [InlineData("data_type_settings")]
    [InlineData("PROCESSOR.DATA_TYPE_SETTINGS")]
    public void OracleStore_Accepts_ValidTableNames(string table)
    {
        // Construction must not connect: only configuration is validated here.
        var store = CreateOracleStore(table: table);

        Assert.NotNull(store);
    }

    [Fact]
    public async Task InMemoryStore_ReturnsConfiguredSettings()
    {
        var store = new InMemoryDataTypeSettingsStore(new[]
        {
            new DataTypeSetting { DataTypeId = "weather", IsActive = true }
        });

        var settings = await store.LoadAllAsync();

        Assert.Single(settings);
        Assert.Equal("weather", settings[0].DataTypeId);
        Assert.Equal(1, store.LoadCount);
    }

    [Fact]
    public async Task InMemoryStore_IsEmptyByDefault()
    {
        var store = new InMemoryDataTypeSettingsStore();

        Assert.Empty(await store.LoadAllAsync());
    }

    [Fact]
    public async Task InMemoryStore_ReturnsSnapshots_NotItsOwnList()
    {
        var store = new InMemoryDataTypeSettingsStore(new[]
        {
            new DataTypeSetting { DataTypeId = "weather", IsActive = true }
        });

        var first = await store.LoadAllAsync();
        store.Replace(Array.Empty<DataTypeSetting>());
        var second = await store.LoadAllAsync();

        // A caller holding an earlier result must not see it mutate underneath them.
        Assert.Single(first);
        Assert.Empty(second);
    }

    [Fact]
    public async Task InMemoryStore_FailWith_MakesLoadAndProbeThrow()
    {
        var store = new InMemoryDataTypeSettingsStore
        {
            FailWith = new InvalidOperationException("down")
        };

        await Assert.ThrowsAsync<InvalidOperationException>(() => store.LoadAllAsync());
        await Assert.ThrowsAsync<InvalidOperationException>(() => store.ProbeAsync());
    }

    [Fact]
    public async Task InMemoryStore_ProbeSucceeds_WhenHealthy()
    {
        var store = new InMemoryDataTypeSettingsStore();

        await store.ProbeAsync();
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task InMemoryStore_FailNextLoads_FailsThenRecovers(int failures)
    {
        var store = new InMemoryDataTypeSettingsStore(new[]
        {
            new DataTypeSetting { DataTypeId = "weather", IsActive = true }
        })
        {
            FailNextLoads = failures
        };

        for (var attempt = 1; attempt <= failures; attempt++)
        {
            await Assert.ThrowsAsync<InvalidOperationException>(() => store.LoadAllAsync());
        }

        // Recovered: the counter is exhausted, so the next load succeeds.
        Assert.Single(await store.LoadAllAsync());
        Assert.Equal(0, store.FailNextLoads);
        Assert.Equal(failures + 1, store.LoadCount);
    }

    [Fact]
    public async Task InMemoryStore_FailNextLoads_UsesFailWith_WhenBothAreSet()
    {
        var store = new InMemoryDataTypeSettingsStore
        {
            FailNextLoads = 1,
            FailWith = new TimeoutException("specific failure")
        };

        await Assert.ThrowsAsync<TimeoutException>(() => store.LoadAllAsync());
    }
}
