using Microsoft.Extensions.Logging.Abstractions;
using Processor.Core.DataTypes;

namespace Processor.Core.Tests.DataTypes;

/// <summary>
/// The snapshot behaviour the whole resilience story rests on: reads never touch the store, and a
/// failed reload keeps the previous data rather than clearing it.
/// </summary>
public class CachingDataTypeSettingsRepositoryTests
{
    private static (CachingDataTypeSettingsRepository repository, InMemoryDataTypeSettingsStore store) Build(
        params DataTypeSetting[] settings)
    {
        var store = new InMemoryDataTypeSettingsStore(settings);
        var repository = new CachingDataTypeSettingsRepository(
            store, NullLogger<CachingDataTypeSettingsRepository>.Instance);
        return (repository, store);
    }

    private static DataTypeSetting Setting(string id, bool active) =>
        new() { DataTypeId = id, IsActive = active };

    [Fact]
    public async Task BeforeFirstRefresh_NotLoaded_AndFindsNothing()
    {
        var (repository, _) = Build(Setting("weather", true));

        Assert.False(repository.IsLoaded);
        Assert.Null(repository.LastSuccessfulLoadUtc);
        Assert.Equal(0, repository.Count);
        Assert.Null(await repository.FindByIdAsync("weather"));
    }

    [Fact]
    public async Task Refresh_PopulatesSnapshot()
    {
        var (repository, _) = Build(Setting("weather", true), Setting("news", false));

        Assert.True(await repository.RefreshAsync());

        Assert.True(repository.IsLoaded);
        Assert.NotNull(repository.LastSuccessfulLoadUtc);
        Assert.Equal(2, repository.Count);

        var weather = await repository.FindByIdAsync("weather");
        Assert.NotNull(weather);
        Assert.True(weather!.IsActive);

        var news = await repository.FindByIdAsync("news");
        Assert.NotNull(news);
        Assert.False(news!.IsActive);
    }

    [Fact]
    public async Task FindById_IsCaseInsensitive()
    {
        var (repository, _) = Build(Setting("Weather", true));
        await repository.RefreshAsync();

        Assert.NotNull(await repository.FindByIdAsync("weather"));
        Assert.NotNull(await repository.FindByIdAsync("WEATHER"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public async Task FindById_ReturnsNull_ForBlankId(string? id)
    {
        var (repository, _) = Build(Setting("weather", true));
        await repository.RefreshAsync();

        Assert.Null(await repository.FindByIdAsync(id!));
    }

    [Fact]
    public async Task FindById_NeverQueriesTheStore_AfterLoad()
    {
        var (repository, store) = Build(Setting("weather", true));
        await repository.RefreshAsync();
        var loadsAfterRefresh = store.LoadCount;

        for (var i = 0; i < 100; i++)
        {
            await repository.FindByIdAsync("weather");
            await repository.FindAllAsync();
        }

        // The hot path is snapshot-only: a store outage cannot be amplified by throughput.
        Assert.Equal(loadsAfterRefresh, store.LoadCount);
    }

    [Fact]
    public async Task Refresh_KeepsPreviousSnapshot_WhenStoreFails()
    {
        var (repository, store) = Build(Setting("weather", true));
        await repository.RefreshAsync();
        var loadedAt = repository.LastSuccessfulLoadUtc;

        store.FailWith = new InvalidOperationException("store down");
        Assert.False(await repository.RefreshAsync());

        // Stale but intact: a data type must not be treated as inactive because a reload failed.
        Assert.True(repository.IsLoaded);
        Assert.Equal(1, repository.Count);
        Assert.Equal(loadedAt, repository.LastSuccessfulLoadUtc);
        var weather = await repository.FindByIdAsync("weather");
        Assert.NotNull(weather);
        Assert.True(weather!.IsActive);
    }

    [Fact]
    public async Task Refresh_ReturnsFalse_AndStaysUnloaded_WhenFirstLoadFails()
    {
        var (repository, store) = Build(Setting("weather", true));
        store.FailWith = new InvalidOperationException("store down");

        Assert.False(await repository.RefreshAsync());

        Assert.False(repository.IsLoaded);
        Assert.Equal(0, repository.Count);
    }

    [Fact]
    public async Task Refresh_PicksUpStoreChanges()
    {
        var (repository, store) = Build(Setting("weather", true));
        await repository.RefreshAsync();

        store.Replace(new[] { Setting("weather", false), Setting("sports", true) });
        Assert.True(await repository.RefreshAsync());

        Assert.Equal(2, repository.Count);
        Assert.False((await repository.FindByIdAsync("weather"))!.IsActive);
        Assert.True((await repository.FindByIdAsync("sports"))!.IsActive);
    }

    [Fact]
    public async Task Refresh_DropsSettingsWithBlankIds()
    {
        var (repository, _) = Build(Setting("weather", true), Setting("  ", true));

        Assert.True(await repository.RefreshAsync());

        Assert.Equal(1, repository.Count);
        Assert.NotNull(await repository.FindByIdAsync("weather"));
    }

    [Fact]
    public async Task Refresh_LastWins_OnDuplicateIds()
    {
        var (repository, _) = Build(Setting("weather", true), Setting("weather", false));

        Assert.True(await repository.RefreshAsync());

        Assert.Equal(1, repository.Count);
        Assert.False((await repository.FindByIdAsync("weather"))!.IsActive);
    }

    [Fact]
    public async Task FindAll_ReturnsSnapshotContents()
    {
        var (repository, _) = Build(Setting("weather", true), Setting("news", false));
        await repository.RefreshAsync();

        var all = await repository.FindAllAsync();

        Assert.Equal(2, all.Count);
        Assert.Contains(all, s => s.DataTypeId == "weather");
        Assert.Contains(all, s => s.DataTypeId == "news");
    }
}
