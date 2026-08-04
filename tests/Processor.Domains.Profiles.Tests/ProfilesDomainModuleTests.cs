using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Processor.Core;
using Processor.Core.Building;
using Processor.Core.DataTypes;
using Processor.Core.Messages;
using Processor.Domains.Profiles.Building;

namespace Processor.Domains.Profiles.Tests;

/// <summary>
/// Mirrors the posts module tests: the profiles module must close Core's generic pipeline over its own
/// types with no extra wiring.
/// </summary>
public class ProfilesDomainModuleTests
{
    private static ServiceProvider BuildProvider()
    {
        var services = new ServiceCollection();
        services.AddSingleton<ILoggerFactory>(NullLoggerFactory.Instance);
        services.AddLogging();
        services.AddOptions();
        services.AddSharedFieldBuilders();
        services.AddSingleton<IDataTypeSettingsStore>(new InMemoryDataTypeSettingsStore());
        services.AddSingleton<CachingDataTypeSettingsRepository>();
        services.AddSingleton<IDataTypeSettingsRepository>(sp =>
            sp.GetRequiredService<CachingDataTypeSettingsRepository>());

        new ProfilesDomainModule().RegisterServices(services);

        return services.BuildServiceProvider(validateScopes: true);
    }

    [Fact]
    public void Name_IsTheProfilesDomain()
    {
        Assert.Equal("profiles", new ProfilesDomainModule().Name);
        Assert.Equal(ProfileData.Domain, new ProfilesDomainModule().Name);
    }

    [Fact]
    public void RegisterServices_RegistersTheProfilesDataBuilder()
    {
        using var provider = BuildProvider();

        var builder = provider.GetRequiredService<IDomainDataBuilder<ProfileInputMessage, ProfileData>>();

        Assert.IsType<ProfileDataBuilder>(builder);
    }

    [Fact]
    public void RegisterServices_ResolvesTheOutputMessageBuilderClosedOverProfilesTypes()
    {
        using var provider = BuildProvider();

        var builder = provider.GetRequiredService<IOutputMessageBuilder<ProfileInputMessage, ProfileData>>();

        Assert.IsType<OutputMessageBuilder<ProfileInputMessage, ProfileData>>(builder);
    }

    [Fact]
    public void ProfileInputMessage_ClosesTheGenericBaseOverProfileData()
    {
        var message = new ProfileInputMessage();

        Assert.IsAssignableFrom<InputMessage<ProfileData>>(message);
        Assert.IsAssignableFrom<IInputMessage>(message);
        Assert.IsType<ProfileData>(message.DomainData);
    }
}
