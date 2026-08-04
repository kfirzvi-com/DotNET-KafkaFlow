using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Processor.Core;
using Processor.Core.Building;
using Processor.Core.DataTypes;
using Processor.Core.Messages;
using Processor.Domains.Posts.Building;

namespace Processor.Domains.Posts.Tests;

/// <summary>
/// The module is what the host activates for this domain, so its registrations are the contract: the
/// generic pipeline must resolve fully closed over the posts types.
/// </summary>
public class PostsDomainModuleTests
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

        new PostsDomainModule().RegisterServices(services);

        return services.BuildServiceProvider(validateScopes: true);
    }

    [Fact]
    public void Name_IsThePostsDomain()
    {
        Assert.Equal("posts", new PostsDomainModule().Name);
        Assert.Equal(PostData.Domain, new PostsDomainModule().Name);
    }

    [Fact]
    public void RegisterServices_RegistersThePostsDataBuilder()
    {
        using var provider = BuildProvider();

        var builder = provider.GetRequiredService<IDomainDataBuilder<PostInputMessage, PostData>>();

        Assert.IsType<PostDataBuilder>(builder);
    }

    [Fact]
    public void RegisterServices_ResolvesTheOutputMessageBuilderClosedOverPostsTypes()
    {
        using var provider = BuildProvider();

        var builder = provider.GetRequiredService<IOutputMessageBuilder<PostInputMessage, PostData>>();

        Assert.IsType<OutputMessageBuilder<PostInputMessage, PostData>>(builder);
    }

    [Fact]
    public void PostInputMessage_ClosesTheGenericBaseOverPostData()
    {
        var message = new PostInputMessage();

        Assert.IsAssignableFrom<InputMessage<PostData>>(message);
        Assert.IsAssignableFrom<IInputMessage>(message);
        Assert.IsType<PostData>(message.DomainData);
    }
}
