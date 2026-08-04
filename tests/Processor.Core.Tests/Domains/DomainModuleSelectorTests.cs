using KafkaFlow.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Processor.Core.Application;
using Processor.Core.Domains;

namespace Processor.Core.Tests.Domains;

/// <summary>
/// Domain selection decides what an entire deployment does, so every way it can go wrong must stop
/// startup rather than degrade quietly.
/// </summary>
public class DomainModuleSelectorTests
{
    private sealed class FakeModule : IDomainModule
    {
        public FakeModule(string name) => Name = name;

        public string Name { get; }

        public void RegisterServices(IServiceCollection services) { }

        public void ConfigureConsumer(IConsumerMiddlewareConfigurationBuilder middlewares) { }

        public void ConfigureProducers(IClusterConfigurationBuilder cluster, KafkaOptions kafka) { }
    }

    private static IDomainModule[] Modules(params string[] names) =>
        names.Select(n => (IDomainModule)new FakeModule(n)).ToArray();

    [Fact]
    public void Select_ReturnsMatchingModule()
    {
        var selected = DomainModuleSelector.Select(Modules("posts", "profiles"), "profiles");

        Assert.Equal("profiles", selected.Name);
    }

    [Theory]
    [InlineData("POSTS")]
    [InlineData("Posts")]
    [InlineData("  posts  ")]
    public void Select_IsCaseInsensitiveAndTrims(string configured)
    {
        var selected = DomainModuleSelector.Select(Modules("posts", "profiles"), configured);

        Assert.Equal("posts", selected.Name);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Select_Throws_WhenDomainNotConfigured(string? domain)
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => DomainModuleSelector.Select(Modules("posts", "profiles"), domain));

        Assert.Contains("Processor:Domain", ex.Message);
        // The message must name the valid options, or an operator cannot fix it.
        Assert.Contains("posts", ex.Message);
        Assert.Contains("profiles", ex.Message);
    }

    [Fact]
    public void Select_Throws_OnUnknownDomain()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => DomainModuleSelector.Select(Modules("posts", "profiles"), "stocks"));

        Assert.Contains("Unknown domain 'stocks'", ex.Message);
        Assert.Contains("posts, profiles", ex.Message);
    }

    [Fact]
    public void Select_Throws_WhenNoModulesRegistered()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => DomainModuleSelector.Select(Array.Empty<IDomainModule>(), "posts"));

        Assert.Contains("No domain modules", ex.Message);
    }

    [Fact]
    public void Select_Throws_OnDuplicateDomainNames()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => DomainModuleSelector.Select(Modules("posts", "POSTS"), "posts"));

        Assert.Contains("must be unique", ex.Message);
    }
}
