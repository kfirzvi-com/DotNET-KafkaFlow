using Microsoft.Extensions.DependencyInjection;
using Processor.Core.Building;
using Processor.Core.Domains;
using Processor.Domains.Posts.Building;

namespace Processor.Domains.Posts;

/// <summary>
/// Activates the posts domain. Everything shared comes from
/// <see cref="DomainModule{TInput, TDomainData}"/>; this class only names the domain and registers the
/// one domain-specific builder.
/// </summary>
public sealed class PostsDomainModule : DomainModule<PostInputMessage, PostData>
{
    public override string Name => PostData.Domain;

    protected override void RegisterDomainServices(IServiceCollection services) =>
        services.AddSingleton<IDomainDataBuilder<PostInputMessage, PostData>, PostDataBuilder>();
}
