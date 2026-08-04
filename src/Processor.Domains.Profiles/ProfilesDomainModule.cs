using Microsoft.Extensions.DependencyInjection;
using Processor.Core.Building;
using Processor.Core.Domains;
using Processor.Domains.Profiles.Building;

namespace Processor.Domains.Profiles;

/// <summary>
/// Activates the profiles domain. Mirrors <c>PostsDomainModule</c> exactly — adding a third domain
/// means a payload type, a message type, a data builder and a module like this one, and nothing else.
/// </summary>
public sealed class ProfilesDomainModule : DomainModule<ProfileInputMessage, ProfileData>
{
    public override string Name => ProfileData.Domain;

    protected override void RegisterDomainServices(IServiceCollection services) =>
        services.AddSingleton<IDomainDataBuilder<ProfileInputMessage, ProfileData>, ProfileDataBuilder>();
}
