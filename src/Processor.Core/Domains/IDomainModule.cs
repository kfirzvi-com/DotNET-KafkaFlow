using KafkaFlow.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Processor.Core.Application;

namespace Processor.Core.Domains;

/// <summary>
/// Everything the host needs to run one domain. Each domain project contributes exactly one module;
/// the host holds them all but activates only the one named by <c>Processor:Domain</c>, so a single
/// image deploys per-domain without conditional code anywhere else.
/// <para>
/// This exists because KafkaFlow's registration API needs closed generic types
/// (<c>AddSingleTypeDeserializer&lt;PostInputMessage, ...&gt;</c>) that the host cannot name without
/// knowing the domain. The module closes them from inside the domain project.
/// </para>
/// </summary>
public interface IDomainModule
{
    /// <summary>Domain name matched against configuration, case-insensitively (e.g. <c>posts</c>).</summary>
    string Name { get; }

    /// <summary>Registers the domain's builders and its closed message handler.</summary>
    void RegisterServices(IServiceCollection services);

    /// <summary>Adds the deserializer and typed handler for this domain's input message.</summary>
    void ConfigureConsumer(IConsumerMiddlewareConfigurationBuilder middlewares);

    /// <summary>Registers the output and dead-letter producers for this domain's message types.</summary>
    void ConfigureProducers(IClusterConfigurationBuilder cluster, KafkaOptions kafka);
}
