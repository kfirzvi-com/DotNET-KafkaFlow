using KafkaFlow;
using KafkaFlow.Configuration;
using KafkaFlow.Serializer;
using Microsoft.Extensions.DependencyInjection;
using Processor.Core.Application;
using Processor.Core.Building;
using Processor.Core.Messages;

namespace Processor.Core.Domains;

/// <summary>
/// Base for a domain module: closes Core's generic pipeline over one domain's types and does all the
/// KafkaFlow and DI wiring. A domain project only has to name itself and register its own data
/// builder — everything shared (orchestrating builder, handler, serializers, producers) is inherited.
/// </summary>
public abstract class DomainModule<TInput, TDomainData> : IDomainModule
    where TInput : InputMessage<TDomainData>
    where TDomainData : class, IDomainData, new()
{
    public abstract string Name { get; }

    public void RegisterServices(IServiceCollection services)
    {
        services.AddSingleton<
            IOutputMessageBuilder<TInput, TDomainData>,
            OutputMessageBuilder<TInput, TDomainData>>();

        // KafkaFlow resolves the handler by concrete type, so it must be registered as itself.
        services.AddSingleton<MessageHandler<TInput, TDomainData>>();

        RegisterDomainServices(services);
    }

    /// <summary>
    /// Registers the domain's own services — at minimum an
    /// <see cref="IDomainDataBuilder{TInput, TDomainData}"/>, which
    /// <see cref="OutputMessageBuilder{TInput, TDomainData}"/> depends on.
    /// </summary>
    protected abstract void RegisterDomainServices(IServiceCollection services);

    public void ConfigureConsumer(IConsumerMiddlewareConfigurationBuilder middlewares) =>
        middlewares
            .AddSingleTypeDeserializer<TInput, JsonCoreDeserializer>()
            .AddTypedHandlers(handlers => handlers.AddHandler<MessageHandler<TInput, TDomainData>>());

    public void ConfigureProducers(IClusterConfigurationBuilder cluster, KafkaOptions kafka)
    {
        cluster
            .AddProducer<OutputMessage<TDomainData>>(producer => producer
                .DefaultTopic(kafka.OutputTopic)
                .AddMiddlewares(middlewares => middlewares.AddSerializer<JsonCoreSerializer>()))
            .AddProducer<DeadLetterMessage<TDomainData>>(producer => producer
                .DefaultTopic(kafka.DeadLetterTopic)
                .AddMiddlewares(middlewares => middlewares.AddSerializer<JsonCoreSerializer>()));
    }
}
