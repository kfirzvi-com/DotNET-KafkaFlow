using KafkaFlow;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Processor.Core;
using Processor.Core.Application;
using Processor.Core.Building;
using Processor.Core.DataTypes;
using Processor.Core.Domains;
using Processor.Core.Messages;

namespace Processor.MockTests.Support;

/// <summary>
/// The application assembled exactly as the host assembles it — Core registrations plus one domain
/// module selected from configuration — but with Kafka's producers mocked and the in-memory settings
/// store standing in for Oracle. Everything between the consumer handler and the producers is the real
/// code, including the real DI graph, the real refresh hosted service and the real domain builders.
/// </summary>
public sealed class MockProcessorApp<TInput, TDomainData> : IAsyncDisposable
    where TInput : InputMessage<TDomainData>
    where TDomainData : class, IDomainData, new()
{
    private readonly ServiceProvider _provider;

    private MockProcessorApp(
        ServiceProvider provider,
        InMemoryDataTypeSettingsStore store,
        Mock<IMessageProducer<OutputMessage<TDomainData>>> outputProducer,
        Mock<IMessageProducer<DeadLetterMessage<TDomainData>>> deadLetterProducer)
    {
        _provider = provider;
        Store = store;
        OutputProducer = outputProducer;
        DeadLetterProducer = deadLetterProducer;
    }

    public InMemoryDataTypeSettingsStore Store { get; }

    public Mock<IMessageProducer<OutputMessage<TDomainData>>> OutputProducer { get; }

    public Mock<IMessageProducer<DeadLetterMessage<TDomainData>>> DeadLetterProducer { get; }

    /// <summary>Messages captured on the output topic, in production order.</summary>
    public List<OutputMessage<TDomainData>> Produced { get; } = new();

    /// <summary>Messages captured on the dead-letter topic, in production order.</summary>
    public List<DeadLetterMessage<TDomainData>> DeadLettered { get; } = new();

    /// <summary>Keys the producers were called with, so key routing is observable.</summary>
    public List<object?> ProducedKeys { get; } = new();

    public MessageHandler<TInput, TDomainData> Handler =>
        _provider.GetRequiredService<MessageHandler<TInput, TDomainData>>();

    /// <summary>The same builder instance the handler uses (both are singletons).</summary>
    public IOutputMessageBuilder<TInput, TDomainData> Builder =>
        _provider.GetRequiredService<IOutputMessageBuilder<TInput, TDomainData>>();

    public IDataTypeSettingsRepository Settings =>
        _provider.GetRequiredService<IDataTypeSettingsRepository>();

    public IDataTypeSettingsCache SettingsCache =>
        _provider.GetRequiredService<IDataTypeSettingsCache>();

    /// <summary>The real health-check service, so readiness can be evaluated as Kubernetes would.</summary>
    public Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckService HealthCheckService =>
        _provider.GetRequiredService<Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckService>();

    /// <summary>
    /// Runs the build step directly. Drops and filters produce no message, so this is how their
    /// reason codes are observed.
    /// </summary>
    public Task<BuildOutcome<TDomainData>> BuildAsync(TInput message, string? dataTypeId) =>
        Builder.Build(message, dataTypeId);

    /// <summary>The hosted services the real host would run, in registration order.</summary>
    public IEnumerable<IHostedService> HostedServices => _provider.GetServices<IHostedService>();

    /// <summary>
    /// Builds the app for a domain, seeding the settings store with the given data types. Mirrors the
    /// host: <c>AddProcessorCore</c> + the module selected by <c>Processor:Domain</c>.
    /// </summary>
    public static MockProcessorApp<TInput, TDomainData> Create(
        IDomainModule module,
        IEnumerable<DataTypeSetting> settings,
        IDictionary<string, string?>? extraConfiguration = null)
    {
        var configuration = BuildConfiguration(module.Name, extraConfiguration);

        var services = new ServiceCollection();
        services.AddSingleton<ILoggerFactory>(NullLoggerFactory.Instance);
        services.AddLogging();

        // The real shared wiring, including the refresh hosted service and the caching repository.
        services.AddProcessorCore(configuration);
        services.AddProcessorHealthChecks();

        // Replace the store instance with one this test controls (configuration already selects the
        // in-memory store, so this only pins which instance is used).
        var store = new InMemoryDataTypeSettingsStore(settings);
        services.AddSingleton<IDataTypeSettingsStore>(store);

        var outputProducer = new Mock<IMessageProducer<OutputMessage<TDomainData>>>();
        var deadLetterProducer = new Mock<IMessageProducer<DeadLetterMessage<TDomainData>>>();
        services.AddSingleton(outputProducer.Object);
        services.AddSingleton(deadLetterProducer.Object);

        // The real domain module — the only domain-specific registration, exactly as in the host.
        module.RegisterServices(services);

        var provider = services.BuildServiceProvider(validateScopes: true);
        var app = new MockProcessorApp<TInput, TDomainData>(
            provider, store, outputProducer, deadLetterProducer);
        app.WireProducerCapture();
        return app;
    }

    private static IConfiguration BuildConfiguration(
        string domain, IDictionary<string, string?>? extraConfiguration)
    {
        var values = new Dictionary<string, string?>
        {
            [$"{ProcessorOptions.SectionName}:{nameof(ProcessorOptions.Domain)}"] = domain,
            [$"{DataTypeSettingsOptions.SectionName}:Store"] =
                ProcessorCoreServiceCollectionExtensions.InMemoryStoreName,
            [$"{DataTypeSettingsOptions.SectionName}:{nameof(DataTypeSettingsOptions.HeaderName)}"] =
                "data-type-id",
            // Production waits 5s between startup attempts; the retry *count* is what these tests
            // care about, so the wait is removed to keep them fast.
            [$"{DataTypeSettingsOptions.SectionName}:{nameof(DataTypeSettingsOptions.StartupRetryDelaySeconds)}"] =
                "0"
        };

        if (extraConfiguration is not null)
        {
            foreach (var pair in extraConfiguration)
            {
                values[pair.Key] = pair.Value;
            }
        }

        return new ConfigurationBuilder().AddInMemoryCollection(values).Build();
    }

    private void WireProducerCapture()
    {
        var deliveryResult = new Confluent.Kafka.DeliveryResult<byte[], byte[]>
        {
            Status = Confluent.Kafka.PersistenceStatus.Persisted,
            Offset = new Confluent.Kafka.Offset(0),
            Partition = new Confluent.Kafka.Partition(0),
            Topic = "output"
        };

        // KafkaFlow's ProduceAsync takes the message as `object`, so the callbacks must too.
        OutputProducer
            .Setup(p => p.ProduceAsync(
                It.IsAny<object>(), It.IsAny<OutputMessage<TDomainData>>(),
                It.IsAny<IMessageHeaders>(), It.IsAny<int?>()))
            .Callback((object key, object message, IMessageHeaders _, int? _) =>
            {
                ProducedKeys.Add(key);
                Produced.Add((OutputMessage<TDomainData>)message);
            })
            .ReturnsAsync(deliveryResult);

        DeadLetterProducer
            .Setup(p => p.ProduceAsync(
                It.IsAny<object>(), It.IsAny<DeadLetterMessage<TDomainData>>(),
                It.IsAny<IMessageHeaders>(), It.IsAny<int?>()))
            .Callback((object key, object message, IMessageHeaders _, int? _) =>
            {
                ProducedKeys.Add(key);
                DeadLettered.Add((DeadLetterMessage<TDomainData>)message);
            })
            .ReturnsAsync(deliveryResult);
    }

    /// <summary>
    /// Runs startup the way the host does: the settings refresh service loads the snapshot (and throws
    /// if it cannot) before any message is handled.
    /// </summary>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        foreach (var hostedService in HostedServices)
        {
            await hostedService.StartAsync(cancellationToken);
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        foreach (var hostedService in HostedServices)
        {
            await hostedService.StopAsync(cancellationToken);
        }
    }

    /// <summary>Delivers a message to the handler the way KafkaFlow would.</summary>
    public Task DeliverAsync(TInput message, string? dataTypeId, bool useHeader = false)
    {
        var context = MessageContexts.For(message, dataTypeId, useHeader);
        return Handler.Handle(context, message);
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        await _provider.DisposeAsync();
    }
}
