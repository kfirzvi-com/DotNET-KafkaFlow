using System.Diagnostics;
using System.Text;
using KafkaFlow;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Processor.Core.Building;
using Processor.Core.Diagnostics;
using Processor.Core.Messages;

namespace Processor.Core.Application;

/// <summary>
/// The single consumer handler, shared by every domain and closed over one domain's types. KafkaFlow
/// resolves <c>MessageHandler&lt;PostInputMessage, PostData&gt;</c> (or the profiles equivalent) from
/// DI, so the routing, metrics and dead-lettering logic exists once regardless of how many domains
/// exist.
/// </summary>
public class MessageHandler<TInput, TDomainData> : IMessageHandler<TInput>
    where TInput : InputMessage<TDomainData>
    where TDomainData : class, IDomainData, new()
{
    private readonly IMessageProducer<OutputMessage<TDomainData>> _producer;
    private readonly IMessageProducer<DeadLetterMessage<TDomainData>> _deadLetterProducer;
    private readonly IOutputMessageBuilder<TInput, TDomainData> _outputMessageBuilder;
    private readonly DataTypeSettingsOptions _settingsOptions;
    private readonly int _workMicros;
    private readonly ILogger<MessageHandler<TInput, TDomainData>> _logger;
    private readonly string _domain;

    public MessageHandler(
        IMessageProducer<OutputMessage<TDomainData>> producer,
        IMessageProducer<DeadLetterMessage<TDomainData>> deadLetterProducer,
        IOutputMessageBuilder<TInput, TDomainData> outputMessageBuilder,
        IOptions<DataTypeSettingsOptions> settingsOptions,
        IOptions<BenchmarkOptions> benchmarkOptions,
        ILogger<MessageHandler<TInput, TDomainData>> logger)
    {
        _producer = producer;
        _deadLetterProducer = deadLetterProducer;
        _outputMessageBuilder = outputMessageBuilder;
        _settingsOptions = settingsOptions.Value;
        _workMicros = benchmarkOptions.Value.WorkMicros;
        _logger = logger;

        // The payload type is the authority on its own domain name, so telemetry cannot drift from
        // the domain actually being processed even if configuration says otherwise.
        _domain = new TDomainData().DomainName;
    }

    public async Task Handle(IMessageContext context, TInput message)
    {
        var stopwatch = Stopwatch.StartNew();

        BurnCpu(_workMicros);

        var dataTypeId = ResolveDataTypeId(context);

        _logger.LogInformation(
            "Processing message with ID: {MessageId}, Domain: {Domain}, DataType: {DataTypeId}, Content: {Content}",
            message.Id, _domain, dataTypeId, message.Content);

        var outcome = await _outputMessageBuilder.Build(message, dataTypeId);

        switch (outcome.Status)
        {
            case BuildStatus.Ok:
                await _producer.ProduceAsync(message.Id, outcome.Message!);
                ProcessorMetrics.RecordProcessed(_domain);
                _logger.LogInformation("Message processed and sent to output topic");
                break;

            case BuildStatus.DeadLetter:
                var deadLetterMessage = new DeadLetterMessage<TDomainData>
                {
                    Id = message.Id,
                    Reason = outcome.Reason ?? "Unknown reason",
                    Domain = _domain,
                    OriginalMessage = DeadLetterOriginal<TDomainData>.From(message),
                    FailedAt = DateTime.UtcNow
                };

                await _deadLetterProducer.ProduceAsync(message.Id, deadLetterMessage);
                ProcessorMetrics.RecordDeadLettered(_domain);
                _logger.LogWarning("Message sent to dead letter queue: {Reason}", outcome.Reason);
                break;

            case BuildStatus.Filtered:
                ProcessorMetrics.RecordFiltered(_domain, outcome.Reason ?? "filtered");
                _logger.LogInformation(
                    "Message filtered out ({Reason}): data type '{DataTypeId}'", outcome.Reason, dataTypeId);
                break;

            case BuildStatus.Drop:
                ProcessorMetrics.RecordDropped(_domain);
                _logger.LogWarning("Message dropped: {Reason}", outcome.Reason);
                break;

            default:
                _logger.LogWarning("Unhandled build status: {Status}", outcome.Status);
                break;
        }

        stopwatch.Stop();
        ProcessorMetrics.RecordProcessingDuration(_domain, stopwatch.Elapsed.TotalMilliseconds);
    }

    /// <summary>
    /// Reads the data type id from the configured Kafka header, falling back to the message key.
    /// </summary>
    private string? ResolveDataTypeId(IMessageContext context)
    {
        var fromHeader = context.Headers?.GetString(_settingsOptions.HeaderName);
        if (!string.IsNullOrWhiteSpace(fromHeader))
        {
            return fromHeader;
        }

        return KeyToString(context.Message.Key);
    }

    private static string? KeyToString(object? key) => key switch
    {
        null => null,
        string s => s,
        byte[] bytes => Encoding.UTF8.GetString(bytes),
        _ => key.ToString()
    };

    /// <summary>Busy-spins for approximately <paramref name="micros"/> microseconds to emulate CPU-bound work.</summary>
    private static void BurnCpu(int micros)
    {
        if (micros <= 0)
        {
            return;
        }

        var targetTicks = (long)micros * Stopwatch.Frequency / 1_000_000;
        var start = Stopwatch.GetTimestamp();
        long acc = 0;
        while (Stopwatch.GetTimestamp() - start < targetTicks)
        {
            // Do a little real work so the loop isn't optimized into a pure timer poll.
            acc += start % 97;
        }

        if (acc == long.MinValue)
        {
            _ = acc; // prevent the JIT from eliminating the accumulator
        }
    }
}
